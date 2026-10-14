"""
Sarvatra Senani — FastAPI application, streaming, and workflow execution.

Runtime architecture
--------------------

    producers ──► Kafka `senani.telemetry` ──┐
                                              ├──► SenaniService._drive() ──► LangGraph (checkpointed)
    POST /api/trigger-telemetry ─────────────┘           │
                                                         ├──► EventBroker ──► WebSocket /ws/stream (dashboard)
                                                         │              └──► Kafka `senani.events` (downstream C2)
    POST /api/review-action ──► Command(resume=…) ───────┘

* **Transport.** If KAFKA_BOOTSTRAP_SERVERS is set, the trigger endpoint
  publishes to the telemetry topic and a consumer group starts the workflows
  (so any external sensor can feed the system too). Otherwise the endpoint
  starts the workflow directly — same code path from ``_drive`` onward.
* **Persistence.** With DATABASE_URL set, LangGraph checkpoints go to
  Postgres (AsyncPostgresSaver) and the geospatial agent uses PostGIS; one
  shared psycopg pool serves both. Paused HITL runs survive restarts and are
  rehydrated at startup. Without it, MemorySaver + in-memory geometry.
* **Concurrency.** Each run executes as an asyncio task bounded by a
  semaphore. Review actions take a per-thread lock so a double-click can
  never resume the same interrupt twice.

Run:  python main.py          (uses a selector event loop on Windows for psycopg)
 or:  uvicorn main:app --port 8000
"""

import asyncio
import json
import logging
import os
import secrets
import sys
from collections import deque
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect, status
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from langgraph.checkpoint.memory import MemorySaver
from langgraph.types import Command
from pydantic import ValidationError

from agents import LLMGateway, reference_data
from graph import MAX_REVISIONS, SenaniAgents, build_checkpoint_serde, build_graph
from models import (
    MissionSummary,
    PendingReview,
    ReviewActionRequest,
    ReviewActionResponse,
    ReviewDecision,
    ReviewRecord,
    SenaniState,
    StreamEvent,
    Telemetry,
    TriggerResponse,
    WorkflowStatus,
    new_id,
    utcnow,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("senani.api")

TERMINAL = {WorkflowStatus.APPROVED, WorkflowStatus.REJECTED, WorkflowStatus.AUTO_FINALIZED, WorkflowStatus.FAILED}


class Settings:
    """Environment configuration (read once at import)."""

    database_url = os.getenv("DATABASE_URL", "").strip()
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    kafka_telemetry_topic = os.getenv("KAFKA_TELEMETRY_TOPIC", "senani.telemetry")
    kafka_events_topic = os.getenv("KAFKA_EVENTS_TOPIC", "senani.events")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID", "senani-workers")
    api_key = os.getenv("SENANI_API_KEY", "").strip()
    cors_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",") if o.strip()]
    max_concurrent_runs = int(os.getenv("MAX_CONCURRENT_RUNS", "8"))
    max_tracked_missions = int(os.getenv("MAX_TRACKED_MISSIONS", "1000"))
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))


settings = Settings()


# ═════════════════════════════════════════════════════════════════════════════
# Event broker — fan-out to WebSocket subscribers (+ optional Kafka mirror)
# ═════════════════════════════════════════════════════════════════════════════


class EventBroker:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self.recent_logs: deque[dict[str, Any]] = deque(maxlen=300)  # replayed to new clients
        self.kafka_producer: Any = None

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1000)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(q)

    async def publish(self, event: StreamEvent) -> None:
        data = jsonable_encoder(event)
        if event.type == "log":
            self.recent_logs.append(data)
        for q in list(self._subscribers):
            if q.full():  # slow client: drop its oldest event rather than block everyone
                q.get_nowait()
            q.put_nowait(data)
        if self.kafka_producer is not None and event.type != "log":
            try:
                await self.kafka_producer.send(
                    settings.kafka_events_topic, json.dumps(data).encode(),
                    key=(event.thread_id or "").encode(),
                )
            except Exception as exc:
                log.warning("Kafka event publish failed: %s", exc)


# ═════════════════════════════════════════════════════════════════════════════
# Workflow service — owns the graph, registry and background runs
# ═════════════════════════════════════════════════════════════════════════════


class SenaniService:
    def __init__(self, graph: Any, checkpointer: Any, agents: SenaniAgents, broker: EventBroker,
                 checkpointer_kind: str) -> None:
        self.graph = graph
        self.checkpointer = checkpointer
        self.agents = agents
        self.broker = broker
        self.checkpointer_kind = checkpointer_kind
        self.missions: dict[str, MissionSummary] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._sem = asyncio.Semaphore(settings.max_concurrent_runs)
        self._tasks: set[asyncio.Task[None]] = set()
        self._kafka_consumer: Any = None
        self._consumer_task: asyncio.Task[None] | None = None

    @staticmethod
    def _config(thread_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": thread_id}}

    @property
    def kafka_enabled(self) -> bool:
        return self._kafka_consumer is not None

    # -- lifecycle ------------------------------------------------------------
    async def start(self) -> None:
        await self.rehydrate()
        if settings.kafka_bootstrap:
            await self._start_kafka()

    async def shutdown(self) -> None:
        if self._consumer_task:
            self._consumer_task.cancel()
        for t in list(self._tasks):
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for client in (self._kafka_consumer, self.broker.kafka_producer):
            if client is not None:
                try:
                    await client.stop()
                except Exception:
                    pass

    async def _start_kafka(self) -> None:
        try:
            from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

            producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
            consumer = AIOKafkaConsumer(
                settings.kafka_telemetry_topic, bootstrap_servers=settings.kafka_bootstrap,
                group_id=settings.kafka_group_id, auto_offset_reset="latest", enable_auto_commit=True,
            )
            await producer.start()
            try:
                await consumer.start()
            except Exception:
                await producer.stop()
                raise
        except Exception as exc:
            # Degrade to direct mode rather than refusing to boot the command center.
            log.error("Kafka unavailable (%s: %s) — falling back to direct ingestion", type(exc).__name__, exc)
            return
        self.broker.kafka_producer, self._kafka_consumer = producer, consumer
        self._consumer_task = asyncio.create_task(self._consume(), name="kafka-telemetry-consumer")
        log.info("Kafka enabled: consuming %s, publishing %s",
                 settings.kafka_telemetry_topic, settings.kafka_events_topic)

    async def _consume(self) -> None:
        """Telemetry topic consumer. Accepts a bare Telemetry object or
        an envelope {"thread_id": "...", "telemetry": {...}}."""
        async for msg in self._kafka_consumer:
            try:
                envelope = json.loads(msg.value)
                body = envelope.get("telemetry", envelope)
                telemetry = Telemetry.model_validate(body)
                thread_id = str(envelope.get("thread_id") or new_id("OP"))
            except (ValueError, ValidationError, AttributeError) as exc:
                log.warning("Dropping malformed telemetry at offset %s: %s", msg.offset, exc)
                continue
            # At-least-once delivery: skip redelivered messages that already have state.
            snap = await self.graph.aget_state(self._config(thread_id))
            if snap.values:
                log.info("Skipping duplicate telemetry for %s", thread_id)
                continue
            self.launch(thread_id, telemetry)

    async def rehydrate(self, limit: int = 2000) -> None:
        """Rebuild the mission registry from the checkpointer after a restart."""
        seen: list[str] = []
        try:
            async for cp in self.checkpointer.alist(None, limit=limit):
                tid = cp.config["configurable"]["thread_id"]
                if tid not in seen:
                    seen.append(tid)
        except Exception as exc:
            log.warning("Rehydration skipped: %s", exc)
            return
        for tid in seen[: settings.max_tracked_missions]:
            await self._sync(tid, announce=False)
        if seen:
            log.info("Rehydrated %d mission(s) from %s checkpointer", len(seen), self.checkpointer_kind)

    # -- submission -------------------------------------------------------------
    async def submit(self, telemetry: Telemetry) -> TriggerResponse:
        thread_id = new_id("OP")
        self._register(thread_id, telemetry)
        if self.kafka_enabled:
            envelope = {"thread_id": thread_id, "telemetry": jsonable_encoder(telemetry)}
            await self.broker.kafka_producer.send_and_wait(
                settings.kafka_telemetry_topic, json.dumps(envelope).encode(), key=thread_id.encode())
            await self.broker.publish(StreamEvent(type="mission", thread_id=thread_id,
                                                  data=jsonable_encoder(self.missions[thread_id])))
            return TriggerResponse(thread_id=thread_id, status=WorkflowStatus.RUNNING, transport="kafka")
        self.launch(thread_id, telemetry)
        return TriggerResponse(thread_id=thread_id, status=WorkflowStatus.RUNNING, transport="direct")

    def _register(self, thread_id: str, telemetry: Telemetry) -> None:
        now = utcnow()
        self.missions[thread_id] = MissionSummary(
            thread_id=thread_id, source_id=telemetry.source_id, source_type=telemetry.source_type,
            status=WorkflowStatus.RUNNING, created_at=now, updated_at=now, current_node="queued",
        )
        # Bound memory: evict the oldest *finished* missions first.
        if len(self.missions) > settings.max_tracked_missions:
            for old in sorted((m for m in self.missions.values() if m.status in TERMINAL),
                              key=lambda m: m.updated_at)[: len(self.missions) - settings.max_tracked_missions]:
                self.missions.pop(old.thread_id, None)
                self._locks.pop(old.thread_id, None)

    def launch(self, thread_id: str, telemetry: Telemetry) -> None:
        if thread_id not in self.missions:
            self._register(thread_id, telemetry)
        self._spawn(thread_id, {"thread_id": thread_id, "raw_input": telemetry})

    def _spawn(self, thread_id: str, graph_input: Any) -> None:
        task = asyncio.create_task(self._drive(thread_id, graph_input), name=f"run-{thread_id}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # -- execution ----------------------------------------------------------------
    async def _drive(self, thread_id: str, graph_input: Any) -> None:
        """Stream one graph run (fresh start or HITL resume) until it ends or pauses."""
        async with self._sem:
            try:
                async for chunk in self.graph.astream(graph_input, self._config(thread_id), stream_mode="updates"):
                    for node, update in chunk.items():
                        if node != "__interrupt__":
                            await self._on_node_update(thread_id, node, update or {})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("Workflow %s crashed", thread_id)
                if m := self.missions.get(thread_id):
                    self.missions[thread_id] = m.model_copy(update={
                        "status": WorkflowStatus.FAILED, "error": f"{type(exc).__name__}: {exc}", "updated_at": utcnow()})
                await self.broker.publish(StreamEvent(type="log", thread_id=thread_id, data={
                    "agent": "orchestrator", "level": "error", "message": f"Run crashed: {exc}", "ts": utcnow()}))
            await self._sync(thread_id)

    async def _on_node_update(self, thread_id: str, node: str, update: dict[str, Any]) -> None:
        for entry in update.get("logs", []):
            await self.broker.publish(StreamEvent(type="log", thread_id=thread_id, data=jsonable_encoder(entry)))
        m = self.missions.get(thread_id)
        if m is None:
            return
        patch: dict[str, Any] = {"current_node": node, "updated_at": utcnow()}
        if ev := update.get("parsed_event"):
            patch.update(event_type=ev.event_type, summary=ev.summary, latitude=ev.latitude, longitude=ev.longitude)
        if risk := update.get("risk"):
            patch.update(severity=risk.severity, score=risk.score)
        if "status" in update:
            patch["status"] = update["status"]
        if update.get("error"):
            patch["error"] = update["error"]
        self.missions[thread_id] = m.model_copy(update=patch)
        await self.broker.publish(StreamEvent(type="mission", thread_id=thread_id,
                                              data=jsonable_encoder(self.missions[thread_id])))

    async def _sync(self, thread_id: str, announce: bool = True) -> None:
        """Reconcile the registry with the authoritative checkpointed state."""
        loaded = await self.load(thread_id)
        if loaded is None:
            return
        snap, state = loaded
        paused = self._has_interrupt(snap)
        prev = self.missions.get(thread_id)
        status_ = WorkflowStatus.AWAITING_REVIEW if paused else state.status
        ev, risk = state.parsed_event, state.risk
        self.missions[thread_id] = MissionSummary(
            thread_id=thread_id,
            source_id=state.raw_input.source_id,
            source_type=state.raw_input.source_type,
            status=status_,
            created_at=prev.created_at if prev else (state.logs[0].ts if state.logs else state.raw_input.received_at),
            updated_at=utcnow() if announce else (state.logs[-1].ts if state.logs else state.raw_input.received_at),
            event_type=ev.event_type if ev else None,
            summary=ev.summary if ev else None,
            latitude=ev.latitude if ev else None,
            longitude=ev.longitude if ev else None,
            severity=risk.severity if risk else None,
            score=risk.score if risk else None,
            current_node=snap.next[0] if snap.next else "done",
            error=state.error,
        )
        if not announce:
            return
        summary = jsonable_encoder(self.missions[thread_id])
        await self.broker.publish(StreamEvent(type="mission", thread_id=thread_id, data=summary))
        if paused:
            await self.broker.publish(StreamEvent(type="review_required", thread_id=thread_id, data=summary))
        elif status_ in TERMINAL:
            await self.broker.publish(StreamEvent(type="finalized", thread_id=thread_id, data=summary))

    # -- queries ------------------------------------------------------------------
    @staticmethod
    def _has_interrupt(snap: Any) -> bool:
        return any(getattr(t, "interrupts", None) for t in snap.tasks)

    async def load(self, thread_id: str) -> tuple[Any, SenaniState] | None:
        snap = await self.graph.aget_state(self._config(thread_id))
        if not snap.values:
            return None
        return snap, SenaniState.model_validate(snap.values)

    async def pending_reviews(self) -> list[PendingReview]:
        out: list[PendingReview] = []
        waiting = sorted((m for m in self.missions.values() if m.status == WorkflowStatus.AWAITING_REVIEW),
                         key=lambda m: (-(m.score or 0), m.created_at))  # highest risk first
        for m in waiting:
            loaded = await self.load(m.thread_id)
            if loaded is None or not self._has_interrupt(loaded[0]):
                continue
            s = loaded[1]
            if not (s.parsed_event and s.geo_context and s.risk and s.sitrep and s.sitrep_text):
                continue
            out.append(PendingReview(
                thread_id=m.thread_id, telemetry=s.raw_input, parsed_event=s.parsed_event, geo_context=s.geo_context,
                risk=s.risk, sitrep=s.sitrep, sitrep_text=s.sitrep_text, review_history=s.review_history,
            ))
        return out

    # -- HITL resume ----------------------------------------------------------------
    async def review(self, req: ReviewActionRequest) -> ReviewActionResponse:
        lock = self._locks.setdefault(req.thread_id, asyncio.Lock())
        async with lock:
            loaded = await self.load(req.thread_id)
            if loaded is None:
                raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown thread {req.thread_id}")
            snap, state = loaded
            mission = self.missions.get(req.thread_id)
            if not self._has_interrupt(snap) or (mission and mission.status != WorkflowStatus.AWAITING_REVIEW):
                raise HTTPException(status.HTTP_409_CONFLICT, "thread is not awaiting review")
            if req.decision in (ReviewDecision.REJECT, ReviewDecision.REVISE) and not (req.notes or "").strip():
                raise HTTPException(422, f"notes are required to {req.decision.value}")
            revisions = sum(1 for r in state.review_history if r.decision == ReviewDecision.REVISE)
            if req.decision == ReviewDecision.REVISE and revisions >= MAX_REVISIONS:
                raise HTTPException(status.HTTP_409_CONFLICT, f"revision limit ({MAX_REVISIONS}) reached")

            record = ReviewRecord(decision=req.decision, operator_id=req.operator_id.strip(),
                                  notes=(req.notes or "").strip() or None)
            if mission:  # mark RUNNING inside the lock → a duplicate request gets 409
                self.missions[req.thread_id] = mission.model_copy(update={
                    "status": WorkflowStatus.RUNNING, "current_node": "hitl_checkpoint", "updated_at": utcnow()})
            self._spawn(req.thread_id, Command(resume=record.model_dump(mode="json")))
        return ReviewActionResponse(thread_id=req.thread_id, decision=req.decision, status=WorkflowStatus.RUNNING)

    def health(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for m in self.missions.values():
            counts[m.status.value] = counts.get(m.status.value, 0) + 1
        return {
            "status": "ok",
            "llm": self.agents.llm.label,
            "geo_engine": self.agents.geospatial.engine,
            "checkpointer": self.checkpointer_kind,
            "transport": "kafka" if self.kafka_enabled else "direct",
            "active_runs": len(self._tasks),
            "missions": counts,
            "auth": bool(settings.api_key),
        }


# ═════════════════════════════════════════════════════════════════════════════
# App lifespan — build infra, graph and service
# ═════════════════════════════════════════════════════════════════════════════


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        pool = None
        serde = build_checkpoint_serde()
        if settings.database_url:
            # Fail fast: if Postgres is configured but unreachable, don't silently lose durability.
            from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
            from psycopg.rows import dict_row
            from psycopg_pool import AsyncConnectionPool

            pool = AsyncConnectionPool(
                settings.database_url, open=False, min_size=1, max_size=10,
                kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
            )
            await pool.open(wait=True, timeout=15)
            stack.push_async_callback(pool.close)
            checkpointer: Any = AsyncPostgresSaver(pool, serde=serde)
            await checkpointer.setup()
            kind = "postgres"
        else:
            checkpointer, kind = MemorySaver(serde=serde), "memory"

        agents = SenaniAgents(LLMGateway(), geo_pool=pool)
        await agents.geospatial.seed()
        service = SenaniService(build_graph(agents, checkpointer), checkpointer, agents, EventBroker(), kind)
        await service.start()
        stack.push_async_callback(service.shutdown)
        app.state.service = service
        log.info("Sarvatra Senani online — %s", service.health())
        yield


app = FastAPI(
    title="Sarvatra Senani",
    description="Multi-agent battlefield decision support engine (LangGraph + HITL).",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware, allow_origins=settings.cors_origins, allow_credentials=False,
    allow_methods=["GET", "POST"], allow_headers=["Content-Type", "X-API-Key"],
)


def svc() -> SenaniService:
    return app.state.service


async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """Optional shared-secret auth; enabled when SENANI_API_KEY is set."""
    if settings.api_key and not (x_api_key and secrets.compare_digest(x_api_key, settings.api_key)):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing X-API-Key")


auth = [Depends(require_api_key)]


# ═════════════════════════════════════════════════════════════════════════════
# REST endpoints
# ═════════════════════════════════════════════════════════════════════════════


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return svc().health()


@app.get("/api/reference-data", dependencies=auth)
async def get_reference_data() -> dict[str, Any]:
    return reference_data()


@app.post("/api/trigger-telemetry", response_model=TriggerResponse,
          status_code=status.HTTP_202_ACCEPTED, dependencies=auth)
async def trigger_telemetry(telemetry: Telemetry) -> TriggerResponse:
    """Ingest a tactical report and start a new workflow run (async).

    Progress is streamed over /ws/stream; the response returns immediately
    with the thread_id that identifies the run everywhere else.
    """
    return await svc().submit(telemetry)


@app.get("/api/pending-reviews", response_model=list[PendingReview], dependencies=auth)
async def pending_reviews() -> list[PendingReview]:
    """SITREPs paused at the HITL checkpoint, highest risk first."""
    return await svc().pending_reviews()


@app.post("/api/review-action", response_model=ReviewActionResponse,
          status_code=status.HTTP_202_ACCEPTED, dependencies=auth)
async def review_action(req: ReviewActionRequest) -> ReviewActionResponse:
    """Operator sign-off: approve / reject finalise the run; revise loops it
    back to the SITREP agent with the operator's notes."""
    return await svc().review(req)


@app.get("/api/missions", response_model=list[MissionSummary], dependencies=auth)
async def list_missions(limit: int = Query(200, ge=1, le=1000)) -> list[MissionSummary]:
    return sorted(svc().missions.values(), key=lambda m: m.created_at, reverse=True)[:limit]


@app.get("/api/missions/{thread_id}", dependencies=auth)
async def get_mission(thread_id: str) -> dict[str, Any]:
    loaded = await svc().load(thread_id)
    if loaded is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown thread {thread_id}")
    snap, state = loaded
    return {"state": jsonable_encoder(state), "next": list(snap.next),
            "awaiting_review": SenaniService._has_interrupt(snap)}


# ═════════════════════════════════════════════════════════════════════════════
# WebSocket stream
# ═════════════════════════════════════════════════════════════════════════════


@app.websocket("/ws/stream")
async def ws_stream(ws: WebSocket, api_key: str | None = Query(default=None)) -> None:
    """Live feed for the dashboard: snapshot on connect, then log / mission /
    review_required / finalized events. Pings every 20 s keep proxies happy
    and detect dead clients."""
    if settings.api_key and not (api_key and secrets.compare_digest(api_key, settings.api_key)):
        await ws.close(code=4401)
        return
    await ws.accept()
    service = svc()
    queue = service.broker.subscribe()
    try:
        await ws.send_json(jsonable_encoder(StreamEvent(type="snapshot", data={
            "health": service.health(),
            "missions": sorted(service.missions.values(), key=lambda m: m.created_at, reverse=True)[:200],
            "logs": list(service.broker.recent_logs),
        })))
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=20)
            except asyncio.TimeoutError:
                event = jsonable_encoder(StreamEvent(type="ping", data={"health": service.health()}))
            await ws.send_json(event)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        service.broker.unsubscribe(queue)


if __name__ == "__main__":
    import uvicorn

    server = uvicorn.Server(uvicorn.Config(app, host=settings.host, port=settings.port, log_level="info"))
    if sys.platform == "win32":
        # psycopg's async driver cannot run on the default Proactor loop.
        with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:
            runner.run(server.serve())
    else:
        asyncio.run(server.serve())
