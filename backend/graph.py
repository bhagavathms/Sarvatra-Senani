"""
Sarvatra Senani — LangGraph workflow, state management and HITL checkpoint.

Topology
--------

    START
      │
      ▼
  ingestion ──► geospatial ──► risk ──► sitrep ─┬─► queue_review ──► hitl_checkpoint ─┬─► finalize ──► END
      │             │            │         ▲     │                     (interrupt)     │
      └─(failed)────┴────────────┴──► END  │     └─(below HITL threshold)──► finalize  │
                                           └──────────────(decision = revise)─────────┘

* Every node is wrapped by ``_guarded``: an unexpected exception marks the
  run FAILED (with a log entry) instead of crashing the worker, and the
  router sends it straight to END.
* ``queue_review`` flips status to AWAITING_REVIEW *before* the interrupt.
  This is a separate node because LangGraph re-executes the interrupting node
  from the top on resume — anything written before ``interrupt()`` in the
  same node would be discarded.
* ``hitl_checkpoint`` calls ``langgraph.types.interrupt``. The run is
  persisted by the checkpointer and the worker is released. FastAPI resumes
  it later with ``Command(resume=<ReviewRecord>)`` — possibly in a different
  process, if the Postgres checkpointer is used.
* A "revise" decision loops back to the SITREP agent with the operator's
  notes, then re-enters review. Revisions are capped at MAX_REVISIONS.
"""

import functools
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.errors import GraphBubbleUp
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from agents import (
    AgentError,
    GeospatialAgent,
    IngestionAgent,
    LLMGateway,
    RiskAgent,
    SitrepAgent,
)
from models import (
    CHECKPOINT_TYPES,
    AgentLog,
    ReviewDecision,
    ReviewRecord,
    SenaniState,
    WorkflowStatus,
)

log = logging.getLogger("senani.graph")

INGESTION, GEOSPATIAL, RISK, SITREP_GEN = "ingestion", "geospatial", "risk", "sitrep"
QUEUE_REVIEW, HITL, FINALIZE = "queue_review", "hitl_checkpoint", "finalize"
PIPELINE_NODES = (INGESTION, GEOSPATIAL, RISK, SITREP_GEN, QUEUE_REVIEW, HITL, FINALIZE)

MAX_REVISIONS = 3


def build_checkpoint_serde() -> JsonPlusSerializer:
    """Checkpoint serializer that only revives our own model types.

    LangGraph's default is permissive (any importable class can be revived
    from the checkpoint store). An explicit allowlist closes that hole.
    """
    return JsonPlusSerializer(allowed_msgpack_modules=CHECKPOINT_TYPES)


class SenaniAgents:
    """Dependency container: one instance of each agent, shared by all runs."""

    def __init__(self, llm: LLMGateway, geo_pool: Any = None) -> None:
        self.llm = llm
        self.ingestion = IngestionAgent(llm)
        self.geospatial = GeospatialAgent(geo_pool)
        self.risk = RiskAgent()
        self.sitrep = SitrepAgent(llm)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

NodeFn = Callable[[SenaniState], Awaitable[dict[str, Any]]]


def _entry(agent: str, message: str, level: str = "info", t0: float | None = None) -> AgentLog:
    ms = round((time.perf_counter() - t0) * 1000, 1) if t0 is not None else None
    return AgentLog(agent=agent, message=message, level=level, duration_ms=ms)  # type: ignore[arg-type]


def _guarded(agent: str) -> Callable[[NodeFn], NodeFn]:
    """Convert agent exceptions into a FAILED state transition."""

    def deco(fn: NodeFn) -> NodeFn:
        @functools.wraps(fn)
        async def wrapper(state: SenaniState) -> dict[str, Any]:
            t0 = time.perf_counter()
            try:
                return await fn(state)
            except GraphBubbleUp:
                raise  # interrupts / commands are LangGraph control flow, not failures
            except AgentError as exc:
                msg = str(exc)
            except Exception as exc:
                log.exception("node %s crashed (thread %s)", agent, state.thread_id)
                msg = f"internal error: {type(exc).__name__}: {exc}"
            return {
                "status": WorkflowStatus.FAILED,
                "error": f"[{agent}] {msg}",
                "logs": [_entry(agent, f"FAILED — {msg}", "error", t0)],
            }

        return wrapper

    return deco


def _revisions(state: SenaniState) -> int:
    return sum(1 for r in state.review_history if r.decision == ReviewDecision.REVISE)


# ─────────────────────────────────────────────────────────────────────────────
# Graph factory
# ─────────────────────────────────────────────────────────────────────────────


def build_graph(agents: SenaniAgents, checkpointer: BaseCheckpointSaver):
    """Compile the Senani StateGraph bound to the given agents + checkpointer."""

    @_guarded(INGESTION)
    async def ingestion_node(state: SenaniState) -> dict[str, Any]:
        t0 = time.perf_counter()
        ev = await agents.ingestion.run(state.raw_input)
        logs = [_entry(
            INGESTION,
            f"Parsed {state.raw_input.source_type.value.upper()} report via {ev.parse_method}: "
            f"{ev.event_type.value} @ {ev.latitude:.4f},{ev.longitude:.4f} "
            f"(threat {ev.threat_level}/5, conf {ev.confidence:.0%})",
            t0=t0,
        )]
        logs += [_entry(INGESTION, w, "warn") for w in ev.warnings]
        return {"parsed_event": ev, "logs": logs}

    @_guarded(GEOSPATIAL)
    async def geospatial_node(state: SenaniState) -> dict[str, Any]:
        t0 = time.perf_counter()
        geo, notes = await agents.geospatial.run(state.parsed_event)  # type: ignore[arg-type]
        parts = [f"{geo.sector} [{geo.engine}]"]
        if geo.nearest_border:
            parts.append(f"border {geo.nearest_border.distance_km:.1f} km")
        if geo.nearest_friendly:
            parts.append(f"{geo.nearest_friendly.name} {geo.nearest_friendly.distance_km:.1f} km")
        parts.append(f"{len(geo.nearby_infrastructure)} infra site(s) in range")
        if geo.restricted_zone:
            parts.append(f"INSIDE {geo.restricted_zone}")
        logs = [_entry(GEOSPATIAL, " · ".join(parts), t0=t0)]
        logs += [_entry(GEOSPATIAL, n, "warn") for n in notes]
        return {"geo_context": geo, "logs": logs}

    @_guarded(RISK)
    async def risk_node(state: SenaniState) -> dict[str, Any]:
        t0 = time.perf_counter()
        risk = agents.risk.run(state.parsed_event, state.geo_context)  # type: ignore[arg-type]
        top = sorted(risk.factors, key=lambda f: f.contribution, reverse=True)[0]
        msg = (f"Score {risk.score:.1f}/100 → {risk.severity.value}; top driver: {top.name} "
               f"(+{top.contribution:.1f}); HITL {'REQUIRED' if risk.requires_human_approval else 'not required'}")
        level = "warn" if risk.severity.value in ("HIGH", "CRITICAL") else "info"
        return {"risk": risk, "requires_human_approval": risk.requires_human_approval,
                "logs": [_entry(RISK, msg, level, t0)]}

    @_guarded(SITREP_GEN)
    async def sitrep_node(state: SenaniState) -> dict[str, Any]:
        t0 = time.perf_counter()
        revision = _revisions(state)
        feedback = next((r.notes for r in reversed(state.review_history)
                         if r.decision == ReviewDecision.REVISE), None) if revision else None
        sitrep, notes = await agents.sitrep.run(
            state.raw_input, state.parsed_event, state.geo_context, state.risk,  # type: ignore[arg-type]
            revision=revision, operator_feedback=feedback,
            sitrep_id=state.sitrep.sitrep_id if state.sitrep else None,
        )
        logs = [_entry(SITREP_GEN, f"{sitrep.sitrep_id} drafted ({sitrep.precedence.value}"
                                   f"{f', rev {revision}' if revision else ''}) by {sitrep.generated_by}", t0=t0)]
        logs += [_entry(SITREP_GEN, n, "warn") for n in notes]
        return {"sitrep": sitrep, "sitrep_text": sitrep.render(), "status": WorkflowStatus.RUNNING, "logs": logs}

    async def queue_review_node(state: SenaniState) -> dict[str, Any]:
        return {"status": WorkflowStatus.AWAITING_REVIEW,
                "logs": [_entry(HITL, f"{state.sitrep.sitrep_id} queued — awaiting operator sign-off")]}  # type: ignore[union-attr]

    async def hitl_node(state: SenaniState) -> dict[str, Any]:
        # ── execution pauses here until /api/review-action resumes the thread ──
        decision = interrupt({
            "thread_id": state.thread_id,
            "sitrep_id": state.sitrep.sitrep_id if state.sitrep else None,
            "severity": state.risk.severity.value if state.risk else None,
            "score": state.risk.score if state.risk else None,
            "subject": state.sitrep.subject if state.sitrep else None,
        })
        record = ReviewRecord.model_validate(decision)
        msg = f"Operator {record.operator_id} → {record.decision.value.upper()}"
        if record.notes:
            msg += f": “{record.notes}”"
        level = "warn" if record.decision != ReviewDecision.APPROVE else "info"
        return {"review_history": [record], "status": WorkflowStatus.RUNNING, "logs": [_entry(HITL, msg, level)]}

    async def finalize_node(state: SenaniState) -> dict[str, Any]:
        last = state.review_history[-1] if state.review_history else None
        if last is None:
            status, msg = WorkflowStatus.AUTO_FINALIZED, "Below HITL threshold — SITREP auto-released to log"
        elif last.decision == ReviewDecision.APPROVE:
            status, msg = WorkflowStatus.APPROVED, f"SITREP APPROVED by {last.operator_id} — released for dissemination"
        elif last.decision == ReviewDecision.REVISE:
            status, msg = WorkflowStatus.REJECTED, f"Revision limit ({MAX_REVISIONS}) reached — SITREP closed as rejected"
        else:
            status, msg = WorkflowStatus.REJECTED, f"SITREP REJECTED by {last.operator_id} — not disseminated"
        return {"status": status, "logs": [_entry(FINALIZE, msg)]}

    # ── routers ──────────────────────────────────────────────────────────────
    def unless_failed(next_node: str) -> Callable[[SenaniState], str]:
        def route(state: SenaniState) -> str:
            return END if state.status == WorkflowStatus.FAILED else next_node
        return route

    def after_sitrep(state: SenaniState) -> str:
        if state.status == WorkflowStatus.FAILED:
            return END
        return QUEUE_REVIEW if state.requires_human_approval else FINALIZE

    def after_review(state: SenaniState) -> str:
        last = state.review_history[-1]
        if last.decision == ReviewDecision.REVISE and _revisions(state) <= MAX_REVISIONS:
            return SITREP_GEN
        return FINALIZE

    # ── wiring ───────────────────────────────────────────────────────────────
    g = StateGraph(SenaniState)
    g.add_node(INGESTION, ingestion_node)
    g.add_node(GEOSPATIAL, geospatial_node)
    g.add_node(RISK, risk_node)
    g.add_node(SITREP_GEN, sitrep_node)
    g.add_node(QUEUE_REVIEW, queue_review_node)
    g.add_node(HITL, hitl_node)
    g.add_node(FINALIZE, finalize_node)

    g.add_edge(START, INGESTION)
    g.add_conditional_edges(INGESTION, unless_failed(GEOSPATIAL), [GEOSPATIAL, END])
    g.add_conditional_edges(GEOSPATIAL, unless_failed(RISK), [RISK, END])
    g.add_conditional_edges(RISK, unless_failed(SITREP_GEN), [SITREP_GEN, END])
    g.add_conditional_edges(SITREP_GEN, after_sitrep, [QUEUE_REVIEW, FINALIZE, END])
    g.add_edge(QUEUE_REVIEW, HITL)
    g.add_conditional_edges(HITL, after_review, [SITREP_GEN, FINALIZE])
    g.add_edge(FINALIZE, END)

    return g.compile(checkpointer=checkpointer)
