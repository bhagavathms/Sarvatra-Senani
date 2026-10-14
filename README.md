# Sarvatra Senani: Multi-Agent Battlefield Decision Support Engine

**Sarvatra Senani** ingests tactical telemetry (JSON sensor feeds, radio transcripts, free-text and OSINT reports) and runs it through a pipeline of AI agents. The agents normalize the report, analyze geospatial proximity, score risk and logistics, and draft a standardized military **SITREP**. The workflow then **pauses at a human-in-the-loop (HITL) checkpoint** until an operator approves, rejects or sends the report back for revision from a dark-mode command-center dashboard.

> All reference data (boundary, infrastructure, friendly positions, zones) is **notional exercise data**. This is a decision-*support* system: no output leaves the pipeline without operator sign-off.

---

## Architecture

```
                         ┌───────────────────────── LangGraph StateGraph (checkpointed) ─────────────────────────┐
 Sensors / C2 ──► Kafka  │                                                                                        │
 senani.telemetry ──┐    │  ingestion ─► geospatial ─► risk ─► sitrep ─► queue_review ─► hitl_checkpoint ─► finalize
                    ├──► │      │            │          │        ▲   └──(below threshold)─────────────────►┘   ▲      │
 POST /api/trigger ─┘    │      └──(failed)──┴──────────┴─► END  └────────────── revise (operator notes) ─────┘      │
                         └───────────────────────────────────────┬──────────────────────────────────────────┬────────┘
                                                                 │ node updates                             │ interrupt()
                                                                 ▼                                          ▼
                                     EventBroker ──► WebSocket /ws/stream ──► React dashboard ──► POST /api/review-action
                                                └──► Kafka senani.events (downstream C2)              (Command(resume=…))
```

| Agent | Responsibility | Implementation |
|---|---|---|
| **Ingestion & Parsing** | Normalize heterogeneous feeds into a SALUTE-aligned `ParsedEvent` | Alias mapping for JSON keys (`lat`/`latitude`/`position.lat`…), regex extraction for coordinates (`27.18N 70.69E`, `27.31, 71.38`), heading, speed and unit size. Optional LLM structured extraction for prose; regex coordinates always win over LLM coordinates, which can be hallucinated. |
| **Geospatial & Context** | Distance and bearing to the boundary, critical infrastructure, friendly units; restricted-zone containment; sector; H3 cell | **PostGIS** (`ST_Distance`, `ST_DWithin`, `ST_ClosestPoint`, `ST_Covers` on `geography`) when `DATABASE_URL` is set; otherwise an in-memory haversine engine with the same semantics. |
| **Risk & Logistics** | 0–100 severity score, severity band, QRF ETA, time-to-boundary for closing tracks, readiness posture, recommended actions | Deterministic weighted model. Each factor is returned with its contribution so the score can be explained line by line. |
| **SITREP Generator** | Standard SITREP (precedence, DTG, SALUTE, assessment, actions, logistics) | The LLM writes only the prose sections. Numbers and recommended actions come from the deterministic engine, so what the operator signs off on can be audited. |
| **HITL checkpoint** | Pause until operator sign-off | `langgraph.types.interrupt()`; resumed with `Command(resume=ReviewRecord)`. Approve or reject finalizes the run. Revise loops back to the SITREP agent with the operator's notes (max 3 times). |

### Key design decisions

- **The LLM is never on the critical path.** With `LLM_PROVIDER=none` (the default), the whole system runs deterministically. When an LLM is configured, timeouts, provider errors and schema violations fall back to the heuristic or template path, and a warning appears in the agent log.
- **Durable pauses.** With Postgres configured, the HITL interrupt is persisted by `AsyncPostgresSaver`. A paused SITREP survives a backend restart and can be approved from a different process. The mission registry is rebuilt from checkpoints at startup.
- **Safe checkpoint deserialization.** The checkpointer uses an explicit msgpack allowlist (`models.CHECKPOINT_TYPES`) instead of LangGraph's permissive default.
- **Prompt-injection hygiene.** Report text is fenced as untrusted data in every prompt, and the LLM output only fills narrative fields of a validated schema.
- **Concurrency safety.** Runs are bounded by a semaphore. Review actions take a per-thread lock and re-check the interrupt, so a double-clicked "Approve" returns `409` instead of resuming twice.
- **Kafka is optional.** Without it, the trigger endpoint calls the same `_drive()` code path directly. If Kafka is configured but unreachable, the backend logs an error and falls back to direct ingestion rather than refusing to start.

---

## Project structure

```
backend/
  models.py        Pydantic v2 schemas: Telemetry, ParsedEvent, GeospatialContext, RiskAssessment, SITREP, graph state, API contracts
  agents.py        LLM gateway, reference data, the four agents, PostGIS + in-memory geospatial engines
  graph.py         LangGraph StateGraph, failure routing, HITL interrupt, revision loop
  main.py          FastAPI app, WebSocket stream, Kafka consumer/producer, workflow runner
  requirements.txt
frontend/
  src/App.jsx      Entire dashboard (stream hook, SVG tactical map, pipeline tracker, log stream, review console)
  (main.jsx, index.css, index.html, vite/tailwind/postcss configs: build scaffolding only)
docker-compose.yml PostGIS 16 + Kafka 3.8 (KRaft) + topic bootstrap
docs/
  EXECUTION_GUIDE.md  Run, verify, demo and troubleshoot the system
```

---

## Quick start

> **Full instructions:** [docs/EXECUTION_GUIDE.md](docs/EXECUTION_GUIDE.md) covers verification steps, Kafka and PostGIS checks, a 5-minute demo script, and troubleshooting.

### Prerequisites
Python 3.11+, Node 18+, and Docker Desktop **running** (only for the full stack).

### Option A: zero infrastructure (in-memory)

**PowerShell (Windows)**
```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py                   # http://localhost:8000  (OpenAPI docs at /docs)
```

**bash (macOS/Linux)**
```bash
cd backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

**Frontend** (second terminal):
```bash
cd frontend
npm install
npm run dev                      # http://localhost:5173
```

Open the dashboard and click a preset (for example **Indirect fire**), then **Transmit to pipeline**. Watch the agents run in the log, and the SITREP appear in the HITL queue for sign-off.

### Option B: full stack (PostGIS + Kafka)

```bash
docker compose up -d             # waits for health checks and creates topics
```

Then start the backend with the infrastructure variables set:

```powershell
# PowerShell
$env:DATABASE_URL = "postgresql://senani:senani@localhost:5432/senani"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
python main.py
```
```bash
# bash
export DATABASE_URL="postgresql://senani:senani@localhost:5432/senani"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
python main.py
```

The header pills should read `GEO POSTGIS · STATE POSTGRES · BUS KAFKA`. The PostGIS reference layer is created and seeded automatically on startup. Paused reviews now survive a backend restart.

> **Windows note:** psycopg's async driver cannot use the Proactor event loop. Start the backend with `python main.py`, which selects a compatible loop, rather than `uvicorn main:app`.

### Enabling an LLM

```powershell
# PowerShell (bash: export VAR="value")
$env:LLM_PROVIDER = "anthropic"          # anthropic | openai | ollama | none
$env:ANTHROPIC_API_KEY = "<your key>"    # OPENAI_API_KEY for openai; OLLAMA_BASE_URL for ollama
$env:LLM_MODEL = "claude-sonnet-5-5"     # optional; defaults per provider are in agents.DEFAULT_MODELS
python main.py
```

The LLM only writes free-text extraction and SITREP prose. If it fails or times out, the pipeline falls back to deterministic output and logs a warning.

---

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `LLM_PROVIDER` | `none` | `none`, `openai`, `anthropic`, `ollama` |
| `LLM_MODEL` | per provider | Model id override |
| `LLM_TIMEOUT_S` | `30` | Per-call timeout before falling back to deterministic output |
| `DATABASE_URL` | unset | Enables the Postgres checkpointer + PostGIS geospatial engine |
| `KAFKA_BOOTSTRAP_SERVERS` | unset | Enables Kafka ingestion + event publishing |
| `KAFKA_TELEMETRY_TOPIC` / `KAFKA_EVENTS_TOPIC` | `senani.telemetry` / `senani.events` | Topic names |
| `HITL_SCORE_THRESHOLD` | `0` | Reports scoring below this are auto-released. `0` means every SITREP is reviewed; CRITICAL is always reviewed. |
| `INFRA_SEARCH_RADIUS_KM` | `25` | Infrastructure proximity search radius |
| `SENANI_API_KEY` | unset | If set, requires the `X-API-Key` header (WebSocket: `?api_key=`). The frontend reads `VITE_API_KEY`. |
| `CORS_ORIGINS` | `http://localhost:5173` | Comma-separated list |
| `MAX_CONCURRENT_RUNS` | `8` | Concurrent graph executions |

---

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/trigger-telemetry` | Body: `Telemetry`. Starts a workflow and returns `202 {thread_id, status, transport}` immediately. |
| `GET` | `/api/pending-reviews` | SITREPs paused at the HITL checkpoint, highest risk first, with full context. |
| `POST` | `/api/review-action` | `{thread_id, decision: approve\|reject\|revise, operator_id, notes}`. Resumes the paused graph. Notes are required to reject or revise. Returns `409` if the thread is not awaiting review. |
| `GET` | `/api/missions` · `/api/missions/{id}` | Registry summaries · full checkpointed state |
| `GET` | `/api/reference-data` | Map layers for the area of operations |
| `GET` | `/api/health` | Active engines and mission counts |
| `WS` | `/ws/stream` | `snapshot`, then `log` / `mission` / `review_required` / `finalized` / `ping` events |

Example telemetry:

```bash
curl -X POST localhost:8000/api/trigger-telemetry -H "Content-Type: application/json" -d '{
  "source_id": "BP-SPARROW", "source_type": "radio",
  "payload": "Possible infiltration, section of ~8 personnel crossing the fence near 27.18N 70.69E, moving east, approx 4 kph."
}'
```

Kafka producers can publish either a bare `Telemetry` JSON object or `{"thread_id": "...", "telemetry": {...}}` to `senani.telemetry`. Redelivered messages with an existing `thread_id` are skipped.

---

## Risk model

| Factor | Max pts | Normalization |
|---|---|---|
| Threat level | 35 | `(threat − 1) / 4` |
| Boundary proximity | 20 | linear, 0 at ≥ 30 km |
| Critical infrastructure exposure | 25 | `criticality/5 × (1 − d/20 km)`, worst site |
| Friendly force exposure | 10 | linear, 0 at ≥ 15 km |
| Event type weighting | 10 | artillery 1.0 … civil unrest 0.3 |
| Restricted zone | +8 | if inside |
| Source confidence | ×0.6–1.0 | weak reports are damped but never zeroed |

Bands: **LOW** < 30 ≤ **MODERATE** < 55 ≤ **HIGH** < 75 ≤ **CRITICAL**. Weights are module constants in `agents.py`.


## Limitations and next steps

- The mission registry is in process memory (rebuilt from checkpoints on start). Running multiple API replicas would need a shared registry, for example a Postgres table or Redis.
- The API key is a shared secret. A real deployment needs per-operator identity (OIDC) so `operator_id` is authenticated rather than self-asserted.
- Reference data is notional and seeded from code. Production would load it from authoritative GIS sources.
