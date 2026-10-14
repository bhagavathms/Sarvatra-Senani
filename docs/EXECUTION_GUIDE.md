# Sarvatra Senani: Execution Guide

Step-by-step instructions to install, run, verify, and demo the system, from zero-infrastructure mode up to the full PostGIS + Kafka stack.

Commands are shown for **Windows PowerShell** first and **macOS/Linux (bash)** second where they differ.

---

## Contents

1. [Prerequisites](#1-prerequisites)
2. [Mode A: Zero-infrastructure run (5 minutes)](#2-mode-a-zero-infrastructure-run)
3. [Mode B: Full stack with PostGIS + Kafka](#3-mode-b-full-stack-with-postgis--kafka)
4. [Enabling an LLM provider](#4-enabling-an-llm-provider)
5. [Verifying each component](#5-verifying-each-component)
6. [Demo walkthrough script](#6-demo-walkthrough-script)
7. [Optional: API key auth & production build](#7-optional-api-key-auth--production-build)
8. [Stopping & resetting](#8-stopping--resetting)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

| Tool | Version | Needed for | Check |
|---|---|---|---|
| Python | 3.11+ | Backend | `python --version` |
| Node.js | 18+ | Frontend | `node --version` |
| Docker Desktop | any recent | Mode B only | `docker info` (must show a server version) |
| Git | any | Cloning | `git --version` |

- Ports used: **8000** (backend), **5173** (frontend), **5432** (Postgres), **9092** (Kafka).
- **Docker Desktop must be running**, not just installed. `docker info` printing `error during connect` means the engine is not started.

---

## 2. Mode A: Zero-infrastructure run

Runs everything in memory with deterministic agents (no LLM, no Docker). Best for a first run and for demos on a laptop.

### Step 1: Backend

**PowerShell**
```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

**bash**
```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

Expected output (last lines):
```
INFO    senani.api: Sarvatra Senani online — {'status': 'ok', 'llm': 'deterministic', 'geo_engine': 'in-memory', 'checkpointer': 'memory', 'transport': 'direct', ...}
INFO:     Uvicorn running on http://0.0.0.0:8000
```

> If PowerShell blocks `Activate.ps1`, run once: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

### Step 2: Frontend (new terminal)

```powershell
cd frontend
npm install
npm run dev
```

Open **http://localhost:5173**. The header pills should show:
`LINK LIVE · LLM DETERMINISTIC · GEO IN-MEMORY · STATE MEMORY · BUS DIRECT`

### Step 3: First run

1. In **Inject telemetry**, click the **Indirect fire** preset.
2. Click **Transmit to pipeline**.
3. Watch:
   - **Agent pipeline** stages light up in sequence (ingestion → geospatial → risk → SITREP → HITL).
   - **Agent execution log** streams each agent's output with timings.
   - **Tactical picture** shows the event as a diamond near FUEL DEPOT DELTA.
   - **HITL review queue** gets a new item.
4. Click the item, read the SITREP, enter an **Operator ID**, and click **Approve**. The status becomes `APPROVED`.

> In Mode A, state is lost when the backend stops. That is expected; Mode B fixes it.

---

## 3. Mode B: Full stack with PostGIS + Kafka

### Step 1: Start infrastructure

From the repository root:
```powershell
docker compose up -d
docker compose ps
```

Wait until `senani-postgis` and `senani-kafka` show **healthy**, and `senani-kafka-init` has **exited (0)**. The first pull can take a few minutes.

Confirm the topics exist:
```powershell
docker logs senani-kafka-init
# → senani.events
#   senani.telemetry
```

### Step 2: Start the backend with infrastructure enabled

**PowerShell** (environment variables last for the current terminal session)
```powershell
cd backend
.\.venv\Scripts\Activate.ps1
$env:DATABASE_URL = "postgresql://senani:senani@localhost:5432/senani"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
python main.py
```

**bash**
```bash
cd backend
source .venv/bin/activate
export DATABASE_URL="postgresql://senani:senani@localhost:5432/senani"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
python main.py
```

> **Windows:** always start with `python main.py`, not `uvicorn main:app`. The Postgres async driver needs the selector event loop that `main.py` sets up.

Expected log lines:
```
PostGIS reference layer seeded (16 features)
Kafka enabled: consuming senani.telemetry, publishing senani.events
Sarvatra Senani online — {... 'geo_engine': 'postgis', 'checkpointer': 'postgres', 'transport': 'kafka' ...}
```

### Step 3: Start the frontend

Same as Mode A (`npm run dev`). The header pills should now read `GEO POSTGIS · STATE POSTGRES · BUS KAFKA`.

---

## 4. Enabling an LLM provider

The LLM is optional. It improves free-text extraction and SITREP prose; scores and actions stay deterministic either way. Install **only** the provider you use (all three are already in `requirements.txt`).

| Provider | Environment variables | Default model |
|---|---|---|
| Anthropic | `LLM_PROVIDER=anthropic`, `ANTHROPIC_API_KEY=...` | `claude-sonnet-5-5` |
| OpenAI | `LLM_PROVIDER=openai`, `OPENAI_API_KEY=...` | `gpt-4.1-mini` |
| Ollama (local) | `LLM_PROVIDER=ollama`, optional `OLLAMA_BASE_URL` (default `http://localhost:11434`) | `llama3.1:8b` |

**PowerShell example**
```powershell
$env:LLM_PROVIDER = "anthropic"
$env:ANTHROPIC_API_KEY = "<your key>"
# optional overrides
$env:LLM_MODEL = "claude-sonnet-5-5"
$env:LLM_TIMEOUT_S = "30"
python main.py
```

**Ollama example**
```powershell
ollama pull llama3.1:8b
$env:LLM_PROVIDER = "ollama"
python main.py
```

**Verify:**
- The header **LLM** pill shows `anthropic/claude-sonnet-5-5` (or similar).
- A text preset such as **Radio: infiltration** logs `Parsed RADIO report via llm`.
- If the LLM fails or times out, the log shows a warning (`LLM extraction unavailable; used heuristic parser`) and the pipeline continues. That is by design.

---

## 5. Verifying each component

### 5.1 Health & API (PowerShell)

```powershell
Invoke-RestMethod http://localhost:8000/api/health

$body = @{
  source_id   = "BP-SPARROW"
  source_type = "radio"
  payload     = "Possible infiltration, section of ~8 personnel crossing the fence near 27.18N 70.69E, moving east, approx 4 kph."
} | ConvertTo-Json
$run = Invoke-RestMethod http://localhost:8000/api/trigger-telemetry -Method Post -ContentType "application/json" -Body $body
$run   # → thread_id, status=running, transport

Invoke-RestMethod http://localhost:8000/api/pending-reviews | Select-Object thread_id, @{n="subject";e={$_.sitrep.subject}}

$review = @{ thread_id = $run.thread_id; decision = "approve"; operator_id = "MAJ TEST" } | ConvertTo-Json
Invoke-RestMethod http://localhost:8000/api/review-action -Method Post -ContentType "application/json" -Body $review

Invoke-RestMethod "http://localhost:8000/api/missions/$($run.thread_id)" | Select-Object -ExpandProperty state | Select-Object status
```

**bash equivalent**
```bash
curl -s localhost:8000/api/health
curl -s -X POST localhost:8000/api/trigger-telemetry -H "Content-Type: application/json" \
  -d '{"source_id":"BP-SPARROW","source_type":"radio","payload":"Possible infiltration near 27.18N 70.69E, moving east"}'
curl -s localhost:8000/api/pending-reviews
curl -s -X POST localhost:8000/api/review-action -H "Content-Type: application/json" \
  -d '{"thread_id":"<THREAD_ID>","decision":"approve","operator_id":"MAJ TEST"}'
```

Interactive API docs: **http://localhost:8000/docs**

### 5.2 Expected API behaviours

| Action | Expected |
|---|---|
| Trigger valid telemetry | `202` + `thread_id` |
| Trigger with empty payload | `422` |
| Reject or revise without `notes` | `422` |
| Approve the same thread twice | first `202`, second `409` |
| Review an unknown thread | `404` |
| Text report with no coordinates (**Ungeolocatable** preset) | Run ends `FAILED` with "could not geolocate report" |

### 5.3 PostGIS (Mode B)

```powershell
docker exec -it senani-postgis psql -U senani -d senani -c "SELECT category, count(*) FROM senani_ref_features GROUP BY category;"
docker exec -it senani-postgis psql -U senani -d senani -c "\dt"
```
- Expect four categories (`border`, `infrastructure`, `friendly_unit`, `restricted_zone`).
- Expect LangGraph tables: `checkpoints`, `checkpoint_writes`, `checkpoint_blobs`, `checkpoint_migrations`.

### 5.4 Kafka (Mode B)

**Watch outbound events** (new terminal):
```powershell
docker exec -it senani-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic senani.events
```

**Publish telemetry straight to Kafka**, bypassing the API, as an external sensor would:
```powershell
docker exec -it senani-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic senani.telemetry
```
Then paste one line and press Enter:
```json
{"thread_id":"OP-KAFKA-001","telemetry":{"source_id":"KAFKA-SENSOR","source_type":"text","payload":"Quadcopter sighted over 26.72N 71.02E, probable, heading north east"}}
```
- The track `OP-KAFKA-001` appears on the dashboard.
- Publishing the same line again is ignored (`Skipping duplicate telemetry`), because the `thread_id` already has state.

> The consumer starts at the **latest** offset. Start the backend before producing test messages.

### 5.5 Durable HITL across restarts (Mode B)

This demonstrates the key persistence feature:
1. Transmit any preset and leave it **awaiting review**.
2. Stop the backend (`Ctrl+C`) and start it again with the same environment variables.
3. The log shows `Rehydrated N mission(s) from postgres checkpointer`.
4. The dashboard reconnects automatically, and the review is still in the queue. Approve it; the paused graph resumes and finalizes.

---

## 6. Demo walkthrough script

A 5-minute live demo (Mode A is enough; Mode B adds step 7).

1. **Set the scene (30 s).** Point at the header pills and the classification banner. "Every report becomes a standardized SITREP; nothing is released without sign-off."
2. **Structured feed (60 s).** Transmit **Indirect fire**.
   - Narrate the pipeline lighting up, the log timings, and the diamond appearing next to the fuel depot.
   - Open the **Risk** tab: "the score is explainable; here is each factor's contribution."
3. **Messy radio report (45 s).** Transmit **Radio: infiltration**.
   - In the **Context** tab, show the parsed SALUTE fields from free text: coordinates, headcount, heading.
   - Point out that the event lies **inside NO-GO ZONE DUNE-1**, which adds a risk bonus.
4. **Failure handling (20 s).** Transmit **Ungeolocatable**. It fails cleanly at ingestion: "it refuses to guess a location."
5. **Revise loop (60 s).**
   - On the infiltration SITREP, enter notes such as "Emphasise proximity to BORDER POST SPARROW" and click **Revise**.
   - The SITREP agent re-runs, the same SITREP ID returns as **REV 1**, and the operator notes are incorporated.
6. **Approve and reject (30 s).**
   - Approve one SITREP and reject another (notes are required). Show the stamps and the review history.
   - Try double-approving through the API to show the `409`.
7. **(Mode B) Restart survival (45 s).** Do §5.5 live.

---

## 7. Optional: API key auth & production build

### API key

```powershell
# backend terminal
$env:SENANI_API_KEY = "change-me"
python main.py

# frontend terminal (Vite reads VITE_* at startup)
$env:VITE_API_KEY = "change-me"
npm run dev
```
- REST calls then require the `X-API-Key` header. The WebSocket requires `?api_key=`, which the dashboard adds automatically.
- `/api/health` stays open for liveness probes.

### Other settings

| Variable | Default | Effect |
|---|---|---|
| `HITL_SCORE_THRESHOLD` | `0` | Scores below this are auto-released (CRITICAL is always reviewed) |
| `MAX_CONCURRENT_RUNS` | `8` | Parallel graph executions |
| `INFRA_SEARCH_RADIUS_KM` | `25` | Infrastructure search radius |
| `CORS_ORIGINS` | `http://localhost:5173` | Allowed browser origins (comma-separated) |
| `PORT` / `HOST` | `8000` / `0.0.0.0` | Backend bind address |
| `SENANI_BACKEND` | `http://127.0.0.1:8000` | Backend target for the Vite dev proxy |

### Production frontend build

```powershell
cd frontend
npm run build      # outputs frontend/dist
```
- Serve `dist/` from any static server (nginx, Caddy).
- Reverse-proxy `/api` and `/ws` (with WebSocket upgrade) to the backend.
- Or set `VITE_API_BASE=https://your-backend` at build time and add that origin to `CORS_ORIGINS`.

---

## 8. Stopping & resetting

| Goal | Command |
|---|---|
| Stop backend / frontend | `Ctrl+C` in each terminal |
| Stop infrastructure, keep data | `docker compose down` |
| Stop infrastructure and **wipe all data** (checkpoints, reference layer, Kafka topics) | `docker compose down -v` |
| Clear in-memory state (Mode A) | Restart the backend |

---

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Header shows `LINK DOWN`; Vite logs `ECONNREFUSED ::1:8000` | Proxy targeting IPv6 `localhost` while the backend listens on IPv4 | Keep the default `127.0.0.1` target in `vite.config.js`, or set `SENANI_BACKEND=http://127.0.0.1:8000` |
| `Psycopg cannot use the 'ProactorEventLoop'` | Started with `uvicorn main:app` on Windows | Start with `python main.py` |
| `docker: error during connect … dockerDesktopLinuxEngine` | Docker Desktop not running | Start Docker Desktop and wait for "Engine running" |
| Backend exits on startup with a pool timeout | `DATABASE_URL` set but Postgres unreachable (deliberate fail-fast) | `docker compose ps`; wait for `healthy`; check port 5432 isn't used by a local Postgres |
| Log says `Kafka unavailable … falling back to direct ingestion` | Kafka not ready or wrong bootstrap address | Wait for `senani-kafka` to be healthy, then restart the backend. The system still works in direct mode meanwhile |
| `postgis/postgis` image fails on Apple Silicon | No arm64 image | Use `imresamu/postgis:16-3.4` in `docker-compose.yml` |
| Port already in use (8000 / 5173 / 5432 / 9092) | Another process | Set `PORT=8001` (and `SENANI_BACKEND=http://127.0.0.1:8001`), or stop the other service |
| `401 invalid or missing X-API-Key` | `SENANI_API_KEY` set on backend only | Set the same `VITE_API_KEY` and restart `npm run dev` |
| LLM pill shows `deterministic` despite `LLM_PROVIDER` set | Provider package missing or the variable was set in a different terminal | `pip install langchain-<provider>`; set the variables in the terminal that runs `python main.py` |
| `LLM provider error` warnings in the log | Invalid key, network, or rate limit | Check the key or quota. The pipeline keeps running on the deterministic path |
| A run shows `FAILED` | Agent error (commonly: no coordinates in a text report) | Open the track; the error message names the agent and the reason |
| `set X=Y` in PowerShell has no effect | `set` is cmd syntax | Use `$env:X = "Y"` |
