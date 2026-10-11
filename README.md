# Sarvatra Senani: Multi-Agent Battlefield Decision Support Engine

**Sarvatra Senani** is a production-grade enterprise system designed to automate tactical telemetry ingestion, geospatial context analysis, risk evaluation, and Situation Report (SITREP) generation using a collaborative multi-agent architecture.

## Architecture & Workflow

* **Orchestration:** Built using graph-based state machines (LangGraph) or role-based engines (CrewAI) to coordinate a specialized workforce of autonomous AI agents.
* **Ingestion & Parsing:** Ingests live data streams (Apache Kafka) containing JSON telemetry, radio transcripts, and OSINT logs, normalizing them by region and threat level.
* **Geospatial & Context Analysis:** Integrates Python tools with PostGIS and Uber H3 spatial indexes to evaluate coordinate proximity ($\text{lat}, \text{lng}$) to borders, friendly lines, and critical infrastructure.
* **Risk & Logistical Grading:** Assesses historical patterns and current constraints to calculate severity scores.
* **SITREP Generation:** Synthesizes multi-agent outputs into standardized, time-stamped military Situation Reports with recommended actions.
* **Human-in-the-Loop (HITL) Checkpoint:** Pauses execution via state checkpoints to require operator review and sign-off on a command dashboard before alert finalization.

## Technology Stack

* **Orchestration:** LangGraph / CrewAI
* **LLM & Validation:** OpenAI / Claude API, Ollama (Llama 3), Pydantic for Structured Outputs
* **Backend & Streaming:** FastAPI, Apache Kafka, PostgreSQL / PostGIS
* **Frontend:** ReactJS, Tailwind CSS (Dark-mode command center UI)