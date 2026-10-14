"""
Sarvatra Senani — data contracts.

Every piece of data that crosses an agent boundary, the LangGraph state, or the
HTTP/WebSocket API is defined here as a strict Pydantic v2 model. Keeping the
contracts in one module means:

  * the LangGraph checkpointer can be given an explicit deserialization
    allowlist (see ``CHECKPOINT_TYPES`` at the bottom of this file);
  * LLM structured outputs are validated against the same schemas the
    deterministic code paths produce, so downstream agents never see
    malformed data regardless of where it came from.

Pipeline data flow:

    Telemetry ──► ParsedEvent ──► GeospatialContext ──► RiskAssessment ──► SITREP
       (raw)       (Ingestion)      (Geospatial)          (Risk)          (SITREP Gen)
                                                                              │
                                                     ReviewRecord ◄── HITL checkpoint
"""

import json
import operator
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

MAX_TEXT_PAYLOAD_CHARS = 8_000
MAX_JSON_PAYLOAD_BYTES = 32_768


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:10].upper()}"


# ─────────────────────────────────────────────────────────────────────────────
# Enumerations
# ─────────────────────────────────────────────────────────────────────────────


class SourceType(str, Enum):
    JSON = "json"  # structured sensor / C2 feed
    TEXT = "text"  # free-text field report
    RADIO = "radio"  # radio transcript (speech-to-text output)
    OSINT = "osint"  # open-source intelligence snippet


class EventType(str, Enum):
    TROOP_MOVEMENT = "troop_movement"
    ARMORED_COLUMN = "armored_column"
    ARTILLERY = "artillery"
    AIR_ACTIVITY = "air_activity"
    UAS = "uas"
    IED = "ied"
    INFILTRATION = "infiltration"
    ELECTRONIC_WARFARE = "electronic_warfare"
    LOGISTICS_DISRUPTION = "logistics_disruption"
    CIVIL_UNREST = "civil_unrest"
    UNKNOWN = "unknown"


class SeverityLevel(str, Enum):
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class Precedence(str, Enum):
    """Military message precedence, derived from severity."""

    ROUTINE = "ROUTINE"
    PRIORITY = "PRIORITY"
    IMMEDIATE = "IMMEDIATE"
    FLASH = "FLASH"


class WorkflowStatus(str, Enum):
    RUNNING = "running"
    AWAITING_REVIEW = "awaiting_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    AUTO_FINALIZED = "auto_finalized"  # below the HITL threshold
    FAILED = "failed"


class ReviewDecision(str, Enum):
    APPROVE = "approve"
    REJECT = "reject"
    REVISE = "revise"  # send back to the SITREP agent with operator notes


# ─────────────────────────────────────────────────────────────────────────────
# Stage 0 — raw input
# ─────────────────────────────────────────────────────────────────────────────


class Telemetry(BaseModel):
    """A raw tactical report as it arrives from Kafka or the REST API."""

    model_config = ConfigDict(extra="forbid")

    source_id: str = Field(min_length=1, max_length=64, description="Sensor / unit / feed identifier")
    source_type: SourceType = SourceType.JSON
    payload: dict[str, Any] | str = Field(description="Structured JSON object or free text")
    received_at: datetime = Field(default_factory=utcnow)
    classification: str = Field(default="UNCLASSIFIED//EXERCISE", max_length=64)

    @field_validator("payload")
    @classmethod
    def _bound_payload(cls, v: dict[str, Any] | str) -> dict[str, Any] | str:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                raise ValueError("payload text is empty")
            if len(v) > MAX_TEXT_PAYLOAD_CHARS:
                raise ValueError(f"text payload exceeds {MAX_TEXT_PAYLOAD_CHARS} characters")
        elif len(json.dumps(v, default=str)) > MAX_JSON_PAYLOAD_BYTES:
            raise ValueError(f"JSON payload exceeds {MAX_JSON_PAYLOAD_BYTES} bytes")
        return v


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Ingestion & Parsing Agent output
# ─────────────────────────────────────────────────────────────────────────────


class ExtractedEvent(BaseModel):
    """LLM-facing extraction schema for free-text reports.

    Deliberately permissive (everything optional) — the Ingestion agent merges
    this with regex heuristics and then validates into a strict ParsedEvent.
    """

    event_type: EventType = Field(description="Best matching event category")
    latitude: float | None = Field(default=None, description="Decimal degrees, north positive")
    longitude: float | None = Field(default=None, description="Decimal degrees, east positive")
    threat_level: int | None = Field(default=None, ge=1, le=5, description="1=negligible … 5=imminent danger")
    confidence: float | None = Field(default=None, ge=0, le=1, description="Reliability of the report")
    unit_size: str | None = Field(default=None, description="e.g. 'section', 'platoon (~30)', 'company'")
    activity: str | None = Field(default=None, description="What the observed force is doing")
    equipment: list[str] = Field(default_factory=list, description="Weapons / vehicles observed")
    heading_deg: float | None = Field(default=None, ge=0, lt=360)
    speed_kph: float | None = Field(default=None, ge=0)
    summary: str = Field(description="One-sentence normalized description of the event")


class ParsedEvent(BaseModel):
    """Normalized, validated tactical event (SALUTE-aligned)."""

    event_id: str = Field(default_factory=lambda: new_id("EVT"))
    event_type: EventType
    summary: str = Field(max_length=600)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    threat_level: int = Field(ge=1, le=5)
    confidence: float = Field(ge=0, le=1)
    # SALUTE: Size, Activity, Location, Unit, Time, Equipment
    unit_size: str | None = None
    activity: str | None = None
    equipment: list[str] = Field(default_factory=list)
    heading_deg: float | None = Field(default=None, ge=0, lt=360)
    speed_kph: float | None = Field(default=None, ge=0, le=3000)
    observed_at: datetime
    source_id: str
    source_type: SourceType
    parse_method: Literal["structured", "llm", "heuristic"]
    warnings: list[str] = Field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Geospatial & Context Agent output
# ─────────────────────────────────────────────────────────────────────────────


class ProximityHit(BaseModel):
    name: str
    category: Literal["border", "infrastructure", "friendly_unit"]
    subtype: str
    distance_km: float = Field(ge=0)
    bearing_deg: float = Field(ge=0, lt=360, description="Bearing FROM the event TO this feature")
    criticality: int = Field(default=3, ge=1, le=5)
    latitude: float
    longitude: float


class GeospatialContext(BaseModel):
    grid_ref: str
    h3_cell: str | None = None
    sector: str
    nearest_border: ProximityHit | None = None
    nearby_infrastructure: list[ProximityHit] = Field(default_factory=list)
    nearest_friendly: ProximityHit | None = None
    restricted_zone: str | None = Field(default=None, description="Name of restricted zone containing the event")
    inside_ao: bool = True
    engine: Literal["postgis", "in-memory"]


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Risk & Logistical Analysis Agent output
# ─────────────────────────────────────────────────────────────────────────────


class RiskFactor(BaseModel):
    """One explainable term of the severity score."""

    name: str
    weight: float = Field(description="Max points this factor can contribute")
    raw: float = Field(ge=0, le=1, description="Normalized factor value")
    contribution: float = Field(description="Points actually contributed")
    detail: str


class RiskAssessment(BaseModel):
    score: float = Field(ge=0, le=100)
    severity: SeverityLevel
    factors: list[RiskFactor]
    time_to_border_min: float | None = Field(default=None, description="If the track is closing on the border")
    friendly_response_eta_min: float | None = None
    readiness_posture: str
    recommended_actions: list[str]
    requires_human_approval: bool


# ─────────────────────────────────────────────────────────────────────────────
# Stage 4 — SITREP Generation Agent output
# ─────────────────────────────────────────────────────────────────────────────


class SitrepNarrative(BaseModel):
    """LLM-facing schema: only the prose sections are model-generated.

    Scores, distances and recommended actions stay deterministic so that the
    numbers an operator signs off on are always auditable.
    """

    situation: str = Field(description="2-3 sentence overview of what happened, where and when")
    enemy_assessment: str = Field(description="Assessment of likely intent and capability of the observed force")
    friendly_situation: str = Field(description="Exposure of friendly units and infrastructure")
    assessment: str = Field(description="Commander-level bottom line: why this severity, what to watch")


class SITREP(BaseModel):
    sitrep_id: str = Field(default_factory=lambda: new_id("SITREP"))
    dtg: str = Field(description="Date-time group, e.g. 141830ZOCT26")
    classification: str
    precedence: Precedence
    originator: str = "SARVATRA SENANI DSS"
    addressee: str = "OPS ROOM / G3"
    subject: str
    situation: str
    enemy: str
    salute: dict[str, str]
    friendly: str
    location: str
    assessment: str
    recommended_actions: list[str]
    logistics: str
    generated_by: str
    revision: int = 0
    operator_feedback: str | None = None

    def render(self) -> str:
        """Standard plain-text SITREP layout for signals traffic / printing."""
        bar = "=" * 72
        salute = "\n".join(f"     {k:<10}: {v}" for k, v in self.salute.items())
        actions = "\n".join(f"     {chr(97 + i)}. {a}" for i, a in enumerate(self.recommended_actions))
        rev = f" (REV {self.revision})" if self.revision else ""
        lines = [
            bar,
            f"CLASSIFICATION: {self.classification}",
            f"PRECEDENCE:     {self.precedence.value}",
            f"DTG:            {self.dtg}",
            f"FROM:           {self.originator}",
            f"TO:             {self.addressee}",
            f"SUBJECT:        {self.subject}{rev}",
            f"REF:            {self.sitrep_id}",
            bar,
            "1. SITUATION",
            f"   {self.situation}",
            "",
            "2. ENEMY / OBSERVED FORCES",
            f"   {self.enemy}",
            "   SALUTE:",
            salute,
            "",
            "3. FRIENDLY FORCES",
            f"   {self.friendly}",
            "",
            "4. LOCATION",
            f"   {self.location}",
            "",
            "5. ASSESSMENT",
            f"   {self.assessment}",
            "",
            "6. RECOMMENDED ACTIONS",
            actions,
            "",
            "7. LOGISTICS / SUSTAINMENT",
            f"   {self.logistics}",
            "",
            "8. COMMAND & SIGNAL",
            f"   Generated by {self.generated_by}. Decision support only — requires operator sign-off.",
        ]
        if self.operator_feedback:
            lines += ["", "OPERATOR NOTES INCORPORATED:", f"   {self.operator_feedback}"]
        lines.append(bar)
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# HITL + observability
# ─────────────────────────────────────────────────────────────────────────────


class ReviewRecord(BaseModel):
    decision: ReviewDecision
    operator_id: str = Field(min_length=1, max_length=64)
    notes: str | None = Field(default=None, max_length=2_000)
    decided_at: datetime = Field(default_factory=utcnow)


class AgentLog(BaseModel):
    ts: datetime = Field(default_factory=utcnow)
    agent: str
    level: Literal["info", "warn", "error"] = "info"
    message: str
    duration_ms: float | None = None


# ─────────────────────────────────────────────────────────────────────────────
# LangGraph state
# ─────────────────────────────────────────────────────────────────────────────


class SenaniState(BaseModel):
    """The single state object threaded through the LangGraph StateGraph.

    Nodes return *partial* dicts; LangGraph merges them. ``logs`` and
    ``review_history`` use an ``operator.add`` reducer so every node appends
    rather than overwrites.
    """

    thread_id: str
    raw_input: Telemetry
    parsed_event: ParsedEvent | None = None
    geo_context: GeospatialContext | None = None
    risk: RiskAssessment | None = None
    sitrep: SITREP | None = None
    sitrep_text: str | None = None
    requires_human_approval: bool = True
    status: WorkflowStatus = WorkflowStatus.RUNNING
    error: str | None = None
    review_history: Annotated[list[ReviewRecord], operator.add] = Field(default_factory=list)
    logs: Annotated[list[AgentLog], operator.add] = Field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# API contracts
# ─────────────────────────────────────────────────────────────────────────────


class TriggerResponse(BaseModel):
    thread_id: str
    status: WorkflowStatus
    transport: Literal["kafka", "direct"]


class ReviewActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    thread_id: str
    decision: ReviewDecision
    operator_id: str = Field(min_length=1, max_length=64)
    notes: str | None = Field(default=None, max_length=2_000)


class ReviewActionResponse(BaseModel):
    thread_id: str
    decision: ReviewDecision
    status: WorkflowStatus


class MissionSummary(BaseModel):
    """Lightweight row for the dashboard's telemetry feed / map."""

    thread_id: str
    source_id: str
    source_type: SourceType
    status: WorkflowStatus
    created_at: datetime
    updated_at: datetime
    event_type: EventType | None = None
    summary: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    severity: SeverityLevel | None = None
    score: float | None = None
    current_node: str | None = None
    error: str | None = None


class PendingReview(BaseModel):
    thread_id: str
    telemetry: Telemetry
    parsed_event: ParsedEvent
    geo_context: GeospatialContext
    risk: RiskAssessment
    sitrep: SITREP
    sitrep_text: str
    review_history: list[ReviewRecord]


class StreamEvent(BaseModel):
    """Envelope pushed to WebSocket clients and the Kafka events topic."""

    type: Literal["log", "mission", "review_required", "finalized", "snapshot", "ping"]
    thread_id: str | None = None
    ts: datetime = Field(default_factory=utcnow)
    data: dict[str, Any] = Field(default_factory=dict)


# Types the checkpointer is allowed to revive from storage (strict msgpack
# allowlist — see graph.build_checkpoint_serde).
CHECKPOINT_TYPES: tuple[type, ...] = (
    SourceType, EventType, SeverityLevel, Precedence, WorkflowStatus, ReviewDecision,
    Telemetry, ParsedEvent, ProximityHit, GeospatialContext, RiskFactor, RiskAssessment,
    SITREP, ReviewRecord, AgentLog, SenaniState,
)
