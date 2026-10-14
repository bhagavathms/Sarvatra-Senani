/**
 * Sarvatra Senani — Command Center dashboard (single-file React + Tailwind).
 *
 * Layout (xl):  ┌──────────── header: link status · engines · Zulu clock ────────────┐
 *               │ Telemetry injector │ Tactical map (SVG)       │ HITL review queue │
 *               │ Mission feed       │ Agent pipeline tracker   │ SITREP console +  │
 *               │                    │ Live agent log stream    │ sign-off controls │
 *               └────────────────────┴──────────────────────────┴───────────────────┘
 *
 * Data flow:
 *   • WebSocket /ws/stream  → snapshot on connect, then log / mission / review_required /
 *                             finalized events (auto-reconnect with backoff).
 *   • REST /api/*           → trigger telemetry, fetch pending reviews, post review actions,
 *                             load full mission state and the map reference layer.
 *
 * The backend is the source of truth; this component only mirrors its events.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

// ─────────────────────────────────────────────────────────────────────────────
// Config & API client
// ─────────────────────────────────────────────────────────────────────────────

const API_BASE = import.meta.env.VITE_API_BASE ?? ""; // "" → same origin (Vite proxy)
const API_KEY = import.meta.env.VITE_API_KEY ?? "";

function wsUrl() {
  const base = API_BASE || window.location.origin;
  const url = new URL("/ws/stream", base);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  if (API_KEY) url.searchParams.set("api_key", API_KEY);
  return url.toString();
}

async function api(path, { method = "GET", body } = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: {
      ...(body ? { "Content-Type": "application/json" } : {}),
      ...(API_KEY ? { "X-API-Key": API_KEY } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const detail = Array.isArray(data.detail)
      ? data.detail.map((d) => `${(d.loc || []).slice(1).join(".")}: ${d.msg}`).join("; ")
      : data.detail;
    throw new Error(detail || `${res.status} ${res.statusText}`);
  }
  return data;
}

// ─────────────────────────────────────────────────────────────────────────────
// Visual vocabulary
// ─────────────────────────────────────────────────────────────────────────────

const SEVERITY = {
  LOW: { chip: "bg-emerald-500/15 text-emerald-300 ring-emerald-500/40", hex: "#34d399", bar: "bg-emerald-400" },
  MODERATE: { chip: "bg-amber-500/15 text-amber-300 ring-amber-500/40", hex: "#fbbf24", bar: "bg-amber-400" },
  HIGH: { chip: "bg-orange-500/15 text-orange-300 ring-orange-500/40", hex: "#fb923c", bar: "bg-orange-400" },
  CRITICAL: { chip: "bg-red-500/20 text-red-300 ring-red-500/50", hex: "#f87171", bar: "bg-red-500" },
};
const PENDING_HEX = "#94a3b8";

const STATUS = {
  running: { label: "RUNNING", cls: "text-sky-300 bg-sky-500/10 ring-sky-500/30" },
  awaiting_review: { label: "AWAITING REVIEW", cls: "text-amber-200 bg-amber-500/15 ring-amber-400/40 animate-pulse" },
  approved: { label: "APPROVED", cls: "text-emerald-300 bg-emerald-500/10 ring-emerald-500/30" },
  rejected: { label: "REJECTED", cls: "text-red-300 bg-red-500/10 ring-red-500/30" },
  auto_finalized: { label: "AUTO-RELEASED", cls: "text-slate-300 bg-slate-500/10 ring-slate-500/30" },
  failed: { label: "FAILED", cls: "text-red-200 bg-red-900/40 ring-red-500/40" },
};

const AGENT_COLOR = {
  ingestion: "text-sky-400",
  geospatial: "text-teal-400",
  risk: "text-orange-400",
  sitrep: "text-violet-400",
  hitl_checkpoint: "text-amber-300",
  finalize: "text-emerald-400",
  orchestrator: "text-red-400",
};

const PIPELINE = [
  { id: "ingestion", label: "Ingestion", sub: "parse · normalize" },
  { id: "geospatial", label: "Geospatial", sub: "proximity · zones" },
  { id: "risk", label: "Risk", sub: "score · logistics" },
  { id: "sitrep", label: "SITREP", sub: "compose report" },
  { id: "hitl_checkpoint", label: "HITL", sub: "operator sign-off" },
  { id: "finalize", label: "Finalize", sub: "release / close" },
];

const TERMINAL = new Set(["approved", "rejected", "auto_finalized", "failed"]);

const pretty = (s) => (s || "").replaceAll("_", " ");
const zulu = (iso) => (iso ? new Date(iso).toISOString().slice(11, 19) + "Z" : "—");

// ─────────────────────────────────────────────────────────────────────────────
// Sample telemetry presets (notional exercise data inside the AO)
// ─────────────────────────────────────────────────────────────────────────────

const PRESETS = [
  {
    name: "Armoured column",
    source_id: "RADAR-N1",
    source_type: "json",
    payload: {
      type: "armour", lat: 27.46, lon: 70.62, threat: "high", heading: 95, speed: 18, confidence: 0.85,
      equipment: ["tank", "apc"], size: "company (~12 vehicles)",
      description: "Armoured column advancing east from the boundary towards BRIDGE ALPHA",
    },
  },
  {
    name: "Radio: infiltration",
    source_id: "BP-SPARROW",
    source_type: "radio",
    payload:
      "Contact report from BP SPARROW: possible infiltration, section of ~8 personnel with rifles crossing the fence near 27.18N 70.69E, moving east, approx 4 kph.",
  },
  {
    name: "UAS over airstrip",
    source_id: "EO-SENSOR-12",
    source_type: "json",
    payload: {
      category: "drone", position: { lat: 26.72, lng: 71.02 }, threat: 3, confidence: "probable",
      heading: 45, speed: 60, remarks: "Quadcopter loitering over AIRSTRIP ECHO perimeter",
    },
  },
  {
    name: "Indirect fire",
    source_id: "CBR-RADAR-2",
    source_type: "json",
    payload: {
      event_type: "artillery", lat: 27.07, lon: 70.8, threat_level: 5, confidence: 0.95,
      description: "Mortar impacts 2 km west of FUEL DEPOT DELTA, 6 rounds observed",
    },
  },
  {
    name: "EW / jamming",
    source_id: "FOB-KESTREL",
    source_type: "text",
    payload: "GPS spoofing and comms jamming reported by FOB KESTREL, centre approx 27.52N 70.78E, moderate impact on VHF nets.",
  },
  {
    name: "OSINT: crowd",
    source_id: "OSINT-FEED",
    source_type: "osint",
    payload: "Social media posts report a crowd gathering near the highway junction at 27.31, 71.38; unconfirmed, low threat.",
  },
  {
    name: "Ungeolocatable",
    source_id: "RUMINT",
    source_type: "text",
    payload: "Something is happening somewhere to the west, details unclear.",
  },
];

// ─────────────────────────────────────────────────────────────────────────────
// Live stream hook
// ─────────────────────────────────────────────────────────────────────────────

const MAX_LOGS = 500;

function useSenaniStream() {
  const [link, setLink] = useState("connecting"); // connecting | live | down
  const [health, setHealth] = useState(null);
  const [missions, setMissions] = useState({});
  const [logs, setLogs] = useState([]);
  const [reviewTick, setReviewTick] = useState(0);
  const retry = useRef(0);

  useEffect(() => {
    let ws;
    let timer;
    let closed = false;

    const connect = () => {
      setLink("connecting");
      ws = new WebSocket(wsUrl());
      ws.onopen = () => {
        retry.current = 0;
        setLink("live");
      };
      ws.onmessage = (msg) => {
        const evt = JSON.parse(msg.data);
        switch (evt.type) {
          case "snapshot":
            setHealth(evt.data.health);
            setMissions(Object.fromEntries(evt.data.missions.map((m) => [m.thread_id, m])));
            setLogs(evt.data.logs.slice(-MAX_LOGS));
            setReviewTick((t) => t + 1);
            break;
          case "ping":
            setHealth(evt.data.health);
            break;
          case "log":
            setLogs((prev) => [...prev.slice(-(MAX_LOGS - 1)), evt]);
            break;
          case "mission":
            setMissions((prev) => ({ ...prev, [evt.thread_id]: evt.data }));
            break;
          case "review_required":
          case "finalized":
            setMissions((prev) => ({ ...prev, [evt.thread_id]: evt.data }));
            setReviewTick((t) => t + 1);
            break;
          default:
        }
      };
      ws.onclose = () => {
        if (closed) return;
        setLink("down");
        const delay = Math.min(10000, 500 * 2 ** retry.current++);
        timer = setTimeout(connect, delay);
      };
      ws.onerror = () => ws.close();
    };

    connect();
    return () => {
      closed = true;
      clearTimeout(timer);
      ws?.close();
    };
  }, []);

  return { link, health, missions, logs, reviewTick, bumpReviews: () => setReviewTick((t) => t + 1) };
}

// ─────────────────────────────────────────────────────────────────────────────
// Small primitives
// ─────────────────────────────────────────────────────────────────────────────

function Panel({ title, right, className = "", bodyClass = "", children }) {
  return (
    <section className={`flex min-h-0 flex-col rounded-lg border border-slate-800 bg-slate-900/60 shadow-lg shadow-black/30 ${className}`}>
      <header className="flex items-center justify-between gap-2 border-b border-slate-800 px-3 py-2">
        <h2 className="font-mono text-[11px] font-bold uppercase tracking-[0.18em] text-slate-400">{title}</h2>
        {right}
      </header>
      <div className={`min-h-0 flex-1 ${bodyClass}`}>{children}</div>
    </section>
  );
}

function SeverityChip({ severity, score }) {
  if (!severity) return <span className="rounded px-1.5 py-0.5 font-mono text-[10px] text-slate-500 ring-1 ring-slate-700">PENDING</span>;
  return (
    <span className={`rounded px-1.5 py-0.5 font-mono text-[10px] font-bold ring-1 ${SEVERITY[severity].chip}`}>
      {severity}
      {score != null && <span className="ml-1 opacity-70">{Math.round(score)}</span>}
    </span>
  );
}

function StatusBadge({ status }) {
  const s = STATUS[status] ?? STATUS.running;
  return <span className={`rounded px-1.5 py-0.5 font-mono text-[10px] font-semibold ring-1 ${s.cls}`}>{s.label}</span>;
}

function Pill({ label, value, tone = "slate" }) {
  const tones = {
    green: "text-emerald-300 ring-emerald-500/30",
    amber: "text-amber-300 ring-amber-500/30",
    red: "text-red-300 ring-red-500/40",
    slate: "text-slate-300 ring-slate-700",
  };
  return (
    <span className={`inline-flex items-center gap-1.5 rounded px-2 py-1 font-mono text-[10px] ring-1 ${tones[tone]}`}>
      <span className="text-slate-500">{label}</span>
      <span className="font-semibold uppercase">{value}</span>
    </span>
  );
}

function useZuluClock() {
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(id);
  }, []);
  return now.toISOString().slice(11, 19) + "Z";
}

// ─────────────────────────────────────────────────────────────────────────────
// Header + stat tiles
// ─────────────────────────────────────────────────────────────────────────────

function Header({ link, health }) {
  const clock = useZuluClock();
  const linkTone = link === "live" ? "green" : link === "connecting" ? "amber" : "red";
  return (
    <header className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 bg-slate-950/80 px-4 py-3 backdrop-blur">
      <div className="flex items-center gap-3">
        <svg viewBox="0 0 32 32" className="h-8 w-8 text-emerald-400" aria-hidden>
          <path d="M16 2 29 9v8c0 7-5.5 11.5-13 13C8.5 28.5 3 24 3 17V9z" fill="none" stroke="currentColor" strokeWidth="2" />
          <path d="M9 16h14M16 9v14" stroke="currentColor" strokeWidth="1.5" opacity=".6" />
          <circle cx="16" cy="16" r="3.5" fill="currentColor" />
        </svg>
        <div>
          <h1 className="font-mono text-lg font-bold tracking-[0.25em] text-slate-100">SARVATRA SENANI</h1>
          <p className="font-mono text-[10px] uppercase tracking-[0.2em] text-slate-500">
            Multi-agent battlefield decision support · Command center
          </p>
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <Pill label="LINK" value={link} tone={linkTone} />
        <Pill label="LLM" value={health?.llm ?? "—"} />
        <Pill label="GEO" value={health?.geo_engine ?? "—"} />
        <Pill label="STATE" value={health?.checkpointer ?? "—"} />
        <Pill label="BUS" value={health?.transport ?? "—"} />
        <span className="ml-1 rounded bg-slate-900 px-2.5 py-1 font-mono text-sm font-bold tabular-nums text-emerald-300 ring-1 ring-slate-800">
          {clock}
        </span>
      </div>
    </header>
  );
}

function StatTiles({ missions }) {
  const counts = useMemo(() => {
    const c = { running: 0, awaiting_review: 0, approved: 0, rejected: 0, failed: 0, critical: 0 };
    for (const m of Object.values(missions)) {
      if (m.status in c) c[m.status] += 1;
      if (m.status === "auto_finalized") c.approved += 1;
      if (m.severity === "CRITICAL" && !TERMINAL.has(m.status)) c.critical += 1;
    }
    return c;
  }, [missions]);
  const tiles = [
    ["In pipeline", counts.running, "text-sky-300"],
    ["Awaiting review", counts.awaiting_review, "text-amber-300"],
    ["Critical open", counts.critical, "text-red-400"],
    ["Released", counts.approved, "text-emerald-300"],
    ["Rejected", counts.rejected, "text-slate-300"],
    ["Failed", counts.failed, "text-red-300"],
  ];
  return (
    <div className="grid grid-cols-3 gap-2 sm:grid-cols-6">
      {tiles.map(([label, n, cls]) => (
        <div key={label} className="rounded-lg border border-slate-800 bg-slate-900/60 px-3 py-2">
          <div className={`font-mono text-2xl font-bold tabular-nums ${cls}`}>{n}</div>
          <div className="font-mono text-[10px] uppercase tracking-wider text-slate-500">{label}</div>
        </div>
      ))}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Telemetry injector + mission feed
// ─────────────────────────────────────────────────────────────────────────────

function TelemetryInjector({ onSubmitted }) {
  const [sourceId, setSourceId] = useState(PRESETS[0].source_id);
  const [sourceType, setSourceType] = useState(PRESETS[0].source_type);
  const [payload, setPayload] = useState(JSON.stringify(PRESETS[0].payload, null, 2));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);

  const loadPreset = (p) => {
    setSourceId(p.source_id);
    setSourceType(p.source_type);
    setPayload(typeof p.payload === "string" ? p.payload : JSON.stringify(p.payload, null, 2));
    setError(null);
  };

  const submit = async (e) => {
    e.preventDefault();
    setError(null);
    let body = payload.trim();
    if (sourceType === "json") {
      try {
        body = JSON.parse(body);
      } catch {
        setError("Payload is not valid JSON (switch type to TEXT for free-form reports).");
        return;
      }
    }
    setBusy(true);
    try {
      const res = await api("/api/trigger-telemetry", {
        method: "POST",
        body: { source_id: sourceId.trim(), source_type: sourceType, payload: body },
      });
      onSubmitted(res.thread_id);
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Panel title="Inject telemetry" bodyClass="p-3">
      <div className="mb-2 flex flex-wrap gap-1">
        {PRESETS.map((p) => (
          <button
            key={p.name}
            type="button"
            onClick={() => loadPreset(p)}
            className="rounded border border-slate-700 bg-slate-800/60 px-2 py-0.5 font-mono text-[10px] text-slate-300 transition hover:border-emerald-500/60 hover:text-emerald-300"
          >
            {p.name}
          </button>
        ))}
      </div>
      <form onSubmit={submit} className="space-y-2">
        <div className="grid grid-cols-2 gap-2">
          <label className="block">
            <span className="font-mono text-[10px] uppercase text-slate-500">Source ID</span>
            <input
              value={sourceId}
              onChange={(e) => setSourceId(e.target.value)}
              required
              maxLength={64}
              className="mt-0.5 w-full rounded border border-slate-700 bg-slate-950 px-2 py-1 font-mono text-xs text-slate-200 outline-none focus:border-emerald-500"
            />
          </label>
          <label className="block">
            <span className="font-mono text-[10px] uppercase text-slate-500">Feed type</span>
            <select
              value={sourceType}
              onChange={(e) => setSourceType(e.target.value)}
              className="mt-0.5 w-full rounded border border-slate-700 bg-slate-950 px-2 py-1 font-mono text-xs text-slate-200 outline-none focus:border-emerald-500"
            >
              {["json", "text", "radio", "osint"].map((t) => (
                <option key={t} value={t}>{t.toUpperCase()}</option>
              ))}
            </select>
          </label>
        </div>
        <textarea
          value={payload}
          onChange={(e) => setPayload(e.target.value)}
          rows={7}
          spellCheck={false}
          className="scroll-thin w-full resize-y rounded border border-slate-700 bg-slate-950 p-2 font-mono text-[11px] leading-relaxed text-emerald-200/90 outline-none focus:border-emerald-500"
        />
        {error && <p className="rounded border border-red-500/40 bg-red-500/10 px-2 py-1 font-mono text-[11px] text-red-300">{error}</p>}
        <button
          type="submit"
          disabled={busy}
          className="w-full rounded bg-emerald-600 py-1.5 font-mono text-xs font-bold uppercase tracking-widest text-emerald-50 transition hover:bg-emerald-500 disabled:opacity-50"
        >
          {busy ? "Transmitting…" : "Transmit to pipeline"}
        </button>
      </form>
    </Panel>
  );
}

function MissionFeed({ missions, selectedId, onSelect }) {
  const rows = useMemo(
    () => Object.values(missions).sort((a, b) => new Date(b.created_at) - new Date(a.created_at)),
    [missions],
  );
  return (
    <Panel title="Telemetry feed" right={<span className="font-mono text-[10px] text-slate-500">{rows.length} tracks</span>} className="flex-1" bodyClass="scroll-thin overflow-y-auto">
      {rows.length === 0 && <p className="p-4 text-center font-mono text-xs text-slate-600">No telemetry received yet.</p>}
      <ul className="divide-y divide-slate-800/70">
        {rows.map((m) => (
          <li key={m.thread_id}>
            <button
              onClick={() => onSelect(m.thread_id)}
              className={`w-full px-3 py-2 text-left transition hover:bg-slate-800/50 ${selectedId === m.thread_id ? "bg-slate-800/80 shadow-[inset_3px_0_0_0_#34d399]" : ""}`}
            >
              <div className="flex items-center justify-between gap-2">
                <span className="truncate font-mono text-xs font-semibold text-slate-200">
                  {m.event_type ? pretty(m.event_type).toUpperCase() : "PARSING…"}
                </span>
                <SeverityChip severity={m.severity} score={m.score} />
              </div>
              <div className="mt-1 flex items-center justify-between gap-2">
                <span className="truncate font-mono text-[10px] text-slate-500">
                  {zulu(m.created_at)} · {m.source_id} · {m.source_type.toUpperCase()}
                </span>
                <StatusBadge status={m.status} />
              </div>
              {m.error && <p className="mt-1 truncate font-mono text-[10px] text-red-400">{m.error}</p>}
            </button>
          </li>
        ))}
      </ul>
    </Panel>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Tactical map (pure SVG — no tile server needed for an air-gapped ops room)
// ─────────────────────────────────────────────────────────────────────────────

function TacticalMap({ reference, missions, selectedId, onSelect }) {
  const W = 640;
  const geo = useMemo(() => {
    if (!reference) return null;
    const { min_lat, max_lat, min_lon, max_lon } = reference.bbox;
    const midLat = ((min_lat + max_lat) / 2) * (Math.PI / 180);
    const H = Math.round((W * (max_lat - min_lat)) / ((max_lon - min_lon) * Math.cos(midLat)));
    const x = (lon) => ((lon - min_lon) / (max_lon - min_lon)) * W;
    const y = (lat) => H - ((lat - min_lat) / (max_lat - min_lat)) * H;
    const grid = [];
    for (let v = Math.ceil(min_lon * 5) / 5; v <= max_lon; v += 0.2) grid.push({ kind: "lon", v });
    for (let v = Math.ceil(min_lat * 5) / 5; v <= max_lat; v += 0.2) grid.push({ kind: "lat", v });
    return { H, x, y, grid };
  }, [reference]);

  if (!reference || !geo) {
    return <div className="flex h-full items-center justify-center font-mono text-xs text-slate-600">Loading reference layer…</div>;
  }
  const { H, x, y, grid } = geo;
  const pts = (coords) => coords.map(([la, lo]) => `${x(lo)},${y(la)}`).join(" ");
  const tracks = Object.values(missions).filter((m) => m.latitude != null);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="h-full w-full" role="img" aria-label="Tactical situation map">
      <defs>
        <pattern id="hatch" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
          <line x1="0" y1="0" x2="0" y2="6" stroke="#f59e0b" strokeWidth="1" opacity=".35" />
        </pattern>
        <radialGradient id="vignette" cx="50%" cy="50%" r="70%">
          <stop offset="60%" stopColor="#020617" stopOpacity="0" />
          <stop offset="100%" stopColor="#020617" stopOpacity=".7" />
        </radialGradient>
      </defs>
      <rect width={W} height={H} fill="#0b1220" />
      {grid.map((g) =>
        g.kind === "lon" ? (
          <g key={`lon${g.v}`}>
            <line x1={x(g.v)} x2={x(g.v)} y1={0} y2={H} stroke="#1e293b" strokeWidth=".7" />
            <text x={x(g.v) + 2} y={H - 4} fontSize="8" fill="#475569" fontFamily="monospace">{g.v.toFixed(1)}E</text>
          </g>
        ) : (
          <g key={`lat${g.v}`}>
            <line x1={0} x2={W} y1={y(g.v)} y2={y(g.v)} stroke="#1e293b" strokeWidth=".7" />
            <text x={3} y={y(g.v) - 2} fontSize="8" fill="#475569" fontFamily="monospace">{g.v.toFixed(1)}N</text>
          </g>
        ),
      )}

      {reference.restricted_zones.map((z) => (
        <g key={z.name}>
          <polygon points={pts(z.coords)} fill="url(#hatch)" stroke="#f59e0b" strokeDasharray="4 3" strokeWidth="1" opacity=".9" />
          <text x={x(z.coords[0][1]) + 3} y={y(z.coords[0][0]) + 10} fontSize="7.5" fill="#fbbf24" fontFamily="monospace" opacity=".8">{z.name}</text>
        </g>
      ))}

      {reference.borders.map((b) => (
        <g key={b.name}>
          <polyline points={pts(b.coords)} fill="none" stroke="#ef4444" strokeWidth="2.2" strokeDasharray="10 4 2 4" />
          <text x={x(b.coords[0][1]) + 6} y={y(b.coords[0][0]) + 12} fontSize="8" fill="#f87171" fontFamily="monospace">{b.name}</text>
        </g>
      ))}

      {reference.infrastructure.map((i) => (
        <g key={i.name}>
          <title>{`${i.name} — ${i.subtype} (criticality ${i.criticality})`}</title>
          <rect x={x(i.lon) - 4} y={y(i.lat) - 4} width="8" height="8" fill="#0e7490" stroke="#22d3ee" strokeWidth="1" transform={`rotate(45 ${x(i.lon)} ${y(i.lat)})`} />
          <text x={x(i.lon) + 8} y={y(i.lat) + 3} fontSize="7.5" fill="#67e8f9" fontFamily="monospace" opacity=".85">{i.name}</text>
        </g>
      ))}

      {reference.friendly_units.map((f) => (
        <g key={f.name}>
          <title>{`${f.name} — ${pretty(f.subtype)}`}</title>
          <rect x={x(f.lon) - 8} y={y(f.lat) - 5} width="16" height="10" fill="#1e3a8a" stroke="#60a5fa" strokeWidth="1.2" />
          <line x1={x(f.lon) - 8} y1={y(f.lat) - 5} x2={x(f.lon) + 8} y2={y(f.lat) + 5} stroke="#60a5fa" strokeWidth=".8" />
          <line x1={x(f.lon) - 8} y1={y(f.lat) + 5} x2={x(f.lon) + 8} y2={y(f.lat) - 5} stroke="#60a5fa" strokeWidth=".8" />
          <text x={x(f.lon) + 11} y={y(f.lat) + 3} fontSize="7.5" fill="#93c5fd" fontFamily="monospace">{f.name}</text>
        </g>
      ))}

      {tracks.map((m) => {
        const cx = x(m.longitude);
        const cy = y(m.latitude);
        const color = m.severity ? SEVERITY[m.severity].hex : PENDING_HEX;
        const selected = m.thread_id === selectedId;
        const open = !TERMINAL.has(m.status);
        return (
          <g key={m.thread_id} onClick={() => onSelect(m.thread_id)} className="cursor-pointer" opacity={open || selected ? 1 : 0.45}>
            <title>{`${pretty(m.event_type)} · ${m.severity ?? "pending"} · ${m.source_id}\n${m.summary ?? ""}`}</title>
            {selected && (
              <circle cx={cx} cy={cy} r="10" fill="none" stroke={color} strokeWidth="1.5">
                <animate attributeName="r" from="8" to="26" dur="1.6s" repeatCount="indefinite" />
                <animate attributeName="opacity" from=".9" to="0" dur="1.6s" repeatCount="indefinite" />
              </circle>
            )}
            {/* APP-6 style hostile symbol: diamond */}
            <rect x={cx - 6} y={cy - 6} width="12" height="12" fill={color} fillOpacity=".25" stroke={color} strokeWidth="1.8" transform={`rotate(45 ${cx} ${cy})`} />
            <circle cx={cx} cy={cy} r="1.8" fill={color} />
          </g>
        );
      })}

      <rect width={W} height={H} fill="url(#vignette)" pointerEvents="none" />
      <text x={W - 6} y={12} textAnchor="end" fontSize="8.5" fill="#64748b" fontFamily="monospace">{reference.ao_name}</text>
    </svg>
  );
}

function MapLegend() {
  const item = (swatch, label) => (
    <span key={label} className="inline-flex items-center gap-1">
      {swatch}
      <span>{label}</span>
    </span>
  );
  return (
    <div className="flex flex-wrap gap-x-3 gap-y-1 px-3 py-1.5 font-mono text-[9px] uppercase text-slate-500">
      {item(<span className="inline-block h-0.5 w-4 bg-red-500" />, "Boundary")}
      {item(<span className="inline-block h-2.5 w-2.5 rotate-45 border border-cyan-400 bg-cyan-800" />, "Infrastructure")}
      {item(<span className="inline-block h-2 w-3 border border-blue-400 bg-blue-900" />, "Friendly")}
      {item(<span className="inline-block h-2.5 w-2.5 border border-dashed border-amber-400" />, "Restricted")}
      {Object.entries(SEVERITY).map(([k, v]) =>
        item(<span className="inline-block h-2 w-2 rotate-45" style={{ background: v.hex }} />, k),
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline tracker + log stream
// ─────────────────────────────────────────────────────────────────────────────

function PipelineTracker({ mission }) {
  const idx = (node) => {
    if (node === "queue_review") return 4;
    const i = PIPELINE.findIndex((p) => p.id === node);
    return i;
  };
  let current = -1;
  if (mission) {
    if (TERMINAL.has(mission.status) && mission.status !== "failed") current = PIPELINE.length;
    else if (mission.status === "awaiting_review") current = 4;
    else current = Math.max(0, idx(mission.current_node));
  }
  const failed = mission?.status === "failed";

  return (
    <div className="grid grid-cols-6 gap-1.5 p-3">
      {PIPELINE.map((p, i) => {
        let state = "idle";
        if (mission) {
          if (failed && i === Math.max(0, idx(mission.current_node))) state = "failed";
          else if (i < current) state = "done";
          else if (i === current) state = mission.status === "awaiting_review" ? "waiting" : "active";
        }
        const cls = {
          idle: "border-slate-800 text-slate-600",
          done: "border-emerald-600/50 bg-emerald-500/10 text-emerald-300",
          active: "border-sky-500/70 bg-sky-500/15 text-sky-200 animate-pulse",
          waiting: "border-amber-400/80 bg-amber-500/15 text-amber-200 animate-pulse",
          failed: "border-red-500/80 bg-red-500/15 text-red-300",
        }[state];
        return (
          <div key={p.id} className={`rounded border px-1.5 py-1.5 text-center transition ${cls}`}>
            <div className="font-mono text-[10px] font-bold uppercase tracking-wider">{p.label}</div>
            <div className="hidden font-mono text-[8.5px] opacity-70 md:block">{p.sub}</div>
          </div>
        );
      })}
    </div>
  );
}

function LogStream({ logs, selectedId }) {
  const [onlySelected, setOnlySelected] = useState(false);
  const [follow, setFollow] = useState(true);
  const box = useRef(null);
  const shown = onlySelected && selectedId ? logs.filter((l) => l.thread_id === selectedId) : logs;

  useEffect(() => {
    if (follow && box.current) box.current.scrollTop = box.current.scrollHeight;
  }, [shown.length, follow]);

  const levelCls = { info: "text-slate-300", warn: "text-amber-300", error: "text-red-400" };
  return (
    <Panel
      title="Agent execution log"
      className="min-h-[220px] flex-1"
      right={
        <div className="flex items-center gap-3 font-mono text-[10px] text-slate-500">
          <label className="flex cursor-pointer items-center gap-1">
            <input type="checkbox" checked={onlySelected} onChange={(e) => setOnlySelected(e.target.checked)} className="accent-emerald-500" />
            selected only
          </label>
          <label className="flex cursor-pointer items-center gap-1">
            <input type="checkbox" checked={follow} onChange={(e) => setFollow(e.target.checked)} className="accent-emerald-500" />
            follow
          </label>
        </div>
      }
    >
      <div ref={box} className="scroll-thin h-full overflow-y-auto bg-black/40 p-2 font-mono text-[11px] leading-relaxed">
        {shown.length === 0 && <p className="text-slate-600">&gt; awaiting agent activity_</p>}
        {shown.map((l, i) => (
          <div key={`${l.ts}-${i}`} className={`flex gap-2 ${l.thread_id === selectedId ? "bg-emerald-500/5" : ""}`}>
            <span className="shrink-0 text-slate-600">{zulu(l.data.ts ?? l.ts)}</span>
            <span className="shrink-0 text-slate-500">{l.thread_id?.slice(-6)}</span>
            <span className={`w-20 shrink-0 font-bold uppercase ${AGENT_COLOR[l.data.agent] ?? "text-slate-400"}`}>
              {l.data.agent === "hitl_checkpoint" ? "hitl" : l.data.agent}
            </span>
            <span className={`min-w-0 break-words ${levelCls[l.data.level] ?? "text-slate-300"}`}>
              {l.data.message}
              {l.data.duration_ms != null && <span className="ml-1 text-slate-600">[{l.data.duration_ms}ms]</span>}
            </span>
          </div>
        ))}
      </div>
    </Panel>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// HITL review queue + SITREP console
// ─────────────────────────────────────────────────────────────────────────────

function ReviewQueue({ pending, selectedId, onSelect }) {
  return (
    <Panel
      title="HITL review queue"
      right={
        <span className={`rounded px-1.5 font-mono text-[10px] font-bold ${pending.length ? "bg-amber-500/20 text-amber-300" : "text-slate-600"}`}>
          {pending.length} pending
        </span>
      }
      bodyClass="scroll-thin max-h-56 overflow-y-auto"
    >
      {pending.length === 0 && <p className="p-4 text-center font-mono text-xs text-slate-600">Queue clear. No SITREPs awaiting sign-off.</p>}
      <ul className="divide-y divide-slate-800/70">
        {pending.map((p) => (
          <li key={p.thread_id}>
            <button
              onClick={() => onSelect(p.thread_id)}
              className={`w-full px-3 py-2 text-left transition hover:bg-slate-800/50 ${selectedId === p.thread_id ? "bg-amber-500/10 shadow-[inset_3px_0_0_0_#fbbf24]" : ""}`}
            >
              <div className="flex items-center justify-between gap-2">
                <span className="truncate font-mono text-[11px] font-semibold text-slate-200">{p.sitrep.subject}</span>
                <span className="shrink-0 rounded bg-slate-800 px-1 font-mono text-[9px] text-slate-300">{p.sitrep.precedence}</span>
              </div>
              <div className="mt-0.5 font-mono text-[10px] text-slate-500">
                {p.sitrep.sitrep_id}
                {p.sitrep.revision > 0 && <span className="text-violet-300"> · REV {p.sitrep.revision}</span>} · {p.telemetry.source_id}
              </div>
            </button>
          </li>
        ))}
      </ul>
    </Panel>
  );
}

function RiskBreakdown({ risk }) {
  const factors = risk.factors.filter((f) => f.weight > 0);
  const multiplier = risk.factors.find((f) => f.weight === 0);
  return (
    <div className="space-y-1.5">
      <div className="flex items-end justify-between">
        <span className="font-mono text-[10px] uppercase tracking-wider text-slate-500">Risk score</span>
        <span className="font-mono text-xl font-bold tabular-nums" style={{ color: SEVERITY[risk.severity].hex }}>
          {risk.score.toFixed(1)}
          <span className="text-xs text-slate-500">/100</span>
        </span>
      </div>
      <div className="h-1.5 overflow-hidden rounded bg-slate-800">
        <div className={`h-full ${SEVERITY[risk.severity].bar}`} style={{ width: `${risk.score}%` }} />
      </div>
      {factors.map((f) => (
        <div key={f.name} title={f.detail}>
          <div className="flex justify-between font-mono text-[10px]">
            <span className="text-slate-400">{f.name}</span>
            <span className="tabular-nums text-slate-300">+{f.contribution.toFixed(1)} / {f.weight}</span>
          </div>
          <div className="h-1 overflow-hidden rounded bg-slate-800">
            <div className="h-full bg-slate-400" style={{ width: `${(f.contribution / f.weight) * 100}%` }} />
          </div>
          <div className="truncate font-mono text-[9px] text-slate-600">{f.detail}</div>
        </div>
      ))}
      {multiplier && <div className="font-mono text-[10px] text-slate-500">Confidence {multiplier.detail}</div>}
      <div className="grid grid-cols-2 gap-2 pt-1 font-mono text-[10px]">
        <div className="rounded bg-slate-950 px-2 py-1">
          <div className="text-slate-500">QRF ETA</div>
          <div className="text-slate-200">{risk.friendly_response_eta_min != null ? `${Math.round(risk.friendly_response_eta_min)} min` : "—"}</div>
        </div>
        <div className="rounded bg-slate-950 px-2 py-1">
          <div className="text-slate-500">Time to boundary</div>
          <div className={risk.time_to_border_min != null ? "text-red-300" : "text-slate-200"}>
            {risk.time_to_border_min != null ? `${Math.round(risk.time_to_border_min)} min` : "not closing"}
          </div>
        </div>
      </div>
    </div>
  );
}

function SignOff({ review, onDone }) {
  const [operatorId, setOperatorId] = useState(() => {
    try {
      return localStorage.getItem("senani.operator") ?? "";
    } catch {
      return "";
    }
  });
  const [notes, setNotes] = useState("");
  const [busy, setBusy] = useState(null);
  const [error, setError] = useState(null);
  const revisions = review.review_history.filter((r) => r.decision === "revise").length;

  useEffect(() => {
    setNotes("");
    setError(null);
  }, [review.thread_id, review.sitrep.revision]);

  const act = async (decision) => {
    setError(null);
    if (!operatorId.trim()) return setError("Operator ID is required for sign-off.");
    if (decision !== "approve" && !notes.trim()) return setError(`Notes are required to ${decision}.`);
    if (decision === "reject" && !window.confirm("Reject this SITREP? It will be closed and not disseminated.")) return;
    setBusy(decision);
    try {
      try {
        localStorage.setItem("senani.operator", operatorId.trim());
      } catch {
        /* storage unavailable — non-fatal */
      }
      await api("/api/review-action", {
        method: "POST",
        body: { thread_id: review.thread_id, decision, operator_id: operatorId.trim(), notes: notes.trim() || null },
      });
      onDone();
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="space-y-2 border-t border-slate-800 p-3">
      <div className="grid grid-cols-3 gap-2">
        <label className="col-span-1 block">
          <span className="font-mono text-[10px] uppercase text-slate-500">Operator ID</span>
          <input
            value={operatorId}
            onChange={(e) => setOperatorId(e.target.value)}
            placeholder="e.g. MAJ R. SINGH"
            maxLength={64}
            className="mt-0.5 w-full rounded border border-slate-700 bg-slate-950 px-2 py-1 font-mono text-xs text-slate-200 outline-none focus:border-amber-400"
          />
        </label>
        <label className="col-span-2 block">
          <span className="font-mono text-[10px] uppercase text-slate-500">Notes (required to reject / revise)</span>
          <input
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            maxLength={2000}
            placeholder="Reason, corrections, or additional context…"
            className="mt-0.5 w-full rounded border border-slate-700 bg-slate-950 px-2 py-1 font-mono text-xs text-slate-200 outline-none focus:border-amber-400"
          />
        </label>
      </div>
      {error && <p className="rounded border border-red-500/40 bg-red-500/10 px-2 py-1 font-mono text-[11px] text-red-300">{error}</p>}
      <div className="grid grid-cols-3 gap-2">
        <button
          onClick={() => act("approve")}
          disabled={!!busy}
          className="rounded bg-emerald-600 py-2 font-mono text-xs font-bold uppercase tracking-widest text-white transition hover:bg-emerald-500 disabled:opacity-50"
        >
          {busy === "approve" ? "…" : "Approve"}
        </button>
        <button
          onClick={() => act("revise")}
          disabled={!!busy || revisions >= 3}
          title={revisions >= 3 ? "Revision limit reached" : "Send back to SITREP agent with your notes"}
          className="rounded border border-violet-500/60 bg-violet-500/10 py-2 font-mono text-xs font-bold uppercase tracking-widest text-violet-200 transition hover:bg-violet-500/20 disabled:opacity-40"
        >
          {busy === "revise" ? "…" : `Revise (${3 - revisions})`}
        </button>
        <button
          onClick={() => act("reject")}
          disabled={!!busy}
          className="rounded border border-red-500/60 bg-red-500/10 py-2 font-mono text-xs font-bold uppercase tracking-widest text-red-300 transition hover:bg-red-500/20 disabled:opacity-50"
        >
          {busy === "reject" ? "…" : "Reject"}
        </button>
      </div>
    </div>
  );
}

function SitrepConsole({ selectedId, mission, review, onReviewed }) {
  const [detail, setDetail] = useState(null);
  const [tab, setTab] = useState("sitrep");

  // Non-pending missions: load full state from the checkpointer.
  useEffect(() => {
    if (!selectedId || review) {
      setDetail(null);
      return;
    }
    let cancelled = false;
    api(`/api/missions/${selectedId}`)
      .then((d) => !cancelled && setDetail(d.state))
      .catch(() => !cancelled && setDetail(null));
    return () => {
      cancelled = true;
    };
  }, [selectedId, review, mission?.status, mission?.current_node]);

  const state = review
    ? { sitrep_text: review.sitrep_text, risk: review.risk, geo_context: review.geo_context, parsed_event: review.parsed_event, review_history: review.review_history }
    : detail;

  if (!selectedId) {
    return (
      <Panel title="SITREP console" className="flex-1">
        <div className="flex h-full min-h-[200px] items-center justify-center p-6 text-center font-mono text-xs text-slate-600">
          Select a track from the feed, map, or review queue.
        </div>
      </Panel>
    );
  }

  const stamp = mission && TERMINAL.has(mission.status) ? STATUS[mission.status] : null;
  return (
    <Panel
      title="SITREP console"
      className="flex-1"
      bodyClass="flex flex-col"
      right={
        <div className="flex gap-1 font-mono text-[10px]">
          {["sitrep", "risk", "context"].map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`rounded px-2 py-0.5 uppercase ${tab === t ? "bg-slate-700 text-slate-100" : "text-slate-500 hover:text-slate-300"}`}
            >
              {t}
            </button>
          ))}
        </div>
      }
    >
      <div className="relative min-h-0 flex-1">
        {stamp && (
          <div className={`pointer-events-none absolute right-4 top-4 z-10 -rotate-12 rounded border-2 px-3 py-1 font-mono text-sm font-black tracking-widest ${stamp.cls}`}>
            {stamp.label}
          </div>
        )}
        <div className="scroll-thin absolute inset-0 overflow-y-auto p-3">
          {!state && mission?.status === "failed" && (
            <p className="font-mono text-xs text-red-300">Run failed: {mission.error}</p>
          )}
          {!state && mission?.status !== "failed" && <p className="font-mono text-xs text-slate-500">Loading…</p>}
          {state && tab === "sitrep" &&
            (state.sitrep_text ? (
              <pre className="whitespace-pre-wrap font-mono text-[10.5px] leading-relaxed text-slate-200">{state.sitrep_text}</pre>
            ) : (
              <p className="font-mono text-xs text-slate-500">{state.error ? `Run failed: ${state.error}` : "SITREP not generated yet."}</p>
            ))}
          {state && tab === "risk" && (state.risk ? <RiskBreakdown risk={state.risk} /> : <p className="font-mono text-xs text-slate-500">No risk assessment.</p>)}
          {state && tab === "context" && <ContextView state={state} />}
        </div>
      </div>
      {review && <SignOff review={review} onDone={onReviewed} />}
    </Panel>
  );
}

function ContextView({ state }) {
  const ev = state.parsed_event;
  const geo = state.geo_context;
  const row = (k, v) => (
    <div className="flex justify-between gap-3 border-b border-slate-800/60 py-0.5">
      <span className="text-slate-500">{k}</span>
      <span className="text-right text-slate-200">{v ?? "—"}</span>
    </div>
  );
  return (
    <div className="space-y-3 font-mono text-[10.5px]">
      {ev && (
        <div>
          <h3 className="mb-1 text-[10px] font-bold uppercase tracking-wider text-sky-400">Parsed event</h3>
          {row("Type", pretty(ev.event_type))}
          {row("Threat / confidence", `${ev.threat_level}/5 · ${Math.round(ev.confidence * 100)}%`)}
          {row("Size", ev.unit_size)}
          {row("Equipment", ev.equipment?.join(", ") || null)}
          {row("Heading / speed", ev.heading_deg != null ? `${Math.round(ev.heading_deg)}° · ${ev.speed_kph ?? "?"} km/h` : null)}
          {row("Parse method", ev.parse_method)}
          {ev.warnings?.map((w) => (
            <div key={w} className="text-amber-300">⚠ {w}</div>
          ))}
        </div>
      )}
      {geo && (
        <div>
          <h3 className="mb-1 text-[10px] font-bold uppercase tracking-wider text-teal-400">Geospatial context · {geo.engine}</h3>
          {row("Grid", geo.grid_ref)}
          {row("Sector", geo.sector)}
          {row("H3 cell", geo.h3_cell)}
          {row("Boundary", geo.nearest_border && `${geo.nearest_border.distance_km} km`)}
          {row("Nearest friendly", geo.nearest_friendly && `${geo.nearest_friendly.name} · ${geo.nearest_friendly.distance_km} km`)}
          {row("Restricted zone", geo.restricted_zone)}
          {geo.nearby_infrastructure.map((i) => row(i.name, `${i.distance_km} km · crit ${i.criticality}`))}
        </div>
      )}
      {state.review_history?.length > 0 && (
        <div>
          <h3 className="mb-1 text-[10px] font-bold uppercase tracking-wider text-amber-300">Review history</h3>
          {state.review_history.map((r, i) => (
            <div key={i} className="border-b border-slate-800/60 py-0.5 text-slate-300">
              {zulu(r.decided_at)} · <b>{r.decision.toUpperCase()}</b> · {r.operator_id}
              {r.notes && <span className="text-slate-500"> — {r.notes}</span>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Root
// ─────────────────────────────────────────────────────────────────────────────

export default function App() {
  const { link, health, missions, logs, reviewTick, bumpReviews } = useSenaniStream();
  const [reference, setReference] = useState(null);
  const [pending, setPending] = useState([]);
  const [selectedId, setSelectedId] = useState(null);

  useEffect(() => {
    api("/api/reference-data").then(setReference).catch(() => setReference(null));
  }, [link === "live"]); // retry once the link comes up

  const refreshPending = useCallback(() => {
    api("/api/pending-reviews").then(setPending).catch(() => {});
  }, []);
  useEffect(refreshPending, [reviewTick, refreshPending]);

  const selectedMission = selectedId ? missions[selectedId] : null;
  const selectedReview = pending.find((p) => p.thread_id === selectedId) ?? null;

  return (
    <div className="flex min-h-full flex-col bg-slate-950 bg-[radial-gradient(ellipse_at_top,rgba(16,185,129,0.06),transparent_60%)] font-sans text-slate-200">
      <div className="bg-amber-500/90 py-0.5 text-center font-mono text-[10px] font-bold tracking-[0.3em] text-black">
        UNCLASSIFIED // EXERCISE — NOTIONAL DATA — DECISION SUPPORT ONLY
      </div>
      <Header link={link} health={health} />

      <main className="grid flex-1 grid-cols-1 gap-3 p-3 xl:grid-cols-12">
        {/* Left: ingest + feed */}
        <div className="flex min-h-0 flex-col gap-3 xl:col-span-3">
          <TelemetryInjector onSubmitted={(id) => setSelectedId(id)} />
          <MissionFeed missions={missions} selectedId={selectedId} onSelect={setSelectedId} />
        </div>

        {/* Center: map + pipeline + logs */}
        <div className="flex min-h-0 flex-col gap-3 xl:col-span-5">
          <StatTiles missions={missions} />
          <Panel title="Tactical picture" right={<span className="font-mono text-[10px] text-slate-500">{selectedId ?? "no track selected"}</span>}>
            <div className="aspect-[4/3] w-full">
              <TacticalMap reference={reference} missions={missions} selectedId={selectedId} onSelect={setSelectedId} />
            </div>
            <MapLegend />
          </Panel>
          <Panel title="Agent pipeline">
            <PipelineTracker mission={selectedMission} />
          </Panel>
          <LogStream logs={logs} selectedId={selectedId} />
        </div>

        {/* Right: HITL */}
        <div className="flex min-h-0 flex-col gap-3 xl:col-span-4">
          <ReviewQueue pending={pending} selectedId={selectedId} onSelect={setSelectedId} />
          <SitrepConsole
            selectedId={selectedId}
            mission={selectedMission}
            review={selectedReview}
            onReviewed={() => {
              refreshPending();
              bumpReviews();
            }}
          />
        </div>
      </main>
    </div>
  );
}
