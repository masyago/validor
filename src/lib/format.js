// Pure, closure-free formatting/helpers shared across the demo UI. Extracted
// from pages/index.js — no React state or props involved.

export const STAGE_MAP = [
  { label: "PARSE", ok: "PARSE_SUCCEEDED", fail: "PARSE_FAILED" },
  { label: "VALIDATE", ok: "VALIDATION_SUCCEEDED", fail: "VALIDATION_FAILED" },
  { label: "NORMALIZE", ok: "NORMALIZATION_SUCCEEDED", fail: "NORMALIZATION_FAILED" },
  { label: "PERSIST", ok: "FHIR_JSON_GENERATION_SUCCEEDED", fail: "FHIR_JSON_GENERATION_FAILED" },
  { label: "AI ANNOTATION", ok: "AI_ENRICHMENT_SUCCEEDED", fail: "AI_ENRICHMENT_FAILED" },
  { label: "MESSAGE DRAFT", ok: "MESSAGE_DRAFT_SUCCEEDED", fail: "MESSAGE_DRAFT_FAILED" },
];

export function deriveStageSummary(events) {
  const types = new Set((events || []).map((e) => e.event_type));
  let hitFail = false;
  return STAGE_MAP.map(({ label, ok, fail }) => {
    if (hitFail) return { label, state: "skip" };
    if (types.has(fail)) {
      hitFail = true;
      return { label, state: "fail" };
    }
    if (types.has(ok)) return { label, state: "pass" };
    return { label, state: "skip" };
  });
}

export function truncateIngestionId(id) {
  if (!id || id.length <= 15) return id || "";
  return `${id.slice(0, 8)}…${id.slice(-5)}`;
}

export function flagPointColor(flag) {
  if (flag === "critical_high" || flag === "critical_low") return "var(--danger)";
  if (flag === "high" || flag === "low") return "var(--warn)";
  if (flag === "normal") return "var(--success)";
  return "var(--muted)";
}

export function fmtChartDate(iso) {
  const d = new Date(iso);
  return isNaN(d.getTime()) ? "" : d.toISOString().slice(0, 10);
}

export function fmtAxisNum(v) {
  if (Math.abs(v) >= 100) return String(Math.round(v));
  if (Math.abs(v) >= 10) return String(Math.round(v * 10) / 10);
  return String(Math.round(v * 100) / 100);
}

export function fmtObsValue(o) {
  if (!o) return "—";
  const base = o.value_num != null ? String(o.value_num) : (o.value_text || "");
  if (!base) return "—";
  return o.unit ? `${base} ${o.unit}` : base;
}

export function fmtRef(o) {
  if (!o) return "—";
  const lo = o.ref_low_num;
  const hi = o.ref_high_num;
  if (lo == null && hi == null) return "—";
  return `${lo == null ? "?" : lo}–${hi == null ? "?" : hi}`;
}

export function flagKey(o) {
  const raw = (o?.flag_system_interpretation || o?.flag_analyzer_interpretation || "")
    .toString()
    .toUpperCase();
  if (!raw) return "unknown";
  if (raw.includes("CRITICAL") && raw.includes("LOW")) return "critical_low";
  if (raw.includes("CRITICAL")) return "critical_high";
  if (raw === "HIGH" || raw === "H") return "high";
  if (raw === "LOW" || raw === "L") return "low";
  if (raw === "NORMAL" || raw === "N") return "normal";
  return "unknown";
}

export function flagLabel(o) {
  const k = flagKey(o);
  return k === "unknown" ? "—" : k.toUpperCase();
}

// True when the AI has flagged this finding as a result that was out of
// range in its most recent prior result but is back within range now.
export function isImprovedFinding(f) {
  return (f?.trend_direction || "").toString().trim().toLowerCase() === "improved";
}

export function cardPriorityClass(o, improved) {
  if (improved) return "improved";
  const k = flagKey(o);
  if (k.startsWith("critical")) return "critical";
  if (k === "high" || k === "low") return "high";
  return "elevated";
}

// Higher rank = more clinically urgent; used to order priority findings.
// Improved findings rank below everything else, including "normal" — they
// are good news, not something needing attention, so they sit at the
// bottom of the list.
export function flagRank(o, improved) {
  if (improved) return -1;
  const k = flagKey(o);
  if (k.startsWith("critical")) return 3;
  if (k === "high" || k === "low") return 2;
  if (k === "normal") return 0;
  return 1; // unknown / unmatched
}

// Combines the result flag with value + reference range, e.g. "HIGH: 93 mmol/L [50–80]".
export function fmtResultWithFlag(o, improved) {
  if (!o) return "REVIEW";
  if (improved) return `IMPROVED: ${fmtObsValue(o)} [${fmtRef(o)}]`;
  const k = flagKey(o);
  const prefix = k === "unknown" ? "" : `${k.toUpperCase().replace("_", " ")}: `;
  return `${prefix}${fmtObsValue(o)} [${fmtRef(o)}]`;
}

// Review banner urgency: routine → yellow, anything else → red.
export function reviewPriorityClass(priority) {
  return (priority || "").toString().trim().toLowerCase() === "routine"
    ? "routine"
    : "urgent";
}

// Formats a UTC timestamp in the viewer's local timezone, e.g. "2026-07-05 15:05:14 EDT".
export function fmtGenerated(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "—";
  const p = {};
  new Intl.DateTimeFormat("en-CA", {
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
    hour12: false, timeZoneName: "short",
  }).formatToParts(d).forEach((x) => { p[x.type] = x.value; });
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second} ${p.timeZoneName}`;
}

// Bedrock model ids arrive as long ARNs / inference-profile strings; show just the
// human-readable model name (e.g. "claude-haiku-4-5").
export function shortModelName(modelId) {
  if (!modelId) return "—";
  let s = modelId.toString().trim();
  // ARN form: arn:aws:bedrock:...:foundation-model/anthropic.claude-...  → take last segment
  if (s.includes("/")) s = s.split("/").pop();
  // Strip provider prefix and region qualifier: "us.anthropic.claude-haiku-4-5-v1:0"
  s = s.replace(/^(us|eu|apac|global)\./, "").replace(/^anthropic\./, "");
  // Drop trailing version/token suffixes like "-20250101-v1:0" or ":0"
  s = s.replace(/:.*$/, "").replace(/-v\d+.*$/, "").replace(/-\d{8}$/, "");
  return s || modelId;
}

export function fmtDate(s) {
  const d = new Date(s);
  return isNaN(d.getTime()) ? (s || "") : d.toISOString().slice(0, 10);
}

// Combines the current result with fetched history rows into an ascending,
// numeric-only series for the trend chart (qualitative/text-only results
// can't be plotted but still show in the history table).
export function buildChartSeries(current, rows) {
  const combined = Array.isArray(rows) ? [...rows] : [];
  if (current) combined.push(current);
  const seen = new Set();
  return combined
    .filter((o) => o && typeof o.value_num === "number" && o.effective_at)
    .filter((o) => {
      const key = o.observation_id || `${o.code}-${o.effective_at}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => (a.effective_at > b.effective_at ? 1 : -1))
    .map((o) => ({
      effective_at: o.effective_at,
      value_num: o.value_num,
      flag: flagKey(o),
      isCurrent: !!current && o.observation_id === current.observation_id,
    }));
}
