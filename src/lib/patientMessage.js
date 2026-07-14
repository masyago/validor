// Builders for the clinician-reviewable patient-message letter body. Extracted
// from pages/index.js — pure functions; the caller passes `result` explicitly.
import { isImprovedFinding } from "./format";

// Patient identity is resolved by SQL from patient_id and injected here — it is never
// shared with the AI. Hardcoded for the demo.
export const PATIENT_NAME = "Jane Doe";

export function labDateFromResult(result) {
  // The lab collection date, formatted for the subject line. Derived from the
  // observations because patientMessageToText runs outside the render scope
  // where `collected` is computed. Returns "" when unknown so the caller can
  // omit the date rather than print a placeholder.
  const obs = Array.isArray(result?.observations) ? result.observations : [];
  const times = obs.map((o) => o.effective_at).filter(Boolean).sort();
  if (times.length) {
    const d = new Date(times[times.length - 1]);
    if (!isNaN(d.getTime())) {
      return d.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" });
    }
  }
  return "";
}

export function patientMessageToText(pm, result) {
  // Render the clinician-reviewable draft as a letter body. The recipient
  // name, the lab date, the clinic name and the signature are applied only
  // here at render time — never baked into the draft_content_json the LLM
  // produced. `result` is passed explicitly by every caller (the demoResult
  // state has not applied yet right after ingestion). The "To:" and "Subject:"
  // lines live in the composer header, not in the body — the body starts with
  // the salutation.
  const c = (pm && (pm.final_content_json || pm.draft_content_json)) || {};
  const firstName = pm?.patient_given_name || "[patient first name]";
  const findings = Array.isArray(c.abnormal_findings) ? c.abnormal_findings : [];
  const improvedFindings = Array.isArray(c.improved_findings) ? c.improved_findings : [];

  const lines = [];
  lines.push(`Dear ${firstName},`, "");
  if (c.opening) lines.push(c.opening, "");
  if (c.normal_summary && c.normal_summary.trim()) lines.push(c.normal_summary, "");
  if (improvedFindings.length) {
    lines.push("The following results have returned to normal since your last test:");
    improvedFindings.forEach((f) => lines.push(`- ${f.title}: ${f.explanation}`, ""));
  }
  if (findings.length) {
    lines.push("The following results fell outside normal ranges and require follow-up:");
    findings.forEach((f) => lines.push(`- ${f.title}: ${f.explanation}`, ""));
    lines.push("Please schedule a follow-up appointment with your clinician to discuss these results.");
  } else {
    lines.push("No follow-up appointment needed at this time.");
  }
  lines.push("", "Regards,", "Dr. [clinician name]");
  return lines.join("\n").trim();
}

export function buildEmailTemplate(result) {
  // Fallback letter body for the no-backend demo path — mirrors the shape of
  // the real draft rendered by patientMessageToText. The "To:" and
  // "Subject:" lines live in the composer header, not in the body.
  const obs = Array.isArray(result?.observations) ? result.observations : [];
  const allFindings = result?.aiAnnotation?.content_json?.analyte_findings || [];
  const improvedFindings = allFindings.filter((f) => isImprovedFinding(f));
  const findings = allFindings.filter((f) => !isImprovedFinding(f));

  const lines = [];
  lines.push(`Dear ${PATIENT_NAME.split(" ")[0]},`, "");
  lines.push("Your blood test results are now available. Here is a summary of the key findings.", "");
  lines.push("Most results were within normal ranges.", "");
  if (improvedFindings.length) {
    lines.push("The following results have returned to normal since your last test:");
    improvedFindings.forEach((f) => {
      const o = obs.find((x) => x.code === f.analyte_code);
      const name = o?.display || f.analyte_code;
      const desc = f.description || "This result has returned to within the normal range.";
      lines.push(`- ${name}: ${desc}`, "");
    });
  }
  if (findings.length) {
    lines.push("The following results fell outside normal ranges and require follow-up:");
    findings.forEach((f) => {
      const o = obs.find((x) => x.code === f.analyte_code);
      const name = o?.display || f.analyte_code;
      const desc = f.description || "This result was outside the normal range and will be reviewed.";
      lines.push(`- ${name}: ${desc}`, "");
    });
    lines.push("Please schedule a follow-up appointment with your clinician to discuss these results.");
  } else {
    lines.push("No follow-up appointment needed at this time.");
  }
  lines.push("", "Regards,", "Dr. [clinician name]");
  return lines.join("\n");
}
