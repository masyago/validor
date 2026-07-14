import HistoryChart from "../HistoryChart";
import {
  buildChartSeries,
  flagKey,
  flagLabel,
  cardPriorityClass,
  fmtResultWithFlag,
  isImprovedFinding,
  fmtObsValue,
  fmtRef,
  reviewPriorityClass,
  shortModelName,
  fmtGenerated,
} from "../../lib/format";
import { PATIENT_NAME } from "../../lib/patientMessage";

export default function AiAnnotationTab({
  ai,
  content,
  panel,
  collected,
  avgConf,
  findings,
  lookupObs,
  otherObs,
  patientMessage,
  openFindings,
  setOpenFindings,
  history,
  toggleHistory,
  showOther,
  setShowOther,
}) {
  return (
          <div>
            <div className="meta-row">
              <div className="meta-cell"><div className="meta-label">PATIENT</div><div className="meta-val">{
                patientMessage
                  ? [patientMessage.patient_given_name, patientMessage.patient_family_name].filter(Boolean).join(" ") || PATIENT_NAME
                  : PATIENT_NAME
              }</div></div>
              <div className="meta-cell"><div className="meta-label">PANEL</div><div className="meta-val">{panel}</div></div>
              <div className="meta-cell"><div className="meta-label">COLLECTED</div><div className="meta-val">{collected}</div></div>
              <div className="meta-cell"><div className="meta-label">ANNOTATION TYPE</div><div className="meta-val" style={{ color: "var(--warn)" }}>{(ai.annotation_type || content.annotation_type || "—").toString().toUpperCase()}</div></div>
            </div>

            {content.requires_review && (
              <div className={`review-banner ${reviewPriorityClass(content.review_priority)}`}>
                ⚠ Clinician review required{content.review_priority ? ` — priority: ${content.review_priority}` : ""}.
              </div>
            )}

            <div className="section-label">AI Panel Summary</div>
            <div className="summary-card">
              <div className="summary-header">
                <div className="summary-type">{(content.annotation_type || "ANNOTATION").toString().toUpperCase()}</div>
                {avgConf != null && (
                  <div className="conf-wrap">
                    <span className="conf-label">Confidence</span>
                    <div className="conf-bar"><div className="conf-fill" style={{ width: `${Math.round(avgConf * 100)}%` }} /></div>
                    <span className="conf-num">{avgConf.toFixed(2)}</span>
                  </div>
                )}
              </div>
              <div className="summary-text">{content.summary}</div>
              <div className="summary-footer">
                {content.requires_review && <span className="requires-review-tag">⚑ REQUIRES_REVIEW</span>}
                <span className="non-auth-tag">NON-AUTHORITATIVE · FOR REVIEW ONLY</span>
              </div>
            </div>

            {findings.length > 0 && (
              <>
                <div className="section-label">Priority Findings</div>
                <div className="priority-grid">
                  {findings.map((f) => {
                    const o = lookupObs(f.analyte_code);
                    const improved = isImprovedFinding(f);
                    const fk = improved ? "improved" : flagKey(o);
                    const open = !!openFindings[f.analyte_code];
                    const h = history[f.analyte_code];
                    return (
                      <div key={f.analyte_code} className={`finding-card ${cardPriorityClass(o, improved)}`}>
                        <div className="finding-header" onClick={() => setOpenFindings((p) => ({ ...p, [f.analyte_code]: !p[f.analyte_code] }))}>
                          <span className="analyte-name">{o?.display || f.analyte_code}</span>
                          <span className={`analyte-result val-${fk}`}>{fmtResultWithFlag(o, improved)}</span>
                          <span className={`finding-chevron${open ? " open" : ""}`}>▼</span>
                        </div>
                        {open && (
                          <div className="finding-body">
                            <div className="finding-desc">{f.description}</div>
                            <div className="finding-meta">
                              <span className="finding-meta-item">Confidence: <span>{typeof f.confidence === "number" ? f.confidence.toFixed(2) : "—"}</span></span>
                              <span className="finding-meta-item">Trend: <span>{f.trend_direction || "—"}</span></span>
                            </div>
                            <button className="hist-btn" onClick={() => toggleHistory(f.analyte_code)}>
                              {h?.shown ? "Hide history ▲" : "Show history ▾"}
                            </button>
                            {h?.shown && (
                              h.loading ? (
                                <div className="hist-empty">Loading…</div>
                              ) : h.error ? (
                                <div className="hist-empty">{h.error}</div>
                              ) : (
                                <>
                                  <HistoryChart
                                    series={buildChartSeries(o, h.rows)}
                                    unit={o?.unit}
                                    refLow={o?.ref_low_num}
                                    refHigh={o?.ref_high_num}
                                  />
                                  {h.rows && h.rows.length > 0 ? (
                                    <div className="hist-table">
                                      <div className="hist-row head"><div className="hist-date">DATE</div><div className="hist-val">RESULT</div></div>
                                      {h.rows.map((r, i) => (
                                        <div
                                          className="hist-row"
                                          key={i}
                                          style={r.isCurrent ? { fontWeight: 600, color: "var(--white)" } : undefined}
                                        >
                                          <div className="hist-date">{r.date}{r.isCurrent ? " · latest" : ""}</div>
                                          <div className="hist-val">{r.result}</div>
                                        </div>
                                      ))}
                                    </div>
                                  ) : (
                                    <div className="hist-empty">No results for this analyte.</div>
                                  )}
                                </>
                              )
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </>
            )}

            {otherObs.length > 0 && (
              <>
                <div className="section-label collapse-head" onClick={() => setShowOther((v) => !v)}>
                  <span>All Other Results ×{otherObs.length}</span>
                  <span className="finding-chevron" style={{ transform: showOther ? "rotate(180deg)" : "none" }}>▼</span>
                </div>
                {showOther && (
                  <div className="other-table-wrap">
                    <div className="other-row head">
                      <div>ANALYTE</div>
                      <div className="other-val">VALUE</div>
                      <div className="other-ref">REFERENCE</div>
                      <div className="other-flag">FLAG</div>
                    </div>
                    {otherObs.map((o) => {
                      const fk = flagKey(o);
                      return (
                        <div className="other-row" key={o.observation_id}>
                          <div className="other-analyte">{o.display || o.code}</div>
                          <div className={`other-val val-${fk}`}>{fmtObsValue(o)}</div>
                          <div className="other-ref">{fmtRef(o)}</div>
                          <div className="other-flag"><span className={`flag-pill flag-${fk}`}>{flagLabel(o)}</span></div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </>
            )}

            <div className="audit-footer">
              <div className="audit-trace-title">OUTPUT TRACE</div>
              <div className="audit-trace-row">
                <span className="audit-item">Model: <span title={ai.model_id || ""}>{shortModelName(ai.model_id)}</span></span>
                <span className="audit-item">Annotation Schema: <span>{ai.content_schema_version || "—"}</span></span>
                <span className="audit-item">Generated: <span>{fmtGenerated(ai.created_at)}</span></span>
              </div>
            </div>
            <div className="disclaimer">
              AI annotations are non-authoritative and generated by an automated pipeline. They are not medical advice, do not constitute a clinical diagnosis, and must not be used as a substitute for clinician review. All findings require independent clinical interpretation.
            </div>
          </div>
  );
}
