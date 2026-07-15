import { deriveStageSummary } from "../../lib/format";
import IngestionTrace from "./IngestionTrace";
import PersistedResources from "./PersistedResources";
import AuditLog from "./AuditLog";
import AiAugSection from "./AiAugSection";

export default function DemoPanel({
  runDemo,
  demoPhase,
  loadingStage,
  demoResult,
  aiReady,
  persistReady,
  showReports,
  setShowReports,
  showObservations,
  setShowObservations,
  showAudit,
  setShowAudit,
  showErrorDetails,
  setShowErrorDetails,
  copiedIngestionId,
  setCopiedIngestionId,
  aiAugProps,
}) {
  return (
      <div className="section" id="demo">
        <div className="section-label">Live Demo</div>
        <div className="section-title">See Validor in action</div>
        <div className="section-sub">
          A single run through the full pipeline, replayed end to end: ingestion, validation,
          FHIR R4 persistence, then AI annotation and a patient message draft.
          Every stage emits timestamped audit trail events.
        </div>

        <div className="demo-wrap">
          <div className="scenario-card">
            <div className="scenario-label">
              <span>Clinical scenario</span>
            </div>
            <p className="scenario-text">
              A patient on long-term statin therapy had four lab visits
              (BMP, LFT, lipid panel) in the past year. Most analytes stay within
              range, some are improving over time, but one test (Glucose) has
              concerning trend. Validor identifies the trends, lists priority findings
              and drafts a patient message for clinician approval.
            </p>
          </div>
          <button
            className="btn-primary demo-run-btn"
            onClick={runDemo}
            disabled={demoPhase === "loading"}
            style={{ opacity: demoPhase === "loading" ? 0.6 : 1, cursor: demoPhase === "loading" ? "not-allowed" : "pointer", marginTop: 12 }}
          >
            {demoPhase === "loading" ? "Running…" : "Run Demo →"}
          </button>
          <div className="demo-body">
            {demoPhase === "idle" && (
              <div style={{ padding: 24, color: "var(--muted)", fontSize: 13, fontFamily: "var(--mono)" }}>
                <p>Click <strong style={{ color: "var(--white)" }}>Run Demo</strong> to call the live API.</p>
              </div>
            )}

            {demoPhase === "loading" && (
              <div style={{ padding: 24 }}>
                <div className="result-grid">
                  <div>
                    <div style={{ color: "var(--muted)", fontSize: 12, marginBottom: 16, fontFamily: "var(--mono)" }}>
                      <span className="spinner" /> Processing through pipeline...
                    </div>
                    {["Parse", "Validate", "Normalize", "Persist", "AI annotation", "Message draft"].map((name, i) => {
                      const isAiStage = i >= 4;
                      const passBadge = isAiStage ? "badge-ai-pass" : "badge-pass";
                      const passMono = isAiStage ? "mono-ai-ok" : "mono-ok";
                      return (
                        <div className="stage" key={name}>
                          <span
                            className={`stage-badge ${
                              i < loadingStage ? passBadge : i === loadingStage ? `${passBadge} stage-active` : "badge-skip"
                            }`}
                          >
                            {name.toUpperCase()}
                          </span>
                          <span
                            className={i < loadingStage ? passMono : i === loadingStage ? "mono-val stage-active" : "mono-skip"}
                          >
                            {i < loadingStage ? "✓ completed" : i === loadingStage ? "● running…" : "· waiting"}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                  <div>
                    {persistReady && demoResult && (
                      <>
                        {<PersistedResources reports={demoResult.reports} observations={demoResult.observations} showReports={showReports} setShowReports={setShowReports} showObservations={showObservations} setShowObservations={setShowObservations} />}
                        {<AuditLog events={demoResult.events} showAudit={showAudit} setShowAudit={setShowAudit} />}
                      </>
                    )}
                  </div>
                </div>
                {persistReady && demoResult && <IngestionTrace ingestionId={demoResult.ingestionId} copiedIngestionId={copiedIngestionId} setCopiedIngestionId={setCopiedIngestionId} />}
              </div>
            )}

            {demoPhase === "loading" && aiReady && (
              <>
                <div className="ai-edge-divider">
                  <div className="line" />
                  <span>AI layer output</span>
                  <div className="line" />
                </div>
                {<AiAugSection {...aiAugProps} />}
              </>
            )}

            {demoPhase === "done" && demoResult && (
              <>
                <div
                  className={`status-banner ${
                    demoResult.status === "COMPLETED" ? "status-success" : "status-fail"
                  }`}
                >
                  {demoResult.status === "COMPLETED" ? "✓" : "✗"} FINAL STATUS: {demoResult.status}
                </div>
                <div className="result-grid">
                  <div>
                    <div
                      style={{
                        color: "var(--muted)",
                        fontSize: 11,
                        marginBottom: 12,
                        letterSpacing: "0.06em",
                      }}
                    >
                      PIPELINE STAGES
                    </div>
                    {deriveStageSummary(demoResult.events).map(({ label, state }) => {
                      const isAiStage = label === "AI ANNOTATION" || label === "MESSAGE DRAFT";
                      return (
                        <div className="stage" key={label}>
                          <span
                            className={`stage-badge ${
                              state === "pass"
                                ? isAiStage
                                  ? "badge-ai-pass"
                                  : "badge-pass"
                                : state === "fail"
                                ? "badge-fail"
                                : "badge-skip"
                            }`}
                          >
                            {label}
                          </span>
                          <span
                            className={
                              state === "pass"
                                ? isAiStage
                                  ? "mono-ai-ok"
                                  : "mono-ok"
                                : state === "fail"
                                ? "mono-err"
                                : "mono-skip"
                            }
                          >
                            {state === "pass" ? "✓ completed" : state === "fail" ? "✗ failed" : "↷ skipped"}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                  <div>
                    {demoResult.status === "COMPLETED" ? (
                      <>
                        {<PersistedResources reports={demoResult.reports} observations={demoResult.observations} showReports={showReports} setShowReports={setShowReports} showObservations={showObservations} setShowObservations={setShowObservations} />}
                        {<AuditLog events={demoResult.events} showAudit={showAudit} setShowAudit={setShowAudit} />}
                      </>
                    ) : (
                      <>
                        <button
                          onClick={() => setShowErrorDetails(!showErrorDetails)}
                          style={{
                            background: "rgba(255,77,106,0.1)",
                            border: "1px solid rgba(255,77,106,0.3)",
                            color: "var(--danger)",
                            padding: "8px 12px",
                            borderRadius: 6,
                            fontSize: 13,
                            fontWeight: 600,
                            cursor: "pointer",
                            transition: "background 0.2s",
                            marginBottom: 8,
                            display: "block",
                          }}
                        >
                          {showErrorDetails ? "Hide error details ▼" : "See error details →"}
                        </button>
                        {showErrorDetails && (
                          <div
                            style={{
                              padding: 12,
                              background: "rgba(255,77,106,0.06)",
                              border: "1px solid rgba(255,77,106,0.2)",
                              borderRadius: 6,
                              maxHeight: 400,
                              overflowY: "auto",
                            }}
                          >
                            <div
                              style={{
                                color: "var(--danger)",
                                fontSize: 11,
                                fontWeight: 600,
                                marginBottom: 8,
                                borderBottom: "1px solid rgba(255,77,106,0.3)",
                                paddingBottom: 8,
                              }}
                            >
                              ERRORS
                            </div>
                            {demoResult.errorDetail?.validation_errors ? (
                              <div>
                                {demoResult.errorDetail.validation_errors.map((err, idx) => (
                                  <div key={idx} style={{ marginBottom: 10 }}>
                                    <div>
                                      <span className="mono-key">row    </span>
                                      <span className="mono-err">{err.row_number}</span>
                                    </div>
                                    <div>
                                      <span className="mono-key">field  </span>
                                      <span className="mono-val">{err.field}</span>
                                    </div>
                                    <div>
                                      <span className="mono-key">reason </span>
                                      <span className="mono-val">{err.message}</span>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            ) : (
                              <div>
                                <span className="mono-val">{demoResult.errorDetail?.message || "No error details available"}</span>
                              </div>
                            )}
                          </div>
                        )}
                        <button
                          onClick={() => setShowAudit(!showAudit)}
                          style={{
                            background: "rgba(58,155,255,0.1)",
                            border: "1px solid rgba(58,155,255,0.3)",
                            color: "var(--blue3)",
                            padding: "8px 12px",
                            borderRadius: 6,
                            fontSize: 13,
                            fontWeight: 600,
                            cursor: "pointer",
                            transition: "background 0.2s",
                            marginTop: showErrorDetails ? 8 : 0,
                            display: "block",
                          }}
                        >
                          {showAudit ? "Hide audit log ▼" : `Audit log ×${(demoResult.events || []).length} →`}
                        </button>
                        {showAudit && (
                          <div style={{ marginTop: 8, maxHeight: 300, overflowY: "auto", background: "rgba(58,155,255,0.05)", border: "1px solid var(--border)", borderRadius: 6, padding: 12 }}>
                            {(demoResult.events || []).map((evt) => {
                              const date = new Date(evt.occurred_at || evt.timestamp || evt.created_at || "");
                              const ts = !isNaN(date.getTime())
                                ? `${date.toLocaleTimeString("en-US", { hour12: false })}.${String(date.getMilliseconds()).padStart(3, "0")}`
                                : "N/A";
                              const isFailed = evt.event_type.endsWith("_FAILED");
                              let stageClass = "es-parse";
                              if (evt.event_type.includes("VALIDATION")) stageClass = "es-valid";
                              else if (evt.event_type.includes("NORMALIZATION")) stageClass = "es-norm";
                              else if (evt.event_type.includes("FHIR")) stageClass = "es-fhir";
                              return (
                                <div key={evt.event_id} className="event-row">
                                  <span className="event-ts">{ts}</span>
                                  <span
                                    className={`event-stage ${!isFailed ? stageClass : ""}`}
                                    style={isFailed ? { background: "rgba(255,77,106,0.08)", color: "var(--danger)" } : {}}
                                  >
                                    {evt.event_type.replace(/_SUCCEEDED|_FAILED/g, "").replace(/_/g, " ")}
                                  </span>
                                  <span className="event-msg" style={isFailed ? { color: "var(--danger)" } : {}}>
                                    {evt.message}
                                  </span>
                                </div>
                              );
                            })}
                          </div>
                        )}
                      </>
                    )}
                  </div>
                </div>
                {<IngestionTrace ingestionId={demoResult.ingestionId} copiedIngestionId={copiedIngestionId} setCopiedIngestionId={setCopiedIngestionId} />}
                {aiReady && (
                  <>
                    <div className="ai-edge-divider">
                      <div className="line" />
                      <span>AI layer output</span>
                      <div className="line" />
                    </div>
                    {<AiAugSection {...aiAugProps} />}
                  </>
                )}
              </>
            )}
          </div>
        </div>
      </div>
  );
}
