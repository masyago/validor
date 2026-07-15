import { shortModelName, fmtGenerated } from "../../lib/format";
import { PATIENT_NAME } from "../../lib/patientMessage";

export default function MessageDraftTab({
  ai,
  messageDraftPending,
  patientMessage,
  emailLabDate,
  emailSubject,
  textareaRef,
  emailBody,
  setEmailBody,
  copied,
  sentNotice,
  rejecting,
  setRejecting,
  rejectReason,
  setRejectReason,
  resetDraft,
  copyDraft,
  sendDraft,
  rejectDraft,
}) {
  return (
          <div className="composer-wrap">
            <div className="composer-top">
              <div className="composer-title-row">
                <div className="composer-icon">
                <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" width="16" height="16">
                  <rect x="2.5" y="4.5" width="15" height="11" rx="2" />
                  <path d="M3 5.5l7 5.5 7-5.5" />
                </svg>
              </div>
                <div>
                  <div className="composer-title">Draft patient message</div>
                  <div className="composer-subtitle">AI-generated</div>
                </div>
              </div>
              <div className={`draft-tag${
                messageDraftPending ? " status-pending"
                  : patientMessage?.review_status === "REJECTED" ? " status-rejected"
                  : patientMessage?.review_status === "SENT" ? " status-approved"
                  : ""
              }`}><span className="draft-dot" />{
                messageDraftPending ? "Drafting…"
                  : patientMessage?.review_status === "REJECTED" ? "Rejected"
                  : patientMessage?.review_status === "SENT" ? "Approved and Sent"
                  : "Clinician review required"
              }</div>
            </div>
            {messageDraftPending ? (
              <div style={{ padding: "40px 22px", textAlign: "center", color: "var(--muted)", fontSize: 13, fontFamily: "var(--mono)" }}>
                <span className="spinner" /> Drafting patient message…
              </div>
            ) : (
              <>
            <div className="meta-fields">
              <div className="meta-field">
                <span className="meta-field-icon">
                  <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" width="14" height="14">
                    <circle cx="8" cy="5" r="2.75" />
                    <path d="M2.5 13.5c0-2.8 2.5-4.5 5.5-4.5s5.5 1.7 5.5 4.5" />
                  </svg>
                </span>
                <span className="meta-field-val">Patient: {
                patientMessage
                  ? [patientMessage.patient_given_name, patientMessage.patient_family_name].filter(Boolean).join(" ") || PATIENT_NAME
                  : PATIENT_NAME
              }</span>
              </div>
              <div className="meta-field">
                <span className="meta-field-icon">
                  <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" width="14" height="14">
                    <rect x="2" y="3.5" width="12" height="10.5" rx="1.5" />
                    <path d="M2 6.5h12M5.5 2v3M10.5 2v3" />
                  </svg>
                </span>
                <span className="meta-field-val">Lab Date: {emailLabDate || "—"}</span>
              </div>
              <div className="meta-field meta-field-full"><span className="meta-field-val">Subject: {emailSubject}{emailLabDate ? ` — ${emailLabDate}` : ""}</span></div>
            </div>
            <div className="message-area">
              <textarea
                ref={textareaRef}
                className="msg-textarea"
                value={emailBody}
                onChange={(e) => setEmailBody(e.target.value)}
                spellCheck={true}
              />
            </div>
            <div className="draft-actions">
              <button className="draft-btn secondary" onClick={resetDraft}>Reset draft</button>
              <div className="draft-actions-right">
                <button className="draft-btn" onClick={copyDraft}>{copied ? "Copied ✓" : "Copy"}</button>
                <button
                  className="draft-btn danger"
                  onClick={() => setRejecting((v) => !v)}
                  disabled={patientMessage?.review_status === "SENT" || patientMessage?.review_status === "REJECTED"}
                >
                  {patientMessage?.review_status === "REJECTED" ? "Rejected ✗" : "Reject"}
                </button>
                <button className="draft-btn primary" onClick={sendDraft} disabled={patientMessage?.review_status === "SENT" || patientMessage?.review_status === "REJECTED"}>
                  {patientMessage?.review_status === "SENT" ? "Sent ✓" : "Approve & send"}
                </button>
              </div>
            </div>
            {rejecting && (
              <div className="reject-panel">
                <label className="reject-label" htmlFor="reject-reason">Reason for rejection</label>
                <textarea
                  id="reject-reason"
                  className="reject-textarea"
                  value={rejectReason}
                  onChange={(e) => setRejectReason(e.target.value)}
                  placeholder="Required: Why is this draft being rejected? (recorded on the message audit trail)"
                  spellCheck={true}
                  required
                />
                <div className="reject-panel-actions">
                  <button className="draft-btn secondary" onClick={() => { setRejecting(false); setRejectReason(""); }}>Cancel</button>
                  <button className="draft-btn danger" onClick={rejectDraft} disabled={!rejectReason.trim()}>Confirm rejection</button>
                </div>
              </div>
            )}
            {sentNotice && <div className="send-notice">This is a demo. The message wasn't actually delivered.</div>}
            {patientMessage?.review_status === "REJECTED" && (
              <div className="rejected-notice">The message was rejected and cannot be sent to patient.</div>
            )}
            <div className="audit-footer">
              <div className="audit-trace-title">OUTPUT TRACE</div>
              <div className="audit-trace-row">
                <span className="audit-item">Model: <span title={(patientMessage?.model_id || ai.model_id) || ""}>{shortModelName(patientMessage?.model_id || ai.model_id)}</span></span>
                <span className="audit-item">Patient Message Schema: <span>{(patientMessage?.content_schema_version || ai.content_schema_version) || "—"}</span></span>
                <span className="audit-item">Generated: <span>{fmtGenerated(patientMessage?.created_at || ai.created_at)}</span></span>
              </div>
            </div>
            <div className="physician-note">
              <div className="pn-text">
                <strong>Physician review required.</strong> This message does not constitute medical advice and must not be sent without clinical sign-off.
              </div>
            </div>
              </>
            )}
          </div>
  );
}
