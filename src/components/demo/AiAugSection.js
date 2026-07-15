import { isImprovedFinding, flagRank } from "../../lib/format";
import { labDateFromResult } from "../../lib/patientMessage";
import AiAnnotationTab from "./AiAnnotationTab";
import MessageDraftTab from "./MessageDraftTab";

export default function AiAugSection({
  demoResult,
  demoPhase,
  aiTab,
  setAiTab,
  patientMessage,
  openFindings,
  setOpenFindings,
  history,
  toggleHistory,
  showOther,
  setShowOther,
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
  if (!demoResult) return null;
  const messageDraftPending = demoPhase !== "done";
  const ai = demoResult.aiAnnotation;
  if (!ai || !ai.content_json) {
    return (
      <div className="ai-aug">
        <div style={{ color: "var(--muted)", fontSize: 12, fontFamily: "var(--mono)" }}>
          AI annotation not available for this run — the AI enrichment layer may be disabled in this environment.
        </div>
      </div>
    );
  }
    const content = ai.content_json;
    const rawFindings = Array.isArray(content.analyte_findings) ? content.analyte_findings : [];
    const obs = Array.isArray(demoResult.observations) ? demoResult.observations : [];
    // Index observations by both code and display name (case-insensitive) so a
    // finding whose analyte_code differs in case/label (e.g. "K" vs "Potassium")
    // still resolves to its observation instead of rendering a bare code.
    const obsIndex = {};
    obs.forEach((o) => {
      if (!o) return;
      [o.code, o.display].filter(Boolean).forEach((k) => {
        const key = k.toString().trim().toUpperCase();
        if (!(key in obsIndex)) obsIndex[key] = o;
      });
    });
    const lookupObs = (code) =>
      code ? obsIndex[code.toString().trim().toUpperCase()] : undefined;
    // Order findings by clinical urgency (critical → high/low → other → improved).
    const findings = rawFindings
      .map((f) => ({ f, o: lookupObs(f.analyte_code), improved: isImprovedFinding(f) }))
      .sort((a, b) => flagRank(b.o, b.improved) - flagRank(a.o, a.improved))
      .map((x) => x.f);
    const priorityObsIds = new Set(
      findings.map((f) => lookupObs(f.analyte_code)?.observation_id).filter(Boolean)
    );
    const otherObs = obs.filter((o) => !priorityObsIds.has(o.observation_id));
    const confVals = findings.map((f) => f.confidence).filter((v) => typeof v === "number");
    const avgConf = confVals.length ? confVals.reduce((a, b) => a + b, 0) / confVals.length : null;
    const panel = (demoResult.reports || []).map((r) => r.panel_code).filter(Boolean).join(" · ") || "—";
    const times = obs.map((o) => o.effective_at).filter(Boolean).sort();
    let collected = "—";
    if (times.length) {
      const d = new Date(times[times.length - 1]);
      if (!isNaN(d.getTime())) collected = d.toISOString().slice(0, 16).replace("T", " ");
    }
    const emailLabDate = labDateFromResult(demoResult);
    const emailPmContent = (patientMessage && (patientMessage.final_content_json || patientMessage.draft_content_json)) || {};
    const emailSubject = emailPmContent.subject || "Blood test results";
    const messageStatusSubtitle = messageDraftPending
      ? "pending approval"
      : patientMessage?.review_status === "REJECTED"
      ? "rejected"
      : patientMessage?.review_status === "SENT"
      ? "approved and sent"
      : "pending approval";
    return (
      <div className="ai-aug">
        <div className="ai-tabs">
          <div className={`ai-tab${aiTab === "annotation" ? " active" : ""}`} onClick={() => setAiTab("annotation")}>
            <div className="ai-tab-label">AI annotation</div>
            <div className="ai-tab-sub">clinician-facing</div>
          </div>
          <div className={`ai-tab${aiTab === "email" ? " active" : ""}`} onClick={() => setAiTab("email")}>
            <div className="ai-tab-label">Patient message</div>
            <div className="ai-tab-sub">{messageStatusSubtitle}</div>
          </div>
        </div>

        {aiTab === "annotation" && (
          <AiAnnotationTab
            ai={ai}
            content={content}
            panel={panel}
            collected={collected}
            avgConf={avgConf}
            findings={findings}
            lookupObs={lookupObs}
            otherObs={otherObs}
            patientMessage={patientMessage}
            openFindings={openFindings}
            setOpenFindings={setOpenFindings}
            history={history}
            toggleHistory={toggleHistory}
            showOther={showOther}
            setShowOther={setShowOther}
          />
        )}

        {aiTab === "email" && (
          <MessageDraftTab
            ai={ai}
            messageDraftPending={messageDraftPending}
            patientMessage={patientMessage}
            emailLabDate={emailLabDate}
            emailSubject={emailSubject}
            textareaRef={textareaRef}
            emailBody={emailBody}
            setEmailBody={setEmailBody}
            copied={copied}
            sentNotice={sentNotice}
            rejecting={rejecting}
            setRejecting={setRejecting}
            rejectReason={rejectReason}
            setRejectReason={setRejectReason}
            resetDraft={resetDraft}
            copyDraft={copyDraft}
            sendDraft={sendDraft}
            rejectDraft={rejectDraft}
          />
        )}
      </div>
    );
}
