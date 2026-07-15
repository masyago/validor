import Head from "next/head";
import { useState, useRef, useEffect } from "react";
import { fmtObsValue, fmtDate } from "../src/lib/format";
import { patientMessageToText, buildEmailTemplate } from "../src/lib/patientMessage";
import { globalCss } from "../src/styles/globalCss";
import SiteNav from "../src/components/SiteNav";
import Hero from "../src/components/Hero";
import PipelineDiagram from "../src/components/PipelineDiagram";
import SiteFooter from "../src/components/SiteFooter";
import ContactModal from "../src/components/ContactModal";
import DemoPanel from "../src/components/demo/DemoPanel";

export default function Home() {
  const [contactOpen, setContactOpen] = useState(false);
  const [contactMessage, setContactMessage] = useState("");
  const [contactStatus, setContactStatus] = useState("idle"); // idle | sending | sent | error
  const [contactHoneypot, setContactHoneypot] = useState("");
  const [demoPhase, setDemoPhase] = useState("idle");
  const [loadingStage, setLoadingStage] = useState(0);
  const [demoResult, setDemoResult] = useState(null);
  const [demoError, setDemoError] = useState(null);
  const [showReports, setShowReports] = useState(false);
  const [showObservations, setShowObservations] = useState(false);
  const [showAudit, setShowAudit] = useState(false);
  const [showErrorDetails, setShowErrorDetails] = useState(false);
  const [aiTab, setAiTab] = useState("annotation");
  const [openFindings, setOpenFindings] = useState({});
  const [history, setHistory] = useState({});
  const [showOther, setShowOther] = useState(false);
  const [emailBody, setEmailBody] = useState("");
  const [copied, setCopied] = useState(false);
  const [sentNotice, setSentNotice] = useState(false);
  const [patientMessage, setPatientMessage] = useState(null);
  const [rejecting, setRejecting] = useState(false);
  const [rejectReason, setRejectReason] = useState("");
  const [aiReady, setAiReady] = useState(false);
  const [persistReady, setPersistReady] = useState(false);
  const [copiedIngestionId, setCopiedIngestionId] = useState(false);
  const pollRef = useRef(null);
  const textareaRef = useRef(null);
  const aiReadyCheckedRef = useRef(false);
  const persistReadyCheckedRef = useRef(false);

  useEffect(() => () => clearInterval(pollRef.current), []);

  // Establish/reuse the session cookie on load and purge any ingestion left
  // over under it (covers a reopened tab without waiting for the TTL sweep).
  useEffect(() => {
    fetch("/v1/session/start", { method: "POST", credentials: "include" }).catch(() => {});
  }, []);

  async function runDemo() {
    clearInterval(pollRef.current);
    setDemoPhase("loading");
    setDemoResult(null);
    setDemoError(null);
    setLoadingStage(0);
    setAiReady(false);
    aiReadyCheckedRef.current = false;
    setPersistReady(false);
    persistReadyCheckedRef.current = false;

    // Purge any ingestion left under this session from a prior run before
    // re-uploading, so a second run in the same tab doesn't stack a duplicate
    // "latest visit" in the patient's history.
    await fetch("/v1/session/start", { method: "POST", credentials: "include" }).catch(() => {});

    const csvPath = "/demo-csv/visit4_latest.csv";
    const instrumentId = "demo-instrument-01";
    const fileName = "visit4_latest.csv";

    const csvBlob = await fetch(csvPath)
      .then((r) => r.blob())
      .catch((err) => {
        setDemoError("Could not load demo file: " + err.message);
        setDemoPhase("idle");
        return null;
      });
    if (!csvBlob) return;

    const form = new FormData();
    form.append("file", csvBlob, fileName);
    form.append("uploader_id", "web-demo");
    form.append("spec_version", "analyzer_csv_v1");
    form.append("instrument_id", instrumentId);
    form.append("run_id", "demo-run-" + Date.now());
    form.append("uploader_received_at", new Date().toISOString());

    const postRes = await fetch("/v1/ingestions", { method: "POST", body: form, credentials: "include" }).catch(() => null);
    const postData = postRes ? await postRes.json().catch(() => null) : null;
    const ingestionId = postData?.ingestion_id;
    if (!ingestionId) {
      setDemoError("Upload failed. Is the API running?");
      setDemoPhase("idle");
      return;
    }
    setLoadingStage(1);

    // Reveal persisted resources + ingestion_id as soon as the PERSIST stage
    // finishes, without waiting for the AI stages that follow it.
    async function checkPersistReadyEarly() {
      if (persistReadyCheckedRef.current) return;
      const evRes = await fetch(`/v1/ingestions/${ingestionId}/processing-events`).catch(() => null);
      const events = evRes ? await evRes.json().catch(() => null) : null;
      if (!Array.isArray(events)) return;
      const types = new Set(events.map((e) => e.event_type));
      const succeeded = types.has("FHIR_JSON_GENERATION_SUCCEEDED");
      const failed = types.has("FHIR_JSON_GENERATION_FAILED");
      if (!succeeded && !failed) return;
      persistReadyCheckedRef.current = true;
      if (!succeeded) return;

      const [rpRes, obRes] = await Promise.allSettled([
        fetch(`/v1/ingestions/${ingestionId}/diagnostic-reports?include_json=1`).then((r) => r.json()),
        fetch(`/v1/ingestions/${ingestionId}/observations?include_json=1&limit=200`).then((r) => r.json()),
      ]);
      const observations = obRes.status === "fulfilled" && Array.isArray(obRes.value) ? obRes.value : [];
      const reports = rpRes.status === "fulfilled" && Array.isArray(rpRes.value) ? rpRes.value : [];
      setDemoResult((prev) => ({
        ...(prev || {}),
        ingestionId,
        reports,
        observations,
        events,
      }));
      setPersistReady(true);
    }

    // Reveal the AI annotation tab as soon as its own stage finishes, without
    // waiting for message-draft (which runs after it and can take longer) —
    // detected via the processing-event log rather than overall ingestion
    // status, since that only goes terminal once both AI stages are done.
    async function checkAiReadyEarly() {
      if (aiReadyCheckedRef.current) return;
      const evRes = await fetch(`/v1/ingestions/${ingestionId}/processing-events`).catch(() => null);
      const events = evRes ? await evRes.json().catch(() => null) : null;
      if (!Array.isArray(events)) return;
      const types = new Set(events.map((e) => e.event_type));
      const succeeded = types.has("AI_ENRICHMENT_SUCCEEDED");
      const failed = types.has("AI_ENRICHMENT_FAILED");
      if (!succeeded && !failed) return;
      aiReadyCheckedRef.current = true;

      let reports = [];
      let observations = [];
      let aiAnnotation = null;
      if (succeeded) {
        const [rpRes, obRes, aiRes] = await Promise.allSettled([
          fetch(`/v1/ingestions/${ingestionId}/diagnostic-reports?include_json=1`).then((r) => r.json()),
          fetch(`/v1/ingestions/${ingestionId}/observations?include_json=1&limit=200`).then((r) => r.json()),
          fetch(`/v1/ingestions/${ingestionId}/ai_annotation`).then((r) => (r.ok ? r.json() : [])),
        ]);
        observations = obRes.status === "fulfilled" && Array.isArray(obRes.value) ? obRes.value : [];
        reports = rpRes.status === "fulfilled" && Array.isArray(rpRes.value) ? rpRes.value : [];
        const aiRows = aiRes.status === "fulfilled" && Array.isArray(aiRes.value) ? aiRes.value : [];
        aiAnnotation =
          aiRows.find((a) => a && a.validation_status === "ACCEPTED" && a.content_json) ||
          aiRows.find((a) => a && a.content_json) ||
          null;
      }
      setDemoResult({
        ingestionId,
        status: undefined,
        errorDetail: undefined,
        events,
        reports,
        observations,
        aiAnnotation,
        patientMessage: null,
        patientId: observations.find((o) => o && o.patient_id)?.patient_id || null,
      });
      setAiReady(true);
    }

    const TERMINAL = new Set(["COMPLETED", "FAILED VALIDATION", "FAILED"]);
    let finalStatus = null;
    await new Promise((resolve) => {
      pollRef.current = setInterval(async () => {
        const res = await fetch(`/v1/ingestions/${ingestionId}`).catch(() => null);
        if (!res) return;
        const body = await res.json().catch(() => null);
        if (!body) return;
        if (body.status === "PROCESSING") {
          setLoadingStage((s) => Math.min(s + 1, 5));
          checkPersistReadyEarly();
          checkAiReadyEarly();
        }
        if (TERMINAL.has(body.status)) {
          finalStatus = body;
          clearInterval(pollRef.current);
          resolve();
        }
      }, 1000);
    });
    setLoadingStage(6);

    const [evRes, rpRes, obRes, aiRes, pmRes] = await Promise.allSettled([
      fetch(`/v1/ingestions/${ingestionId}/processing-events`).then((r) => r.json()),
      fetch(`/v1/ingestions/${ingestionId}/diagnostic-reports?include_json=1`).then((r) => r.json()),
      fetch(`/v1/ingestions/${ingestionId}/observations?include_json=1&limit=200`).then((r) => r.json()),
      fetch(`/v1/ingestions/${ingestionId}/ai_annotation`).then((r) => (r.ok ? r.json() : [])),
      fetch(`/v1/ingestions/${ingestionId}/patient_message`).then((r) => (r.ok ? r.json() : null)),
    ]);

    const observations = obRes.status === "fulfilled" && Array.isArray(obRes.value) ? obRes.value : [];
    const reports = rpRes.status === "fulfilled" && Array.isArray(rpRes.value) ? rpRes.value : [];
    const aiRows = aiRes.status === "fulfilled" && Array.isArray(aiRes.value) ? aiRes.value : [];
    // Prefer an ACCEPTED annotation; fall back to any annotation with content.
    const aiAnnotation =
      aiRows.find((a) => a && a.validation_status === "ACCEPTED" && a.content_json) ||
      aiRows.find((a) => a && a.content_json) ||
      null;
    const patientMessage = pmRes.status === "fulfilled" ? pmRes.value : null;
    const patientId = observations.find((o) => o && o.patient_id)?.patient_id || null;

    const resultObj = {
      ingestionId,
      status: finalStatus?.status,
      errorDetail: finalStatus?.error_detail,
      events: evRes.status === "fulfilled" ? evRes.value : [],
      reports,
      observations,
      aiAnnotation,
      patientMessage,
      patientId,
    };

    setDemoResult(resultObj);
    setPatientMessage(patientMessage);
    // Prefer the clinician-reviewable draft the backend actually produced;
    // fall back to a local template when no LLM-backed draft exists.
    setEmailBody(
      patientMessage ? patientMessageToText(patientMessage, resultObj) : buildEmailTemplate(resultObj)
    );
    // Fallback in case the early per-tick check never caught the AI stage
    // (e.g. it finished between polls) — make sure the tab still reveals.
    if (resultObj.status === "COMPLETED") setAiReady(true);
    setDemoPhase("done");
  }

  async function toggleHistory(code) {
    const cur = history[code];
    if (cur && cur.rows != null) {
      setHistory((p) => ({ ...p, [code]: { ...cur, shown: !cur.shown } }));
      return;
    }
    const pid = demoResult?.patientId;
    if (!pid) {
      setHistory((p) => ({ ...p, [code]: { loading: false, shown: true, rows: [] } }));
      return;
    }
    setHistory((p) => ({ ...p, [code]: { loading: true, shown: true } }));
    try {
      const res = await fetch(`/v1/patients/${pid}/observations?limit=200&offset=0`);
      const all = await res.json();
      // Include the current result (it's in the patient's observations) so the
      // table matches the trend chart, which plots it too. Dedup by
      // observation_id to stay robust against any transient duplicate rows.
      const currentIds = new Set((demoResult?.observations || []).map((o) => o.observation_id));
      const seen = new Set();
      const rows = (Array.isArray(all) ? all : [])
        .filter((o) => o.code === code)
        .filter((o) => {
          if (seen.has(o.observation_id)) return false;
          seen.add(o.observation_id);
          return true;
        })
        .sort((a, b) => (a.effective_at < b.effective_at ? 1 : -1))
        .map((o) => ({
          ...o,
          date: fmtDate(o.effective_at),
          result: fmtObsValue(o),
          isCurrent: currentIds.has(o.observation_id),
        }));
      setHistory((p) => ({ ...p, [code]: { loading: false, shown: true, rows } }));
    } catch (e) {
      setHistory((p) => ({ ...p, [code]: { loading: false, shown: true, error: "Could not load history." } }));
    }
  }

  function resetDraft() {
    setEmailBody(
      patientMessage ? patientMessageToText(patientMessage, demoResult) : buildEmailTemplate(demoResult)
    );
  }

  async function sendDraft() {
    // Demo-send. When a real backend draft exists, drive the clinician gate for
    // real: approve (with any edits) then send. Otherwise it's a local no-op.
    const pm = patientMessage;
    if (pm && pm.patient_message_id) {
      try {
        const id = pm.patient_message_id;
        if (pm.review_status !== "APPROVED" && pm.review_status !== "SENT") {
          const editedText = emailBody;
          await fetch(`/v1/patient_messages/${id}/approve`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              approved_by: "demo-clinician",
              // Preserve the structured draft; record the reviewed text too.
              final_content_json: {
                ...(pm.draft_content_json || {}),
                reviewed_text: editedText,
              },
            }),
          });
        }
        const sendRes = await fetch(`/v1/patient_messages/${id}/send`, { method: "POST" });
        if (sendRes.ok) {
          const updated = await sendRes.json();
          setPatientMessage(updated);
        }
      } catch (e) {
        // fall through to the demo notice regardless
      }
    }
    setSentNotice(true);
    setTimeout(() => setSentNotice(false), 4000);
  }

  async function rejectDraft() {
    // Clinician rejects the draft with a reason. Drives the human gate:
    // review_status -> REJECTED (the message is no longer active).
    const pm = patientMessage;
    if (!rejectReason.trim()) return;
    if (pm && pm.patient_message_id) {
      try {
        const res = await fetch(`/v1/patient_messages/${pm.patient_message_id}/reject`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            reviewed_by: "demo-clinician",
            note: rejectReason.trim(),
          }),
        });
        if (res.ok) {
          const updated = await res.json();
          setPatientMessage(updated);
        }
      } catch (e) {
        // Demo UI — swallow and close the panel regardless.
      }
    }
    setRejecting(false);
    setRejectReason("");
  }

  async function sendContact() {
    if (!contactMessage.trim()) return;
    setContactStatus("sending");
    try {
      const res = await fetch("/api/contact", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: contactMessage, honeypot: contactHoneypot }),
      });
      if (!res.ok) throw new Error("send failed");
      setContactStatus("sent");
      setTimeout(() => {
        setContactOpen(false);
        setContactStatus("idle");
        setContactMessage("");
        setContactHoneypot("");
      }, 1500);
    } catch {
      setContactStatus("error");
    }
  }

  async function copyDraft() {
    try {
      await navigator.clipboard.writeText(emailBody);
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    } catch {
      // Clipboard API unavailable (e.g. non-secure context) — no-op.
    }
  }

  // Props threaded through DemoPanel to the AI-augmentation section.
  const aiAugProps = {
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
  };

  return (
    <>
      <Head>
        <title>Validor</title>
        <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
        <link rel="shortcut icon" href="/favicon.svg" />
      </Head>
      <style dangerouslySetInnerHTML={{ __html: globalCss }} />

      <SiteNav />

      <Hero />

      <DemoPanel
        runDemo={runDemo}
        demoPhase={demoPhase}
        loadingStage={loadingStage}
        demoResult={demoResult}
        aiReady={aiReady}
        persistReady={persistReady}
        showReports={showReports}
        setShowReports={setShowReports}
        showObservations={showObservations}
        setShowObservations={setShowObservations}
        showAudit={showAudit}
        setShowAudit={setShowAudit}
        showErrorDetails={showErrorDetails}
        setShowErrorDetails={setShowErrorDetails}
        copiedIngestionId={copiedIngestionId}
        setCopiedIngestionId={setCopiedIngestionId}
        aiAugProps={aiAugProps}
      />

      <PipelineDiagram />

      <SiteFooter onContact={() => setContactOpen(true)} />

      <ContactModal
        open={contactOpen}
        message={contactMessage}
        setMessage={setContactMessage}
        status={contactStatus}
        honeypot={contactHoneypot}
        setHoneypot={setContactHoneypot}
        onClose={() => setContactOpen(false)}
        onCancel={() => { setContactOpen(false); setContactMessage(""); setContactHoneypot(""); setContactStatus("idle"); }}
        onSend={sendContact}
      />
    </>
  );
}
