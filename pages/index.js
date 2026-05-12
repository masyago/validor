import Head from "next/head";
import { useState, useRef, useEffect } from "react";

const globalCss = `*{box-sizing:border-box;margin:0;padding:0}
:root{
  --navy:#050e1a;
  --navy2:#0a1628;
  --navy3:#0f2040;
  --blue1:#0d4fa8;
  --blue2:#1a6fd4;
  --blue3:#3a9bff;
  --blue4:#7ec4ff;
  --cyan:#00d4ff;
  --white:#f0f6ff;
  --muted:#7a99c4;
  --border:rgba(58,155,255,0.15);
  --border2:rgba(58,155,255,0.3);
  --success:#00e87a;
  --danger:#ff4d6a;
  --font:'Space Grotesk',sans-serif;
  --mono:'JetBrains Mono',monospace;
}
body{background:var(--navy);color:var(--white);font-family:var(--font);line-height:1.6;overflow-x:hidden}

/* NAV */
nav{display:flex;align-items:center;justify-content:space-between;padding:20px 48px;border-bottom:1px solid var(--border);position:sticky;top:0;background:rgba(5,14,26,0.92);backdrop-filter:blur(12px);z-index:100}
.nav-logo{display:flex;align-items:center;gap:10px}
.nav-logo-icon{width:32px;height:32px;background:linear-gradient(135deg,var(--blue1),var(--blue3));border-radius:8px;display:flex;align-items:center;justify-content:center}
.nav-logo-icon svg{width:18px;height:18px}
.nav-logo-text{font-size:18px;font-weight:700;letter-spacing:-0.02em;color:var(--white)}
.nav-links{display:flex;gap:28px;font-size:14px;color:var(--muted)}
.nav-links a{color:var(--muted);text-decoration:none;transition:color .2s}
.nav-links a:hover{color:var(--white)}
.nav-cta{background:var(--blue3);color:var(--navy);padding:8px 20px;border-radius:6px;font-size:13px;font-weight:600;text-decoration:none;transition:background .2s}
.nav-cta:hover{background:var(--blue4)}

/* HERO */
.hero{padding:100px 48px 80px;max-width:1100px;margin:0 auto;position:relative}
.hero-badge{display:inline-flex;align-items:center;gap:8px;background:rgba(58,155,255,0.1);border:1px solid var(--border2);border-radius:100px;padding:6px 14px;font-size:12px;font-weight:500;color:var(--blue3);margin-bottom:32px;font-family:var(--mono)}
.hero-badge-dot{width:6px;height:6px;border-radius:50%;background:var(--cyan);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.6;transform:scale(0.8)}}
h1{font-size:clamp(40px,5vw,64px);font-weight:700;letter-spacing:-0.03em;line-height:1.1;margin-bottom:24px}
h1 em{font-style:normal;background:linear-gradient(90deg,var(--blue3),var(--cyan));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.hero-sub{font-size:18px;color:var(--muted);max-width:580px;margin-bottom:48px;font-weight:400;line-height:1.7}
.hero-actions{display:flex;gap:16px;flex-wrap:wrap}
.btn-primary{background:var(--blue3);color:var(--navy);padding:14px 28px;border-radius:8px;font-weight:600;font-size:15px;text-decoration:none;transition:all .2s;border:none;cursor:pointer}
.btn-primary:hover{background:var(--blue4);transform:translateY(-1px)}
.btn-ghost{background:transparent;color:var(--white);padding:14px 28px;border-radius:8px;font-weight:500;font-size:15px;text-decoration:none;border:1px solid var(--border2);transition:all .2s;cursor:pointer}
.btn-ghost:hover{border-color:var(--blue3);color:var(--blue3)}

/* STATS STRIP */
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--border);border:1px solid var(--border);border-radius:12px;overflow:hidden;max-width:1100px;margin:0 auto 80px;padding:0 48px;box-sizing:content-box;margin-left:48px;margin-right:48px}
.stat{background:var(--navy2);padding:28px 32px}
.stat-num{font-size:28px;font-weight:700;color:var(--blue3);font-family:var(--mono);letter-spacing:-0.02em}
.stat-label{font-size:13px;color:var(--muted);margin-top:4px}

/* PIPELINE SECTION */
.section{padding:80px 48px;max-width:1100px;margin:0 auto}
.section-label{font-size:12px;font-weight:600;color:var(--blue3);letter-spacing:0.1em;text-transform:uppercase;font-family:var(--mono);margin-bottom:12px}
.section-title{font-size:36px;font-weight:700;letter-spacing:-0.02em;margin-bottom:16px}
.section-sub{font-size:16px;color:var(--muted);max-width:520px;margin-bottom:56px}

/* PIPELINE DIAGRAM */
.pipeline{display:flex;align-items:center;gap:0;overflow-x:auto;padding-bottom:8px}
.pipe-step{flex:1;min-width:140px;position:relative}
.pipe-box{background:var(--navy2);border:1px solid var(--border);border-radius:10px;padding:20px 16px;text-align:center;transition:all .25s;cursor:pointer}
.pipe-box:hover{border-color:var(--blue3);background:var(--navy3);transform:translateY(-3px)}
.pipe-icon{width:36px;height:36px;background:rgba(58,155,255,0.12);border-radius:8px;margin:0 auto 12px;display:flex;align-items:center;justify-content:center;font-size:18px}
.pipe-name{font-size:13px;font-weight:600;color:var(--white);margin-bottom:4px}
.pipe-desc{font-size:11px;color:var(--muted)}
.pipe-arrow{color:var(--blue2);font-size:18px;flex-shrink:0;padding:0 8px;margin-top:-20px}
.pipe-input{font-family:var(--mono);font-size:11px;color:var(--muted);text-align:center;margin-bottom:8px}

/* FEATURES GRID */
.features{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:1px;background:var(--border);border:1px solid var(--border);border-radius:16px;overflow:hidden;margin-top:56px}
.feature{background:var(--navy2);padding:32px;transition:background .2s}
.feature:hover{background:var(--navy3)}
.feature-icon{width:40px;height:40px;border-radius:10px;margin-bottom:20px;display:flex;align-items:center;justify-content:center;font-size:20px}
.feature-icon.blue{background:rgba(58,155,255,0.12)}
.feature-icon.cyan{background:rgba(0,212,255,0.1)}
.feature-icon.green{background:rgba(0,232,122,0.1)}
.feature-title{font-size:16px;font-weight:600;margin-bottom:8px}
.feature-desc{font-size:14px;color:var(--muted);line-height:1.6}

/* DEMO SECTION */
.demo-wrap{background:var(--navy2);border:1px solid var(--border);border-radius:16px;overflow:hidden;margin-top:0}
.demo-tabs{display:flex;border-bottom:1px solid var(--border)}
.demo-tab{padding:14px 24px;font-size:13px;font-weight:500;cursor:pointer;color:var(--muted);border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .2s;font-family:var(--mono)}
.demo-tab.active{color:var(--blue3);border-bottom-color:var(--blue3)}
.demo-body{padding:24px;font-family:var(--mono);font-size:12px;line-height:1.8;color:#a8c4e0}
.demo-panel{display:none}
.demo-panel.active{display:block}
.status-banner{padding:12px 16px;border-radius:6px;margin-bottom:20px;font-weight:600;font-size:13px}
.status-success{background:rgba(0,232,122,0.08);border:1px solid rgba(0,232,122,0.25);color:var(--success)}
.status-fail{background:rgba(255,77,106,0.08);border:1px solid rgba(255,77,106,0.25);color:var(--danger)}
.mono-key{color:#7ec4ff}
.mono-val{color:#c8e6ff}
.mono-path{color:var(--cyan)}
.mono-ok{color:var(--success)}
.mono-err{color:var(--danger)}
.mono-skip{color:#888}
.mono-sep{color:#2a4060}
.stage{display:flex;align-items:center;gap:12px;margin:4px 0}
.stage-badge{font-size:10px;padding:2px 8px;border-radius:4px;font-weight:600;min-width:100px;text-align:center;letter-spacing:0.04em}
.badge-pass{background:rgba(0,232,122,0.1);color:var(--success);border:1px solid rgba(0,232,122,0.2)}
.badge-fail{background:rgba(255,77,106,0.1);color:var(--danger);border:1px solid rgba(255,77,106,0.2)}
.badge-skip{background:rgba(120,120,120,0.1);color:#666;border:1px solid rgba(120,120,120,0.15)}

/* DEMO LOADING STATES */
@keyframes spin{to{transform:rotate(360deg)}}
.spinner{width:14px;height:14px;border:2px solid var(--border2);border-top-color:var(--blue3);border-radius:50%;animation:spin .8s linear infinite;display:inline-block;vertical-align:middle;margin-right:6px}
@keyframes stagePulse{0%,100%{opacity:1}50%{opacity:0.4}}
.stage-active{animation:stagePulse 1s ease-in-out infinite}
.demo-run-btn{margin:0 24px 24px;display:block;width:calc(100% - 48px)}

/* TRUST / AUDITABILITY */
.audit-grid{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-top:0}
.audit-card{background:var(--navy2);border:1px solid var(--border);border-radius:12px;padding:24px}
.audit-title{font-size:13px;font-weight:600;color:var(--muted);margin-bottom:16px;font-family:var(--mono);letter-spacing:0.06em}
.event-row{display:flex;align-items:center;gap:12px;padding:8px 0;border-bottom:1px solid rgba(58,155,255,0.07);font-size:12px;font-family:var(--mono)}
.event-row:last-child{border:none}
.event-ts{color:#3a5a80;min-width:70px}
.event-stage{min-width:90px;padding:2px 8px;border-radius:4px;text-align:center;font-size:10px;font-weight:600}
.es-parse{background:rgba(0,212,255,0.08);color:var(--cyan)}
.es-valid{background:rgba(0,232,122,0.08);color:var(--success)}
.es-norm{background:rgba(58,155,255,0.1);color:var(--blue3)}
.es-fhir{background:rgba(126,196,255,0.1);color:var(--blue4)}
.event-msg{color:var(--muted)}

/* FOOTER SECTION */
.cta-band{background:linear-gradient(135deg,var(--navy2),var(--navy3));border:1px solid var(--border);border-radius:20px;padding:64px;text-align:center;margin:80px 48px}
.cta-band h2{font-size:36px;font-weight:700;letter-spacing:-0.02em;margin-bottom:16px}
.cta-band p{font-size:16px;color:var(--muted);max-width:440px;margin:0 auto 40px}

footer{border-top:1px solid var(--border);padding:32px 48px;display:flex;justify-content:space-between;align-items:center;font-size:13px;color:var(--muted)}
.footer-logo{font-weight:700;font-size:15px;color:var(--white)}

/* Grid BG */
body::before{content:'';position:fixed;inset:0;background-image:linear-gradient(var(--border) 1px,transparent 1px),linear-gradient(90deg,var(--border) 1px,transparent 1px);background-size:60px 60px;opacity:0.5;pointer-events:none;z-index:0}
body>*{position:relative;z-index:1}`;

const STAGE_MAP = [
  { label: "PARSE", ok: "PARSE_SUCCEEDED", fail: "PARSE_FAILED" },
  { label: "VALIDATE", ok: "VALIDATION_SUCCEEDED", fail: "VALIDATION_FAILED" },
  { label: "NORMALIZE", ok: "NORMALIZATION_SUCCEEDED", fail: "NORMALIZATION_FAILED" },
  { label: "PERSIST", ok: "FHIR_JSON_GENERATION_SUCCEEDED", fail: "FHIR_JSON_GENERATION_FAILED" },
];

function deriveStageSummary(events) {
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

export default function Home() {
  const [activeTab, setActiveTab] = useState("valid");
  const [demoPhase, setDemoPhase] = useState("idle");
  const [loadingStage, setLoadingStage] = useState(0);
  const [demoResult, setDemoResult] = useState(null);
  const [demoError, setDemoError] = useState(null);
  const pollRef = useRef(null);

  useEffect(() => () => clearInterval(pollRef.current), []);

  function handleTabChange(tab) {
    if (tab === activeTab) return;
    clearInterval(pollRef.current);
    setActiveTab(tab);
    setDemoPhase("idle");
    setDemoResult(null);
    setDemoError(null);
    setLoadingStage(0);
  }

  async function runDemo() {
    clearInterval(pollRef.current);
    setDemoPhase("loading");
    setDemoResult(null);
    setDemoError(null);
    setLoadingStage(0);

    const csvPath = activeTab === "valid" ? "/demo-csv/valid_01.csv" : "/demo-csv/invalid_missing_fields.csv";
    const instrumentId = activeTab === "valid" ? "demo-instrument-01" : "demo-instrument-02";
    const fileName = activeTab === "valid" ? "valid_01.csv" : "invalid_missing_fields.csv";

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

    const postRes = await fetch("/v1/ingestions", { method: "POST", body: form }).catch(() => null);
    const postData = postRes ? await postRes.json().catch(() => null) : null;
    const ingestionId = postData?.ingestion_id;
    if (!ingestionId) {
      setDemoError("Upload failed. Is the API running?");
      setDemoPhase("idle");
      return;
    }
    setLoadingStage(1);

    const TERMINAL = new Set(["COMPLETED", "FAILED VALIDATION", "FAILED"]);
    let finalStatus = null;
    await new Promise((resolve) => {
      pollRef.current = setInterval(async () => {
        const res = await fetch(`/v1/ingestions/${ingestionId}`).catch(() => null);
        if (!res) return;
        const body = await res.json().catch(() => null);
        if (!body) return;
        if (body.status === "PROCESSING") setLoadingStage((s) => Math.min(s + 1, 3));
        if (TERMINAL.has(body.status)) {
          finalStatus = body;
          clearInterval(pollRef.current);
          resolve();
        }
      }, 1000);
    });
    setLoadingStage(4);

    const [evRes, rpRes, obRes] = await Promise.allSettled([
      fetch(`/v1/ingestions/${ingestionId}/processing-events`).then((r) => r.json()),
      fetch(`/v1/ingestions/${ingestionId}/diagnostic-reports?include_json=1`).then((r) => r.json()),
      fetch(`/v1/ingestions/${ingestionId}/observations?include_json=1`).then((r) => r.json()),
    ]);

    setDemoResult({
      ingestionId,
      status: finalStatus?.status,
      events: evRes.status === "fulfilled" ? evRes.value : [],
      reports: rpRes.status === "fulfilled" ? rpRes.value : [],
      observations: obRes.status === "fulfilled" ? obRes.value : [],
    });
    setDemoPhase("done");
  }

  return (
    <>
      <Head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link
          href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap"
          rel="stylesheet"
        />
      </Head>
      <style dangerouslySetInnerHTML={{ __html: globalCss }} />

      <nav>
        <div className="nav-logo">
          <div className="nav-logo-icon">
            <svg viewBox="0 0 18 18" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path
                d="M9 2L15 5.5V12.5L9 16L3 12.5V5.5L9 2Z"
                stroke="white"
                strokeWidth="1.5"
                strokeLinejoin="round"
              />
              <path d="M9 7L11.5 8.5V11.5L9 13L6.5 11.5V8.5L9 7Z" fill="white" />
            </svg>
          </div>
          <span className="nav-logo-text">Validor</span>
        </div>
        <div className="nav-links">
          <a href="#">Pipeline</a>
          <a href="#">Features</a>
          <a href="#">Demo</a>
          <a href="#">Docs</a>
        </div>
        <a href="#demo" className="nav-cta">
          Try Demo →
        </a>
      </nav>

      <div className="hero">
        <div className="hero-badge">
          <span className="hero-badge-dot" />
          FHIR-Compliant · Lab Data Infrastructure
        </div>
        <h1>
          Lab data
          <br />
          you can <em>trust.</em>
        </h1>
        <p className="hero-sub">
          Validor ingests lab analyzer output, validates against business rules, normalizes to FHIR,
          and persists with full provenance — deterministically, atomically, every time.
        </p>
        <div className="hero-actions">
          <a href="#demo" className="btn-primary">
            See it in action
          </a>
          <a href="#pipeline" className="btn-ghost">
            Explore the pipeline
          </a>
        </div>
      </div>

      <div className="stats">
        <div className="stat">
          <div className="stat-num">4</div>
          <div className="stat-label">Pipeline stages</div>
        </div>
        <div className="stat">
          <div className="stat-num">SHA-256</div>
          <div className="stat-label">Content deduplication</div>
        </div>
        <div className="stat">
          <div className="stat-num">100%</div>
          <div className="stat-label">Deterministic output</div>
        </div>
        <div className="stat">
          <div className="stat-num">FHIR R4</div>
          <div className="stat-label">Compliance standard</div>
        </div>
      </div>

      <div className="section" id="pipeline">
        <div className="section-label">Data Flow</div>
        <div className="section-title">From raw CSV to FHIR resources</div>
        <div className="section-sub">
          Every file travels a deterministic path — parsed, validated, normalized, and persisted. No
          partial writes, no surprises.
        </div>

        <div className="pipeline">
          <div className="pipe-step">
            <div className="pipe-input">CSV file</div>
            <div className="pipe-box">
              <div className="pipe-icon">📄</div>
              <div className="pipe-name">Parser</div>
              <div className="pipe-desc">Panel + Test relations</div>
            </div>
          </div>
          <div className="pipe-arrow">→</div>
          <div className="pipe-step">
            <div className="pipe-input">&nbsp;</div>
            <div className="pipe-box">
              <div className="pipe-icon">🛡</div>
              <div className="pipe-name">Validator</div>
              <div className="pipe-desc">Business rule enforcement</div>
            </div>
          </div>
          <div className="pipe-arrow">→</div>
          <div className="pipe-step">
            <div className="pipe-input">&nbsp;</div>
            <div className="pipe-box">
              <div className="pipe-icon">⚙️</div>
              <div className="pipe-name">Normalizer</div>
              <div className="pipe-desc">FHIR transformation</div>
            </div>
          </div>
          <div className="pipe-arrow">→</div>
          <div className="pipe-step">
            <div className="pipe-input">&nbsp;</div>
            <div className="pipe-box">
              <div className="pipe-icon">🗄</div>
              <div className="pipe-name">Persistence</div>
              <div className="pipe-desc">DiagnosticReport + Observation</div>
            </div>
          </div>
          <div className="pipe-arrow">→</div>
          <div className="pipe-step">
            <div className="pipe-input">&nbsp;</div>
            <div className="pipe-box">
              <div className="pipe-icon">📋</div>
              <div className="pipe-name">Events</div>
              <div className="pipe-desc">Full provenance log</div>
            </div>
          </div>
        </div>

        <div className="features">
          <div className="feature">
            <div className="feature-icon blue">🔒</div>
            <div className="feature-title">Idempotency</div>
            <div className="feature-desc">
              Ingestion uniqueness enforced by{" "}
              <code
                style={{
                  color: "var(--blue3)",
                  fontFamily: "var(--mono)",
                  fontSize: 12,
                }}
              >
                (instrument_id, run_id)
              </code>{" "}
              constraint. Resubmit the same file — nothing changes.
            </div>
          </div>
          <div className="feature">
            <div className="feature-icon cyan">🔍</div>
            <div className="feature-title">Deduplication</div>
            <div className="feature-desc">
              SHA-256 content hash detects exact duplicates. Mismatched hashes for the same key are
              rejected before touching the database.
            </div>
          </div>
          <div className="feature">
            <div className="feature-icon green">⚛</div>
            <div className="feature-title">Atomicity</div>
            <div className="feature-desc">
              Validation or normalization failures produce no partial writes. Raw data is always
              persisted; derived resources only on full success.
            </div>
          </div>
          <div className="feature">
            <div className="feature-icon blue">📐</div>
            <div className="feature-title">Determinism</div>
            <div className="feature-desc">
              Same input always produces same output. All transformations are reproducible — no hidden
              state, no runtime variance.
            </div>
          </div>
          <div className="feature">
            <div className="feature-icon cyan">🕐</div>
            <div className="feature-title">Auditability</div>
            <div className="feature-desc">
              Every processing step recorded as a timestamped event with status. Full provenance chain
              from raw upload to persisted FHIR resource.
            </div>
          </div>
          <div className="feature">
            <div className="feature-icon green">📊</div>
            <div className="feature-title">FHIR R4 Output</div>
            <div className="feature-desc">
              Lab results persisted as{" "}
              <code
                style={{
                  color: "var(--blue3)",
                  fontFamily: "var(--mono)",
                  fontSize: 12,
                }}
              >
                DiagnosticReport
              </code>{" "}
              and{" "}
              <code
                style={{
                  color: "var(--blue3)",
                  fontFamily: "var(--mono)",
                  fontSize: 12,
                }}
              >
                Observation
              </code>{" "}
              resources, ready for downstream healthcare systems.
            </div>
          </div>
        </div>
      </div>

      <div className="section" id="demo">
        <div className="section-label">Live Demo</div>
        <div className="section-title">See Validor in action</div>
        <div className="section-sub">
          Two scenarios: a valid file completing the full pipeline, and an invalid file caught at
          validation.
        </div>

        <div className="demo-wrap">
          <div className="demo-tabs">
            <div
              className={`demo-tab${activeTab === "valid" ? " active" : ""}`}
              onClick={() => handleTabChange("valid")}
            >
              ✓ valid_01.csv — Success
            </div>
            <div
              className={`demo-tab${activeTab === "invalid" ? " active" : ""}`}
              onClick={() => handleTabChange("invalid")}
            >
              ✗ invalid_missing_fields.csv — Failure
            </div>
          </div>
          <button
            className="btn-primary demo-run-btn"
            onClick={runDemo}
            disabled={demoPhase === "loading"}
            style={{ opacity: demoPhase === "loading" ? 0.6 : 1, cursor: demoPhase === "loading" ? "not-allowed" : "pointer" }}
          >
            {demoPhase === "loading" ? "Running…" : "Run Demo →"}
          </button>
          <div className="demo-body">
            {demoPhase === "idle" && (
              <div style={{ padding: 24, color: "var(--muted)", fontSize: 13, fontFamily: "var(--mono)" }}>
                <p>Select a scenario above, then click <strong style={{ color: "var(--white)" }}>Run Demo</strong> to call the live API.</p>
              </div>
            )}

            {demoPhase === "loading" && (
              <div style={{ padding: 24 }}>
                <div style={{ color: "var(--muted)", fontSize: 12, marginBottom: 16, fontFamily: "var(--mono)" }}>
                  <span className="spinner" /> Processing through pipeline...
                </div>
                {["Parse", "Validate", "Normalize", "Persist"].map((name, i) => (
                  <div className="stage" key={name}>
                    <span
                      className={`stage-badge ${
                        i < loadingStage ? "badge-pass" : i === loadingStage ? "badge-pass stage-active" : "badge-skip"
                      }`}
                    >
                      {name.toUpperCase()}
                    </span>
                    <span
                      className={i < loadingStage ? "mono-ok" : i === loadingStage ? "mono-val stage-active" : "mono-skip"}
                    >
                      {i < loadingStage ? "✓ completed" : i === loadingStage ? "● running…" : "· waiting"}
                    </span>
                  </div>
                ))}
              </div>
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
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
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
                    {deriveStageSummary(demoResult.events).map(({ label, state }) => (
                      <div className="stage" key={label}>
                        <span
                          className={`stage-badge ${
                            state === "pass" ? "badge-pass" : state === "fail" ? "badge-fail" : "badge-skip"
                          }`}
                        >
                          {label}
                        </span>
                        <span
                          className={state === "pass" ? "mono-ok" : state === "fail" ? "mono-err" : "mono-skip"}
                        >
                          {state === "pass" ? "✓ completed" : state === "fail" ? "✗ failed" : "↷ skipped"}
                        </span>
                      </div>
                    ))}
                  </div>
                  <div>
                    {demoResult.status === "COMPLETED" ? (
                      <>
                        <div
                          style={{
                            color: "var(--muted)",
                            fontSize: 11,
                            marginBottom: 12,
                            letterSpacing: "0.06em",
                          }}
                        >
                          PERSISTED RESOURCES
                        </div>
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          <span
                            style={{
                              background: "rgba(0,212,255,0.08)",
                              border: "1px solid rgba(0,212,255,0.2)",
                              color: "var(--cyan)",
                              padding: "4px 12px",
                              borderRadius: 4,
                              fontSize: 11,
                              fontWeight: 600,
                            }}
                          >
                            DiagnosticReports ×{demoResult.reports.length}
                          </span>
                          <span
                            style={{
                              background: "rgba(0,212,255,0.08)",
                              border: "1px solid rgba(0,212,255,0.2)",
                              color: "var(--cyan)",
                              padding: "4px 12px",
                              borderRadius: 4,
                              fontSize: 11,
                              fontWeight: 600,
                            }}
                          >
                            Observations ×{demoResult.observations.length}
                          </span>
                        </div>
                        <div
                          style={{
                            marginTop: 16,
                            color: "var(--muted)",
                            fontSize: 11,
                          }}
                        >
                          ingestion_id: <span style={{ color: "var(--blue3)" }}>{demoResult.ingestionId}</span>
                        </div>
                      </>
                    ) : (
                      <>
                        <div
                          style={{
                            color: "var(--muted)",
                            fontSize: 11,
                            marginBottom: 12,
                            letterSpacing: "0.06em",
                          }}
                        >
                          FAILURE DETAIL
                        </div>
                        <div
                          style={{
                            padding: 12,
                            background: "rgba(255,77,106,0.06)",
                            border: "1px solid rgba(255,77,106,0.2)",
                            borderRadius: 6,
                          }}
                        >
                          <div
                            style={{
                              color: "var(--danger)",
                              fontSize: 11,
                              fontWeight: 600,
                              marginBottom: 6,
                            }}
                          >
                            PIPELINE ERROR
                          </div>
                          {demoResult.events
                            .filter((e) => e.event_type.endsWith("_FAILED"))
                            .slice(0, 1)
                            .map((e) => (
                              <div key={e.event_id}>
                                <span className="mono-key">stage  </span>
                                <span className="mono-err">{e.event_type}</span>
                                <div>
                                  <span className="mono-key">detail </span>
                                  <span className="mono-val">{e.message}</span>
                                </div>
                              </div>
                            ))}
                        </div>
                      </>
                    )}
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-label">Provenance</div>
        <div className="section-title">Full audit trail, by design</div>
        <div className="section-sub">
          Every ingestion emits a timestamped event log. Every step. Every outcome. Built-in, not
          bolted-on.
        </div>

        <div className="audit-grid">
          <div className="audit-card">
            <div className="audit-title">PROCESSING EVENTS — valid_01</div>
            <div className="event-row">
              <span className="event-ts">09:14:00.012</span>
              <span className="event-stage es-parse">PARSE</span>
              <span className="event-msg">file parsed → 1 panel, 12 tests</span>
            </div>
            <div className="event-row">
              <span className="event-ts">09:14:00.098</span>
              <span className="event-stage es-valid">VALIDATION</span>
              <span className="event-msg">all rules passed</span>
            </div>
            <div className="event-row">
              <span className="event-ts">09:14:00.134</span>
              <span className="event-stage es-norm">NORMALIZE</span>
              <span className="event-msg">FHIR mapping applied</span>
            </div>
            <div className="event-row">
              <span className="event-ts">09:14:00.201</span>
              <span className="event-stage es-fhir">PERSIST</span>
              <span className="event-msg">DiagnosticReport + 12 Observations written</span>
            </div>
          </div>
          <div className="audit-card">
            <div className="audit-title">PROCESSING EVENTS — invalid_missing_fields</div>
            <div className="event-row">
              <span className="event-ts">09:21:04.011</span>
              <span className="event-stage es-parse">PARSE</span>
              <span className="event-msg">file parsed → 1 panel, 11 tests</span>
            </div>
            <div className="event-row">
              <span className="event-ts">09:21:04.089</span>
              <span
                className="event-stage"
                style={{
                  background: "rgba(255,77,106,0.08)",
                  color: "var(--danger)",
                }}
              >
                VALIDATION
              </span>
              <span className="event-msg" style={{ color: "var(--danger)" }}>
                failed: missing 'unit' on row 3
              </span>
            </div>
            <div className="event-row">
              <span className="event-ts">09:21:04.090</span>
              <span
                className="event-stage"
                style={{
                  background: "rgba(120,120,120,0.08)",
                  color: "#555",
                }}
              >
                NORMALIZE
              </span>
              <span className="event-msg" style={{ color: "#444" }}>
                skipped
              </span>
            </div>
            <div className="event-row">
              <span className="event-ts">09:21:04.090</span>
              <span
                className="event-stage"
                style={{
                  background: "rgba(120,120,120,0.08)",
                  color: "#555",
                }}
              >
                PERSIST
              </span>
              <span className="event-msg" style={{ color: "#444" }}>
                skipped — raw ingestion record retained
              </span>
            </div>
          </div>
        </div>
      </div>

      <div className="cta-band">
        <h2>
          Built for reliability.
          <br />
          Designed for healthcare.
        </h2>
        <p>Explore the full pipeline, read the source, or connect to the live API.</p>
        <div style={{ display: "flex", gap: 16, justifyContent: "center", flexWrap: "wrap" }}>
          <a href="#" className="btn-primary">
            Open Live Demo →
          </a>
          <a href="#" className="btn-ghost">
            View on GitHub
          </a>
        </div>
      </div>

      <footer>
        <span className="footer-logo">Validor</span>
        <span>FHIR R4 · PostgreSQL · REST API</span>
        <span>Built with purpose.</span>
      </footer>
    </>
  );
}
