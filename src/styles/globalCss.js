export const globalCss = `*{box-sizing:border-box;margin:0;padding:0}
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
  --warn:#f5a623;
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
.hero{padding:67px 48px 56px;max-width:1100px;margin:0 auto;position:relative;text-align:center}
.hero-sub{margin-left:auto;margin-right:auto}
.hero-actions{justify-content:center}
.hero-badge{display:inline-flex;align-items:center;gap:8px;background:rgba(58,155,255,0.1);border:1px solid var(--border2);border-radius:100px;padding:6px 14px;font-size:12px;font-weight:500;color:var(--blue3);margin-bottom:32px;font-family:var(--mono)}
.hero-badge-dot{width:6px;height:6px;border-radius:50%;background:var(--cyan);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.6;transform:scale(0.8)}}
h1{font-size:clamp(40px,5vw,64px);font-weight:700;letter-spacing:-0.03em;line-height:1.1;margin-bottom:24px}
h1 em{font-style:normal;background:linear-gradient(90deg,var(--blue3),var(--cyan));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.hero-sub{font-size:18px;color:var(--muted);max-width:580px;margin-bottom:17px;font-weight:400;line-height:1.7}
.hero-actions{display:flex;gap:16px;flex-wrap:wrap}
.hero-features{max-width:1100px;margin:6px auto 0;padding:0 48px;display:grid;grid-template-columns:repeat(3,1fr);gap:20px}
.hero-feature{border:1px solid var(--border);border-radius:12px;padding:22px 20px;background:var(--navy2)}
.hero-feature-icon{width:34px;height:34px;border-radius:8px;background:rgba(58,155,255,0.12);display:flex;align-items:center;justify-content:center;color:var(--blue4);margin-bottom:14px}
.hero-feature-icon svg{width:18px;height:18px}
.hero-feature h3{font-size:14.5px;font-weight:600;margin-bottom:6px}
.hero-feature p{font-size:13px;color:var(--muted);line-height:1.5}
@media (max-width:800px){.hero-features{grid-template-columns:1fr}}
.btn-primary{background:var(--blue3);color:var(--navy);padding:14px 28px;border-radius:8px;font-weight:600;font-size:15px;text-decoration:none;transition:all .2s;border:none;cursor:pointer}
.btn-primary:hover{background:var(--blue4);transform:translateY(-1px)}
.btn-ghost{background:transparent;color:var(--white);padding:14px 28px;border-radius:8px;font-weight:500;font-size:15px;text-decoration:none;border:1px solid var(--border2);transition:all .2s;cursor:pointer}
.btn-ghost:hover{border-color:var(--blue3);color:var(--blue3)}

/* PIPELINE SECTION */
.section{padding:80px 48px;max-width:1100px;margin:0 auto}
#demo{padding-bottom:40px}
#pipeline{padding-top:40px}
.section-label{font-size:12px;font-weight:600;color:var(--blue3);letter-spacing:0.1em;text-transform:uppercase;font-family:var(--mono);margin-bottom:12px}
.section-title{font-size:36px;font-weight:700;letter-spacing:-0.02em;margin-bottom:16px}
.section-sub{font-size:16px;color:var(--muted);max-width:520px;margin-bottom:56px}
#demo .section-sub{margin-bottom:39px}

/* PIPELINE DIAGRAM (SVG data-flow) */
.vf-flow{max-width:700px;margin:0;overflow-x:auto}
.vf-flow svg{display:block;width:100%;min-width:560px;height:auto}
.vf-eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:1px}
.vf-mono{font-family:var(--mono);font-size:11px}
.vf-title{font-family:var(--font);font-size:14px;font-weight:500;fill:var(--white)}
.vf-sub{font-family:var(--font);font-size:12px;fill:var(--muted)}
.vf-card{fill:var(--navy2);stroke:var(--border2);stroke-width:.5}
.vf-arrow{stroke:var(--blue2);stroke-width:1.5}
.vf-ai-card{fill:rgba(164,143,214,0.08);stroke:rgba(164,143,214,0.35);stroke-width:.5}
.vf-ai-title{font-family:var(--font);font-size:14px;font-weight:500;fill:#e7defa}
.vf-ai-sub{font-family:var(--font);font-size:12px;fill:#a48fd6}
.vf-ai-arrow{stroke:#8a72c4;stroke-width:1.5}
.vf-chip{fill:rgba(164,143,214,0.1);stroke:rgba(164,143,214,0.35);stroke-width:.5}
.vf-chip-text{fill:#c9b8ef}
.vf-audit-card{fill:rgba(245,166,35,0.06);stroke:rgba(245,166,35,0.3);stroke-width:.5;stroke-dasharray:4 3}
.vf-audit-body{font-family:var(--font);font-size:12px;fill:#d6b98a}
.vf-audit-arrow{stroke:#a37c2f;stroke-width:1.5}

/* DEMO SECTION */
.demo-wrap{background:var(--navy2);border:1px solid var(--border);border-radius:16px;overflow:hidden;margin-top:0}
.demo-body{padding:24px;font-family:var(--mono);font-size:12px;line-height:1.8;color:#a8c4e0}
.scenario-card{padding:24px 24px 0}
.scenario-label{display:flex;align-items:center;gap:7px;margin-bottom:12px}
.scenario-label span{font-size:11px;font-weight:500;letter-spacing:0.06em;color:var(--blue3);text-transform:uppercase;font-family:var(--mono)}
.scenario-text{color:var(--white);font-size:14px;line-height:1.65;margin:0 0 14px;max-width:640px;opacity:0.9}
.scenario-tags{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap}
.scenario-tag{border-radius:6px;padding:6px 10px;font-size:11.5px}
.scenario-note{display:flex;gap:8px;align-items:flex-start;background:var(--navy3);border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:20px;max-width:640px}
.scenario-note p{margin:0;font-size:12px;line-height:1.6;color:var(--muted)}
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
.mono-ai-ok{color:#c4a6f5}
.stage{display:flex;align-items:center;gap:12px;margin:4px 0}
.stage-badge{font-size:10px;padding:2px 8px;border-radius:4px;font-weight:600;min-width:100px;text-align:center;letter-spacing:0.04em}
.badge-pass{background:rgba(0,232,122,0.1);color:var(--success);border:1px solid rgba(0,232,122,0.2)}
.badge-fail{background:rgba(255,77,106,0.1);color:var(--danger);border:1px solid rgba(255,77,106,0.2)}
.badge-skip{background:rgba(120,120,120,0.1);color:#666;border:1px solid rgba(120,120,120,0.15)}
.badge-ai-pass{background:rgba(124,95,196,0.15);color:#c4a6f5;border:1px solid rgba(124,95,196,0.35)}
.run-trace{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;border-top:1px solid var(--border);padding-top:10px;margin-top:8px;font-size:11px;color:var(--muted);font-family:var(--mono)}
.run-trace-id{display:inline-flex;align-items:center;gap:6px}
.copy-btn{background:none;border:none;padding:2px;margin:0;cursor:pointer;color:var(--muted);display:inline-flex;align-items:center;position:relative}
.copy-btn:hover{color:var(--blue3)}
.copy-tooltip{position:absolute;bottom:calc(100% + 6px);left:50%;transform:translateX(-50%);background:var(--navy3);border:1px solid var(--border2);color:var(--white);font-size:10px;padding:3px 7px;border-radius:4px;white-space:nowrap;pointer-events:none}
.ai-edge-divider{display:flex;align-items:center;gap:10px;margin:16px 0}
.ai-edge-divider .line{flex:1;height:1px;background:var(--border)}
.ai-edge-divider span{font-size:11px;font-weight:700;letter-spacing:0.08em;color:#a48fd6;font-family:var(--mono);text-transform:uppercase;white-space:nowrap}

/* DEMO LOADING STATES */
@keyframes spin{to{transform:rotate(360deg)}}
.spinner{width:14px;height:14px;border:2px solid var(--border2);border-top-color:var(--blue3);border-radius:50%;animation:spin .8s linear infinite;display:inline-block;vertical-align:middle;margin-right:6px}
@keyframes stagePulse{0%,100%{opacity:1}50%{opacity:0.4}}
.stage-active{animation:stagePulse 1s ease-in-out infinite}
.demo-run-btn{margin:12px 0 24px 24px;display:block;width:50%}

/* TRUST / AUDITABILITY */
.audit-grid{display:grid;grid-template-columns:1fr;gap:24px;margin-top:0;max-width:800px}
.audit-card{background:var(--navy2);border:1px solid var(--border);border-radius:12px;padding:24px}
.audit-title{font-size:13px;font-weight:600;color:var(--muted);margin-bottom:16px;font-family:var(--mono);letter-spacing:0.06em}
.event-row{display:flex;align-items:center;gap:12px;padding:8px 0;border-bottom:1px solid rgba(58,155,255,0.07);font-size:12px;font-family:var(--mono)}
.event-row:last-child{border:none}
.event-ts{color:#3a5a80;min-width:100px;flex-shrink:0}
.event-stage{min-width:160px;padding:2px 8px;border-radius:4px;text-align:center;font-size:10px;font-weight:600;flex-shrink:0;white-space:nowrap}
.es-parse{background:rgba(0,212,255,0.08);color:var(--cyan)}
.es-valid{background:rgba(0,232,122,0.08);color:var(--success)}
.es-norm{background:rgba(58,155,255,0.1);color:var(--blue3)}
.es-fhir{background:rgba(126,196,255,0.1);color:var(--blue4)}
.event-msg{color:var(--muted)}

/* FOOTER SECTION */
footer{border-top:1px solid var(--border);padding:24px 48px;font-size:13px;color:var(--muted);display:flex;flex-direction:column;align-items:center;gap:16px;text-align:center;background:rgba(5,14,26,0.92);backdrop-filter:blur(12px)}
.footer-title{font-weight:700;font-size:20px;color:var(--white);letter-spacing:-0.02em}
.footer-links{display:flex;gap:24px}
.footer-link{background:none;border:none;padding:0;font:inherit;color:var(--muted);text-decoration:none;cursor:pointer;letter-spacing:0.04em;text-transform:uppercase;font-size:12px;transition:color .2s}
.footer-link:hover{color:var(--blue3)}

/* CONTACT MODAL */
.contact-overlay{position:fixed;inset:0;background:rgba(3,8,16,0.7);backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center;z-index:100;padding:24px}
.contact-modal{background:var(--navy2);border:1px solid var(--border2);border-radius:16px;padding:32px;max-width:440px;width:100%}
.contact-modal h3{font-size:20px;font-weight:700;margin-bottom:8px}
.contact-honeypot{position:absolute;left:-9999px;width:1px;height:1px;opacity:0;pointer-events:none}
.contact-sub{color:var(--muted);font-size:14px;margin-bottom:20px}
.contact-textarea{width:100%;min-height:140px;background:var(--navy);border:1px solid var(--border);border-radius:8px;padding:14px;color:var(--white);font-family:inherit;font-size:14px;resize:vertical}
.contact-textarea:focus{outline:none;border-color:var(--blue3)}
.contact-error{color:var(--danger);font-size:13px;margin-top:10px}
.contact-actions{display:flex;justify-content:flex-end;gap:12px;margin-top:20px}
.contact-actions .draft-btn{padding:8px 18px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;background:transparent;border:1px solid var(--border2);color:var(--blue3);font-family:var(--mono);transition:all .15s}
.contact-actions .draft-btn:hover:not(:disabled){border-color:var(--blue3);color:var(--white)}
.contact-actions .draft-btn.secondary{color:var(--muted);border-color:var(--border)}
.contact-actions .draft-btn.secondary:hover:not(:disabled){color:var(--blue3);border-color:var(--blue3)}
.contact-actions .draft-btn.primary{background:var(--blue3);border-color:var(--blue3);color:#04121f}
.contact-actions .draft-btn.primary:hover:not(:disabled){background:var(--white);border-color:var(--white)}
.contact-actions .draft-btn:disabled{opacity:0.45;cursor:not-allowed}

/* Result Grid */
.result-grid{display:grid;grid-template-columns:1fr 1fr;gap:24px}

/* Grid BG */
body::before{content:'';position:fixed;inset:0;background-image:linear-gradient(var(--border) 1px,transparent 1px),linear-gradient(90deg,var(--border) 1px,transparent 1px);background-size:60px 60px;opacity:0.5;pointer-events:none;z-index:0}
body>*{position:relative;z-index:1}

/* Mobile Responsive (max-width: 640px) */
@media (max-width:640px){
  nav{flex-wrap:wrap;padding:12px 16px;gap:20px}
  .nav-logo{order:1}
  .nav-cta{order:2}
  .nav-links{order:3;width:100%;justify-content:center;gap:16px}

  .stats{display:none}

  .section{padding:32px 16px}

  .vf-flow{overflow-x:auto}

  .audit-grid{max-width:100%;padding-right:8px}
  .event-row{align-items:flex-start;flex-wrap:wrap}
  .event-msg{flex:1;min-width:150px}

  .footer-logo{font-size:13px}

  .result-grid{grid-template-columns:1fr}
}

/* ===== AI AUGMENTATION LAYER (scoped under .ai-aug) ===== */
.ai-aug{margin-top:24px;border-top:1px solid var(--border);padding-top:20px}
.ai-aug .ai-tabs{display:flex;border-bottom:1px solid var(--border);margin-bottom:20px}
.ai-aug .ai-tab{padding:10px 20px 11px;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .2s;font-family:var(--mono);text-align:left}
.ai-aug .ai-tab.active{border-bottom-color:var(--blue3)}
.ai-aug .ai-tab-label{font-size:13.5px;font-weight:500;color:var(--muted)}
.ai-aug .ai-tab.active .ai-tab-label{color:var(--blue3)}
.ai-aug .ai-tab-sub{font-size:11px;margin-top:2px;color:#5c6472}
.ai-aug .section-label{font-size:10px;font-weight:600;color:var(--blue3);letter-spacing:0.1em;text-transform:uppercase;font-family:var(--mono);margin-bottom:10px}
.ai-aug .meta-row{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--border);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-bottom:20px}
.ai-aug .meta-cell{background:var(--navy2);padding:12px 16px;min-width:0}
.ai-aug .meta-label{font-size:10px;color:var(--muted);font-family:var(--mono);letter-spacing:0.06em;margin-bottom:4px}
.ai-aug .meta-val{font-size:13px;font-weight:600;color:var(--white);font-family:var(--mono);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ai-aug .review-banner{display:flex;align-items:center;gap:10px;padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:13px;font-weight:600}
.ai-aug .review-banner.urgent{background:rgba(255,77,106,0.08);border:1px solid rgba(255,77,106,0.3);color:var(--danger)}
.ai-aug .review-banner.routine{background:rgba(245,166,35,0.08);border:1px solid rgba(245,166,35,0.3);color:var(--warn)}
.ai-aug .summary-card{background:var(--navy2);border:1px solid var(--border2);border-radius:12px;padding:20px 22px;margin-bottom:20px}
.ai-aug .summary-header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:14px}
.ai-aug .summary-type{display:inline-flex;align-items:center;gap:6px;background:rgba(245,166,35,0.1);border:1px solid rgba(245,166,35,0.3);border-radius:6px;padding:4px 12px;font-size:11px;font-weight:700;color:var(--warn);font-family:var(--mono);letter-spacing:0.05em}
.ai-aug .conf-wrap{display:flex;align-items:center;gap:10px;flex-shrink:0}
.ai-aug .conf-label{font-size:11px;color:var(--muted);font-family:var(--mono)}
.ai-aug .conf-bar{width:80px;height:6px;background:rgba(58,155,255,0.15);border-radius:3px;overflow:hidden}
.ai-aug .conf-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,var(--blue3),var(--cyan))}
.ai-aug .conf-num{font-size:12px;font-weight:600;color:var(--blue3);font-family:var(--mono);min-width:28px}
.ai-aug .summary-text{font-size:14px;color:#c8dff5;line-height:1.7;margin-bottom:12px}
.ai-aug .summary-footer{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.ai-aug .requires-review-tag{background:rgba(255,77,106,0.1);border:1px solid rgba(255,77,106,0.25);color:var(--danger);border-radius:5px;padding:3px 10px;font-size:11px;font-weight:600;font-family:var(--mono)}
.ai-aug .non-auth-tag{background:rgba(122,153,196,0.1);border:1px solid rgba(122,153,196,0.2);color:var(--muted);border-radius:5px;padding:3px 10px;font-size:11px;font-family:var(--mono)}
.ai-aug .priority-grid{display:flex;flex-direction:column;gap:10px;margin-bottom:20px}
.ai-aug .finding-card{background:var(--navy2);border:1px solid var(--border);border-radius:10px;overflow:hidden;transition:border-color .2s}
.ai-aug .finding-card:hover{border-color:var(--border2)}
.ai-aug .finding-card.critical{border-left:3px solid var(--danger)}
.ai-aug .finding-card.high{border-left:3px solid var(--warn)}
.ai-aug .finding-card.elevated{border-left:3px solid #7ec4ff}
.ai-aug .finding-card.improved{border-left:3px solid var(--success)}
.ai-aug .finding-header{display:flex;align-items:center;gap:12px;padding:14px 18px;cursor:pointer;user-select:none}
.ai-aug .flag-pill{display:inline-flex;padding:3px 10px;border-radius:5px;font-size:10px;font-weight:700;font-family:var(--mono);letter-spacing:0.05em;white-space:nowrap}
.ai-aug .flag-critical_high,.ai-aug .flag-critical_low{background:rgba(255,77,106,0.12);color:var(--danger);border:1px solid rgba(255,77,106,0.3)}
.ai-aug .flag-high,.ai-aug .flag-low{background:rgba(245,166,35,0.1);color:var(--warn);border:1px solid rgba(245,166,35,0.3)}
.ai-aug .flag-normal{background:rgba(0,232,122,0.08);color:var(--success);border:1px solid rgba(0,232,122,0.2)}
.ai-aug .flag-unknown{background:rgba(122,153,196,0.1);color:var(--muted);border:1px solid rgba(122,153,196,0.2)}
.ai-aug .analyte-name{font-size:14px;font-weight:600;color:var(--white);flex:1}
.ai-aug .analyte-result{font-family:var(--mono);font-size:13px;font-weight:600;white-space:nowrap}
.ai-aug .val-critical_high,.ai-aug .val-critical_low{color:var(--danger)}
.ai-aug .val-high,.ai-aug .val-low{color:var(--warn)}
.ai-aug .val-normal{color:var(--success)}
.ai-aug .val-unknown{color:var(--white)}
.ai-aug .val-improved{color:var(--success)}
.ai-aug .finding-chevron{color:var(--muted);font-size:12px;transition:transform .2s;margin-left:4px}
.ai-aug .finding-chevron.open{transform:rotate(180deg)}
.ai-aug .finding-body{padding:0 18px 14px;border-top:1px solid var(--border)}
.ai-aug .finding-desc{font-size:13px;color:#a8c4e0;line-height:1.7;margin:12px 0 10px}
.ai-aug .finding-meta{display:flex;gap:12px;flex-wrap:wrap;margin-top:10px;padding-top:10px;border-top:1px solid var(--border)}
.ai-aug .finding-meta-item{font-size:11px;color:var(--muted);font-family:var(--mono)}
.ai-aug .finding-meta-item span{color:var(--blue4)}
.ai-aug .hist-btn{margin-top:12px;background:rgba(58,155,255,0.08);border:1px solid var(--border2);color:var(--blue3);padding:5px 12px;border-radius:5px;font-size:11px;font-weight:600;cursor:pointer;font-family:var(--mono)}
.ai-aug .hist-table{margin-top:12px;border:1px solid var(--border);border-radius:8px;overflow:hidden;max-width:320px}
.ai-aug .hist-row{display:grid;grid-template-columns:1fr 1fr;padding:7px 14px;border-bottom:1px solid rgba(58,155,255,0.07);font-size:12px;font-family:var(--mono)}
.ai-aug .hist-row:last-child{border:none}
.ai-aug .hist-row.head{background:rgba(58,155,255,0.05);font-size:10px;letter-spacing:0.06em}
.ai-aug .hist-date{color:var(--blue4)}
.ai-aug .hist-val{color:#c8e6ff;text-align:right}
.ai-aug .hist-empty{margin-top:10px;font-size:12px;color:var(--muted);font-family:var(--mono)}
.ai-aug .hist-chart{position:relative;margin-top:12px;max-width:320px}
.ai-aug .hist-chart-svg{display:block}
.ai-aug .hist-chart-line{stroke:var(--blue3);stroke-width:2}
.ai-aug .hist-chart-axis{stroke:var(--border2);stroke-width:1}
.ai-aug .hist-chart-refline{stroke:var(--muted);stroke-width:1.25;stroke-dasharray:4 3}
.ai-aug .hist-chart-refline-label{fill:var(--muted);font-size:9px;font-family:var(--mono)}
.ai-aug .hist-chart-axis-label{fill:var(--muted);font-size:9px;font-family:var(--mono)}
.ai-aug .hist-chart-hit{fill:transparent;cursor:pointer}
.ai-aug .hist-chart-tooltip{position:absolute;transform:translate(-50%,-115%);background:var(--navy2);border:1px solid var(--border2);border-radius:8px;padding:8px 10px;font-family:var(--mono);font-size:11px;white-space:nowrap;pointer-events:none;box-shadow:0 4px 14px rgba(0,0,0,0.35);z-index:5}
.ai-aug .hist-chart-tooltip-val{font-weight:700;color:var(--white)}
.ai-aug .hist-chart-tooltip-date{color:var(--muted);margin-top:2px}
.ai-aug .hist-chart-tooltip-flag{margin-top:2px;font-weight:600;letter-spacing:0.04em}
.ai-aug .other-table-wrap{background:var(--navy2);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-bottom:20px}
.ai-aug .other-row{display:grid;grid-template-columns:1fr 100px 100px 90px;align-items:center;padding:10px 18px;border-bottom:1px solid rgba(58,155,255,0.07);font-size:13px;gap:8px}
.ai-aug .other-row:last-child{border:none}
.ai-aug .other-row.head{background:rgba(58,155,255,0.05);font-size:10px;font-weight:600;color:var(--muted);letter-spacing:0.07em;font-family:var(--mono);padding:8px 18px}
.ai-aug .other-analyte{font-weight:500;color:var(--white)}
.ai-aug .other-val{font-family:var(--mono);font-size:12px;font-weight:600;text-align:right}
.ai-aug .other-ref{font-family:var(--mono);font-size:11px;color:var(--muted);text-align:right}
.ai-aug .other-flag{text-align:right}
.ai-aug .collapse-head{display:flex;align-items:center;justify-content:space-between;cursor:pointer;user-select:none;padding:12px 16px;background:var(--navy2);border:1px solid var(--border);border-radius:10px;margin-bottom:12px}
.ai-aug .collapse-head:hover{border-color:var(--border2)}
.ai-aug .audit-footer{padding:12px 16px;background:rgba(10,22,40,0.6);border:1px solid var(--border);border-radius:8px;margin-top:20px}
.ai-aug .audit-trace-title{font-size:11px;color:var(--muted);font-family:var(--mono);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:10px}
.ai-aug .audit-trace-row{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}
.ai-aug .audit-item{font-size:11px;color:var(--muted);font-family:var(--mono)}
.ai-aug .audit-item span{color:var(--blue4)}
.ai-aug .disclaimer{font-size:11px;color:#4a6585;font-style:italic;text-align:center;margin-top:14px;line-height:1.6}
.ai-aug .composer-wrap{background:var(--navy2);border:1px solid var(--border2);border-radius:14px;overflow:hidden}
.ai-aug .composer-top{padding:18px 22px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}
.ai-aug .composer-title-row{display:flex;align-items:center;gap:10px}
.ai-aug .composer-icon{width:32px;height:32px;background:rgba(58,155,255,0.1);border:1px solid var(--border2);border-radius:8px;display:flex;align-items:center;justify-content:center;color:var(--blue3);font-size:16px;flex-shrink:0}
.ai-aug .composer-title{font-size:14px;font-weight:600;color:var(--white)}
.ai-aug .composer-subtitle{font-size:12px;color:var(--muted);margin-top:1px;font-family:var(--mono)}
.ai-aug .draft-tag{display:inline-flex;align-items:center;gap:5px;background:rgba(245,166,35,0.1);border:1px solid rgba(245,166,35,0.3);border-radius:5px;padding:3px 10px;font-size:11px;font-weight:600;color:var(--warn);font-family:var(--mono)}
.ai-aug .draft-tag.status-rejected{background:rgba(255,77,106,0.1);border-color:rgba(255,77,106,0.3);color:var(--danger)}
.ai-aug .draft-tag.status-approved{background:rgba(0,232,122,0.1);border-color:rgba(0,232,122,0.3);color:var(--success)}
.ai-aug .draft-tag.status-pending{background:rgba(58,155,255,0.1);border-color:rgba(58,155,255,0.3);color:var(--blue3)}
.ai-aug .draft-dot{width:5px;height:5px;border-radius:50%;background:var(--warn)}
.ai-aug .draft-tag.status-rejected .draft-dot{background:var(--danger)}
.ai-aug .draft-tag.status-approved .draft-dot{background:var(--success)}
.ai-aug .draft-tag.status-pending .draft-dot{background:var(--blue3);animation:stagePulse 1s ease-in-out infinite}
.ai-aug .meta-fields{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:var(--border);border-bottom:1px solid var(--border)}
.ai-aug .meta-field{background:var(--navy2);padding:11px 22px;display:flex;align-items:center;gap:10px}
.ai-aug .meta-field-full{grid-column:1 / -1}
.ai-aug .meta-field-icon{color:var(--blue3);display:flex;align-items:center;flex-shrink:0}
.ai-aug .meta-field-val{font-size:13px;color:var(--white);font-family:var(--mono)}
.ai-aug .message-area{padding:20px 22px;border-bottom:1px solid var(--border)}
.ai-aug .msg-toolbar{display:flex;align-items:center;gap:6px;margin-bottom:10px;flex-wrap:wrap}
.ai-aug .tb-btn{padding:4px 10px;border-radius:5px;font-size:11px;font-weight:500;cursor:pointer;background:transparent;border:1px solid var(--border);color:var(--muted);font-family:var(--mono);transition:all .15s}
.ai-aug .tb-btn:hover{border-color:var(--blue3);color:var(--blue3)}
.ai-aug .tb-sep{width:1px;height:16px;background:var(--border);margin:0 4px}
.ai-aug .msg-textarea{width:100%;background:rgba(5,14,26,0.6);border:1px solid var(--border2);border-radius:8px;padding:16px;font-size:14px;color:var(--white);font-family:var(--font);line-height:1.75;resize:vertical;min-height:510px;outline:none;transition:border-color .2s}
.ai-aug .msg-textarea:focus{border-color:var(--blue3)}
.ai-aug .draft-actions{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:12px 22px;border-top:1px solid var(--border)}
.ai-aug .draft-actions-right{display:flex;align-items:center;gap:10px}
.ai-aug .draft-btn{padding:8px 18px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;background:transparent;border:1px solid var(--border2);color:var(--blue3);font-family:var(--mono);transition:all .15s}
.ai-aug .draft-btn:hover{border-color:var(--blue3);color:var(--white)}
.ai-aug .draft-btn.secondary{color:var(--muted);border-color:var(--border)}
.ai-aug .draft-btn.secondary:hover{color:var(--blue3);border-color:var(--blue3)}
.ai-aug .draft-btn.primary{background:var(--blue3);border-color:var(--blue3);color:#04121f}
.ai-aug .draft-btn.primary:hover:not(:disabled){background:var(--white);border-color:var(--white)}
.ai-aug .draft-btn:disabled{opacity:0.45;cursor:not-allowed}
.ai-aug .draft-btn.danger{color:var(--danger);border-color:rgba(255,77,106,0.3)}
.ai-aug .draft-btn.danger:hover:not(:disabled){background:var(--danger);border-color:var(--danger);color:#04121f}
.ai-aug .reject-panel{display:flex;flex-direction:column;gap:10px;padding:14px 22px;border-top:1px solid var(--border);background:rgba(255,77,106,0.05)}
.ai-aug .reject-label{font-size:11px;font-weight:600;color:var(--danger);font-family:var(--mono);text-transform:uppercase;letter-spacing:.04em}
.ai-aug .reject-textarea{width:100%;background:rgba(5,14,26,0.6);border:1px solid rgba(255,77,106,0.3);border-radius:8px;padding:12px;font-size:13px;color:var(--white);font-family:var(--font);line-height:1.6;resize:vertical;min-height:80px;outline:none;transition:border-color .2s}
.ai-aug .reject-textarea:focus{border-color:var(--danger)}
.ai-aug .reject-panel-actions{display:flex;align-items:center;justify-content:flex-end;gap:10px}
.ai-aug .send-notice{font-size:11px;color:var(--warn);font-family:var(--mono);text-align:right;padding:0 22px 12px}
.ai-aug .rejected-notice{font-size:12px;font-weight:600;color:var(--danger);font-family:var(--mono);text-align:center;padding:10px 22px;background:rgba(255,77,106,0.06);border-top:1px solid rgba(255,77,106,0.2)}
.ai-aug .physician-note{display:flex;align-items:flex-start;gap:10px;padding:12px 22px;background:rgba(0,0,0,0.15);border-top:1px solid var(--border)}
.ai-aug .pn-text{font-size:12px;color:var(--muted);line-height:1.6;font-style:italic}
.ai-aug .pn-text strong{color:var(--blue4);font-style:normal;font-weight:600}
@media (max-width:640px){
  .ai-aug .meta-row{grid-template-columns:1fr 1fr}
  .ai-aug .other-row{grid-template-columns:1fr 70px 60px}
  .ai-aug .other-ref{display:none}
  .ai-aug .meta-fields{grid-template-columns:1fr}
}`;
