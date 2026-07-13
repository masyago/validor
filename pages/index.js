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
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--border);border:1px solid var(--border);border-radius:12px;overflow:hidden;max-width:1100px;margin:0 auto 80px;padding:0 48px}
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
.demo-tab:first-child{border-right:1px solid var(--border)}
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
.cta-band{background:linear-gradient(135deg,var(--navy2),var(--navy3));border:1px solid var(--border);border-radius:20px;padding:64px;text-align:center;margin:80px 48px}
.cta-band h2{font-size:36px;font-weight:700;letter-spacing:-0.02em;margin-bottom:16px}
.cta-band p{font-size:16px;color:var(--muted);max-width:440px;margin:0 auto 40px}

footer{border-top:1px solid var(--border);padding:32px 48px;display:grid;grid-template-columns:1fr 1fr;gap:24px;font-size:13px;color:var(--muted)}
.footer-logo{font-weight:700;font-size:15px;color:var(--white);grid-column:1/-1;text-align:left;margin-bottom:16px}
footer > span:nth-child(3){text-align:right}

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

  .pipeline{flex-direction:column;align-items:stretch;overflow-x:unset;gap:2px}
  .pipe-arrow{transform:rotate(90deg);text-align:center;margin-top:0;margin-bottom:-4px;padding:0;font-size:16px}
  .pipe-step{width:100%}
  .pipe-box{width:100%;box-sizing:border-box}

  .audit-grid{max-width:100%;padding-right:8px}
  .event-row{align-items:flex-start;flex-wrap:wrap}
  .event-msg{flex:1;min-width:150px}

  footer{gap:16px;align-items:center}
  .footer-logo{grid-column:1;grid-row:1;margin-bottom:0;font-size:13px}
  footer > span:nth-child(2){grid-column:1/-1;grid-row:2;text-align:center}
  footer > span:nth-child(3){grid-column:2;grid-row:1;text-align:right}

  .result-grid{grid-template-columns:1fr}
}

/* ===== AI AUGMENTATION LAYER (scoped under .ai-aug) ===== */
.ai-aug{margin-top:24px;border-top:1px solid var(--border);padding-top:20px}
.ai-aug .ai-tabs{display:flex;border-bottom:1px solid var(--border);margin-bottom:20px}
.ai-aug .ai-tab{padding:12px 20px;font-size:13px;font-weight:500;cursor:pointer;color:var(--muted);border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .2s;font-family:var(--mono)}
.ai-aug .ai-tab.active{color:var(--blue3);border-bottom-color:var(--blue3)}
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
.ai-aug .audit-footer{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;padding:12px 16px;background:rgba(10,22,40,0.6);border:1px solid var(--border);border-radius:8px;margin-top:4px}
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
.ai-aug .msg-textarea{width:100%;background:rgba(5,14,26,0.6);border:1px solid var(--border2);border-radius:8px;padding:16px;font-size:14px;color:var(--white);font-family:var(--font);line-height:1.75;resize:vertical;min-height:340px;outline:none;transition:border-color .2s}
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

const STAGE_MAP = [
  { label: "PARSE", ok: "PARSE_SUCCEEDED", fail: "PARSE_FAILED" },
  { label: "VALIDATE", ok: "VALIDATION_SUCCEEDED", fail: "VALIDATION_FAILED" },
  { label: "NORMALIZE", ok: "NORMALIZATION_SUCCEEDED", fail: "NORMALIZATION_FAILED" },
  { label: "PERSIST", ok: "FHIR_JSON_GENERATION_SUCCEEDED", fail: "FHIR_JSON_GENERATION_FAILED" },
  { label: "AI ANNOTATION", ok: "AI_ENRICHMENT_SUCCEEDED", fail: "AI_ENRICHMENT_FAILED" },
  { label: "MESSAGE DRAFT", ok: "MESSAGE_DRAFT_SUCCEEDED", fail: "MESSAGE_DRAFT_FAILED" },
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

function flagPointColor(flag) {
  if (flag === "critical_high" || flag === "critical_low") return "var(--danger)";
  if (flag === "high" || flag === "low") return "var(--warn)";
  if (flag === "normal") return "var(--success)";
  return "var(--muted)";
}

function fmtChartDate(iso) {
  const d = new Date(iso);
  return isNaN(d.getTime()) ? "" : d.toISOString().slice(0, 10);
}

function fmtAxisNum(v) {
  if (Math.abs(v) >= 100) return String(Math.round(v));
  if (Math.abs(v) >= 10) return String(Math.round(v * 10) / 10);
  return String(Math.round(v * 100) / 100);
}

// Renders a single-analyte trend: value over time with dashed low/high
// reference lines. Kept as a top-level component (not nested in Home) so its
// hover state isn't torn down on every parent re-render.
function HistoryChart({ series, unit, refLow, refHigh }) {
  const [hoverIdx, setHoverIdx] = useState(null);
  if (!series || series.length === 0) return null;

  const width = 300;
  const height = 132;
  const padLeft = 34;
  const padRight = 46;
  const padTop = 14;
  const padBottom = 26;
  const plotW = width - padLeft - padRight;
  const plotH = height - padTop - padBottom;
  const plotBottom = padTop + plotH;

  const values = series.map((s) => s.value_num);
  const refVals = [refLow, refHigh].filter((v) => typeof v === "number");
  const allVals = values.concat(refVals);
  let yMin = Math.min(...allVals);
  let yMax = Math.max(...allVals);
  if (yMin === yMax) {
    yMin -= 1;
    yMax += 1;
  }
  // Extra headroom (vs. a plain 5-10%) so a value sitting below the low
  // reference line still has visible separation from the axis and doesn't
  // read as clipped/crowded against the ref line or the plot edge.
  const pad = (yMax - yMin) * 0.18;
  yMin -= pad;
  yMax += pad;

  const xAt = (i) =>
    series.length === 1 ? padLeft + plotW / 2 : padLeft + (i / (series.length - 1)) * plotW;
  const yAt = (v) => padTop + (1 - (v - yMin) / (yMax - yMin)) * plotH;

  const points = series.map((s, i) => ({ ...s, x: xAt(i), y: yAt(s.value_num) }));
  const linePath = points.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
  const hovered = hoverIdx != null ? points[hoverIdx] : null;

  // Three y-axis labels: the axis/x-axis intersection (bottom), a midpoint,
  // and one near the top (nudged down slightly so its text isn't clipped).
  const yTicks = [
    { value: yMin, y: plotBottom },
    { value: (yMin + yMax) / 2, y: (padTop + plotBottom) / 2 },
    { value: yMax, y: padTop + 3 },
  ];

  return (
    <div className="hist-chart">
      <svg width={width} height={height} className="hist-chart-svg">
        <line x1={padLeft} x2={padLeft} y1={padTop} y2={plotBottom} className="hist-chart-axis" />
        <line x1={padLeft} x2={width - padRight} y1={plotBottom} y2={plotBottom} className="hist-chart-axis" />
        {yTicks.map((t, i) => (
          <g key={i}>
            <line x1={padLeft - 4} x2={padLeft} y1={t.y} y2={t.y} className="hist-chart-axis" />
            <text x={padLeft - 7} y={t.y} textAnchor="end" dominantBaseline="middle" className="hist-chart-axis-label">
              {fmtAxisNum(t.value)}
            </text>
          </g>
        ))}
        {refLow != null && (
          <>
            <line x1={padLeft} x2={width - padRight} y1={yAt(refLow)} y2={yAt(refLow)} className="hist-chart-refline" />
            <text x={width - padRight + 4} y={yAt(refLow)} dominantBaseline="middle" className="hist-chart-refline-label">
              {refLow}
            </text>
          </>
        )}
        {refHigh != null && (
          <>
            <line x1={padLeft} x2={width - padRight} y1={yAt(refHigh)} y2={yAt(refHigh)} className="hist-chart-refline" />
            <text x={width - padRight + 4} y={yAt(refHigh)} dominantBaseline="middle" className="hist-chart-refline-label">
              {refHigh}
            </text>
          </>
        )}
        {points.length > 1 && <path d={linePath} className="hist-chart-line" fill="none" />}
        {points.map((p, i) => (
          <g key={i} onMouseEnter={() => setHoverIdx(i)} onMouseLeave={() => setHoverIdx(null)}>
            <circle cx={p.x} cy={p.y} r={10} className="hist-chart-hit" />
            <circle cx={p.x} cy={p.y} r={p.isCurrent ? 5 : 3.5} style={{ fill: flagPointColor(p.flag) }} />
          </g>
        ))}
        <text x={padLeft} y={height - 4} className="hist-chart-axis-label">
          {fmtChartDate(points[0].effective_at)}
        </text>
        {points.length > 1 && (
          <text x={width - padRight} y={height - 4} textAnchor="end" className="hist-chart-axis-label">
            {fmtChartDate(points[points.length - 1].effective_at)}
          </text>
        )}
      </svg>
      {hovered && (
        <div
          className="hist-chart-tooltip"
          style={{ left: `${(hovered.x / width) * 100}%`, top: `${(hovered.y / height) * 100}%` }}
        >
          <div className="hist-chart-tooltip-val">
            {hovered.value_num}
            {unit ? ` ${unit}` : ""}
          </div>
          <div className="hist-chart-tooltip-date">{fmtChartDate(hovered.effective_at)}</div>
          {hovered.flag && hovered.flag !== "unknown" && (
            <div className="hist-chart-tooltip-flag" style={{ color: flagPointColor(hovered.flag) }}>
              {hovered.flag.toUpperCase().replace("_", " ")}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default function Home() {
  const [activeTab, setActiveTab] = useState("valid");
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
  const pollRef = useRef(null);
  const textareaRef = useRef(null);
  const aiReadyCheckedRef = useRef(false);

  useEffect(() => () => clearInterval(pollRef.current), []);

  // Establish/reuse the session cookie on load and purge any ingestion left
  // over under it (covers a reopened tab without waiting for the TTL sweep).
  useEffect(() => {
    fetch("/v1/session/start", { method: "POST", credentials: "include" }).catch(() => {});
  }, []);

  function handleTabChange(tab) {
    if (tab === activeTab) return;
    clearInterval(pollRef.current);
    setActiveTab(tab);
    setDemoPhase("idle");
    setDemoResult(null);
    setDemoError(null);
    setLoadingStage(0);
    setShowReports(false);
    setShowObservations(false);
    setShowAudit(false);
    setShowErrorDetails(false);
    setAiTab("annotation");
    setOpenFindings({});
    setHistory({});
    setShowOther(false);
    setEmailBody("");
    setPatientMessage(null);
    setAiReady(false);
    aiReadyCheckedRef.current = false;
  }

  async function runDemo() {
    clearInterval(pollRef.current);
    setDemoPhase("loading");
    setDemoResult(null);
    setDemoError(null);
    setLoadingStage(0);
    setAiReady(false);
    aiReadyCheckedRef.current = false;

    const csvPath = activeTab === "valid" ? "/demo-csv/visit4_latest.csv" : "/demo-csv/invalid_missing_fields.csv";
    const instrumentId = activeTab === "valid" ? "demo-instrument-01" : "demo-instrument-02";
    const fileName = activeTab === "valid" ? "visit4_latest.csv" : "invalid_missing_fields.csv";

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

  function fmtObsValue(o) {
    if (!o) return "—";
    const base = o.value_num != null ? String(o.value_num) : (o.value_text || "");
    if (!base) return "—";
    return o.unit ? `${base} ${o.unit}` : base;
  }

  function fmtRef(o) {
    if (!o) return "—";
    const lo = o.ref_low_num;
    const hi = o.ref_high_num;
    if (lo == null && hi == null) return "—";
    return `${lo == null ? "?" : lo}–${hi == null ? "?" : hi}`;
  }

  function flagKey(o) {
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

  function flagLabel(o) {
    const k = flagKey(o);
    return k === "unknown" ? "—" : k.toUpperCase();
  }

  // True when the AI has flagged this finding as a result that was out of
  // range in its most recent prior result but is back within range now.
  function isImprovedFinding(f) {
    return (f?.trend_direction || "").toString().trim().toLowerCase() === "improved";
  }

  function cardPriorityClass(o, improved) {
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
  function flagRank(o, improved) {
    if (improved) return -1;
    const k = flagKey(o);
    if (k.startsWith("critical")) return 3;
    if (k === "high" || k === "low") return 2;
    if (k === "normal") return 0;
    return 1; // unknown / unmatched
  }

  // Combines the result flag with value + reference range, e.g. "HIGH: 93 mmol/L [50–80]".
  function fmtResultWithFlag(o, improved) {
    if (!o) return "REVIEW";
    if (improved) return `IMPROVED: ${fmtObsValue(o)} [${fmtRef(o)}]`;
    const k = flagKey(o);
    const prefix = k === "unknown" ? "" : `${k.toUpperCase().replace("_", " ")}: `;
    return `${prefix}${fmtObsValue(o)} [${fmtRef(o)}]`;
  }

  // Review banner urgency: routine → yellow, anything else → red.
  function reviewPriorityClass(priority) {
    return (priority || "").toString().trim().toLowerCase() === "routine"
      ? "routine"
      : "urgent";
  }

  // Formats a UTC timestamp in the viewer's local timezone, e.g. "2026-07-05 15:05:14 EDT".
  function fmtGenerated(iso) {
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
  function shortModelName(modelId) {
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

  function fmtDate(s) {
    const d = new Date(s);
    return isNaN(d.getTime()) ? (s || "") : d.toISOString().slice(0, 10);
  }

  // Patient identity is resolved by SQL from patient_id and injected here — it is never
  // shared with the AI. Hardcoded for the demo.
  const PATIENT_NAME = "Jane Doe";

  function labDateFromResult(result) {
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

  function patientMessageToText(pm, result = demoResult) {
    // Render the clinician-reviewable draft as a letter body. The recipient
    // name, the lab date, the clinic name and the signature are applied only
    // here at render time — never baked into the draft_content_json the LLM
    // produced. `result` is passed explicitly when called right after
    // ingestion, because the demoResult state has not applied yet at that
    // point. The "To:" and "Subject:" lines live in the composer header, not
    // in the body — the body starts with the salutation.
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

  function buildEmailTemplate(result) {
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
      const currentIds = new Set((demoResult?.observations || []).map((o) => o.observation_id));
      const rows = (Array.isArray(all) ? all : [])
        .filter((o) => o.code === code && !currentIds.has(o.observation_id))
        .sort((a, b) => (a.effective_at < b.effective_at ? 1 : -1))
        .map((o) => ({ ...o, date: fmtDate(o.effective_at), result: fmtObsValue(o) }));
      setHistory((p) => ({ ...p, [code]: { loading: false, shown: true, rows } }));
    } catch (e) {
      setHistory((p) => ({ ...p, [code]: { loading: false, shown: true, error: "Could not load history." } }));
    }
  }

  function resetDraft() {
    setEmailBody(
      patientMessage ? patientMessageToText(patientMessage) : buildEmailTemplate(demoResult)
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
    if (pm && pm.patient_message_id) {
      try {
        const res = await fetch(`/v1/patient_messages/${pm.patient_message_id}/reject`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            reviewed_by: "demo-clinician",
            note: rejectReason.trim() || null,
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

  async function copyDraft() {
    try {
      await navigator.clipboard.writeText(emailBody);
      setCopied(true);
      setTimeout(() => setCopied(false), 1800);
    } catch {
      // Clipboard API unavailable (e.g. non-secure context) — no-op.
    }
  }

  // Combines the current result with fetched history rows into an ascending,
  // numeric-only series for the trend chart (qualitative/text-only results
  // can't be plotted but still show in the history table).
  function buildChartSeries(current, rows) {
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

  function renderAiAugSection() {
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
    return (
      <div className="ai-aug">
        <div className="ai-tabs">
          <div className={`ai-tab${aiTab === "annotation" ? " active" : ""}`} onClick={() => setAiTab("annotation")}>AI annotation</div>
          <div className={`ai-tab${aiTab === "email" ? " active" : ""}`} onClick={() => setAiTab("email")}>Message draft</div>
        </div>

        {aiTab === "annotation" && (
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
                                        <div className="hist-row" key={i}><div className="hist-date">{r.date}</div><div className="hist-val">{r.result}</div></div>
                                      ))}
                                    </div>
                                  ) : (
                                    <div className="hist-empty">No prior results for this analyte.</div>
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
              <span className="audit-item">Model: <span title={ai.model_id || ""}>{shortModelName(ai.model_id)}</span></span>
              <span className="audit-item">Generated: <span>{fmtGenerated(ai.created_at)}</span></span>
              <span className="audit-item">Schema: <span>{ai.content_schema_version || "—"}</span></span>
            </div>
            <div className="disclaimer">
              AI annotations are non-authoritative and generated by an automated pipeline. They are not medical advice, do not constitute a clinical diagnosis, and must not be used as a substitute for clinician review. All findings require independent clinical interpretation.
            </div>
          </div>
        )}

        {aiTab === "email" && (
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
                  <div className="composer-subtitle">AI-generated · ingestion id: {(demoResult.ingestionId || "").slice(0, 18)}</div>
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
                  placeholder="Why is this draft being rejected? (recorded on the message audit trail)"
                  spellCheck={true}
                />
                <div className="reject-panel-actions">
                  <button className="draft-btn secondary" onClick={() => { setRejecting(false); setRejectReason(""); }}>Cancel</button>
                  <button className="draft-btn danger" onClick={rejectDraft}>Confirm rejection</button>
                </div>
              </div>
            )}
            {sentNotice && <div className="send-notice">This is a demo. The message wasn't actually delivered.</div>}
            {patientMessage?.review_status === "REJECTED" && (
              <div className="rejected-notice">The message was rejected and cannot be sent to patient.</div>
            )}
            <div className="audit-footer">
              <span className="audit-item">Model: <span title={(patientMessage?.model_id || ai.model_id) || ""}>{shortModelName(patientMessage?.model_id || ai.model_id)}</span></span>
              <span className="audit-item">Generated: <span>{fmtGenerated(patientMessage?.created_at || ai.created_at)}</span></span>
              <span className="audit-item">Schema: <span>{(patientMessage?.content_schema_version || ai.content_schema_version) || "—"}</span></span>
            </div>
            <div className="physician-note">
              <div className="pn-text">
                <strong>Physician review required.</strong> This draft was generated by an AI pipeline based on structured lab annotations and is provided as a starting point only. You must review, edit as needed, and personally approve before sending. This message does not constitute medical advice and must not be sent without clinical sign-off.
              </div>
            </div>
              </>
            )}
          </div>
        )}
      </div>
    );
  }

  return (
    <>
      <Head>
        <title>Validor</title>
        <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
        <link rel="shortcut icon" href="/favicon.svg" />
      </Head>
      <style dangerouslySetInnerHTML={{ __html: globalCss }} />

      <nav>
        <a href="#" onClick={(e) => { e.preventDefault(); window.scrollTo({ top: 0, behavior: 'smooth' }); }} className="nav-logo" style={{ textDecoration: 'none', cursor: 'pointer' }}>
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
        </a>
        <div className="nav-links">
          <a href="#pipeline">Pipeline</a>
          <a href="#features">Features</a>
          <a href="#demo">Demo</a>
          <a href="https://github.com/masyago/validor" target="_blank" rel="noopener noreferrer">Docs</a>
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

      </div>

      <div className="section" id="features">
        <div className="section-label">Key Properties</div>
        <div className="section-title">Built-in guarantees</div>
        <div className="section-sub">
          Six core properties that make Validor reliable for healthcare data workflows.
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
          validation. Every run emits a full, timestamped audit trail — expand it inline with the
          persisted resources below.
        </div>

        <div className="demo-wrap">
          <div className="demo-tabs">
            <div
              className={`demo-tab${activeTab === "valid" ? " active" : ""}`}
              onClick={() => handleTabChange("valid")}
            >
              ✓ Valid CSV
            </div>
            <div
              className={`demo-tab${activeTab === "invalid" ? " active" : ""}`}
              onClick={() => handleTabChange("invalid")}
            >
              ✗ Invalid CSV
            </div>
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
                <p>Select a scenario above, then click <strong style={{ color: "var(--white)" }}>Run Demo</strong> to call the live API.</p>
              </div>
            )}

            {demoPhase === "loading" && (
              <div style={{ padding: 24 }}>
                <div style={{ color: "var(--muted)", fontSize: 12, marginBottom: 16, fontFamily: "var(--mono)" }}>
                  <span className="spinner" /> Processing through pipeline...
                </div>
                {["Parse", "Validate", "Normalize", "Persist", "AI annotation", "Message draft"].map((name, i) => (
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

            {demoPhase === "loading" && aiReady && renderAiAugSection()}

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
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 16 }}>
                          <button
                            onClick={() => setShowReports(!showReports)}
                            style={{
                              background: showReports ? "rgba(0,212,255,0.15)" : "rgba(0,212,255,0.08)",
                              border: "1px solid rgba(0,212,255,0.2)",
                              color: "var(--cyan)",
                              padding: "4px 12px",
                              borderRadius: 4,
                              fontSize: 11,
                              fontWeight: 600,
                              cursor: "pointer",
                              transition: "background 0.2s",
                            }}
                          >
                            DiagnosticReports ×{demoResult.reports.length}
                          </button>
                          <button
                            onClick={() => setShowObservations(!showObservations)}
                            style={{
                              background: showObservations ? "rgba(0,212,255,0.15)" : "rgba(0,212,255,0.08)",
                              border: "1px solid rgba(0,212,255,0.2)",
                              color: "var(--cyan)",
                              padding: "4px 12px",
                              borderRadius: 4,
                              fontSize: 11,
                              fontWeight: 600,
                              cursor: "pointer",
                              transition: "background 0.2s",
                            }}
                          >
                            Observations ×{demoResult.observations.length}
                          </button>
                          <button
                            onClick={() => setShowAudit(!showAudit)}
                            style={{
                              background: showAudit ? "rgba(0,212,255,0.15)" : "rgba(0,212,255,0.08)",
                              border: "1px solid rgba(0,212,255,0.2)",
                              color: "var(--cyan)",
                              padding: "4px 12px",
                              borderRadius: 4,
                              fontSize: 11,
                              fontWeight: 600,
                              cursor: "pointer",
                              transition: "background 0.2s",
                            }}
                          >
                            Audit log ×{(demoResult.events || []).length}
                          </button>
                        </div>
                        {showReports && (
                          <div style={{ marginBottom: 16, maxHeight: 300, overflowY: "auto" }}>
                            <pre
                              style={{
                                background: "rgba(58,155,255,0.05)",
                                border: "1px solid var(--border)",
                                borderRadius: 6,
                                padding: 12,
                                fontSize: 10,
                                color: "var(--cyan)",
                                margin: 0,
                              }}
                            >
                              {JSON.stringify(demoResult.reports, null, 2)}
                            </pre>
                          </div>
                        )}
                        {showObservations && (
                          <div style={{ marginBottom: 16, maxHeight: 300, overflowY: "auto" }}>
                            <pre
                              style={{
                                background: "rgba(58,155,255,0.05)",
                                border: "1px solid var(--border)",
                                borderRadius: 6,
                                padding: 12,
                                fontSize: 10,
                                color: "var(--cyan)",
                                margin: 0,
                              }}
                            >
                              {JSON.stringify(demoResult.observations.slice(0, 3), null, 2)}
                              {demoResult.observations.length > 3 && (
                                <div style={{ marginTop: 8, color: "var(--muted)" }}>
                                  ... and {demoResult.observations.length - 3} more
                                </div>
                              )}
                            </pre>
                          </div>
                        )}
                        {showAudit && (
                          <div style={{ marginBottom: 16, maxHeight: 300, overflowY: "auto", background: "rgba(58,155,255,0.05)", border: "1px solid var(--border)", borderRadius: 6, padding: 12 }}>
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
                        <div
                          style={{
                            marginTop: 8,
                            color: "var(--muted)",
                            fontSize: 11,
                            marginBottom: 16,
                          }}
                        >
                          ingestion_id: <span style={{ color: "var(--blue3)" }}>{demoResult.ingestionId}</span>
                        </div>
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
                {aiReady && renderAiAugSection()}
              </>
            )}
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
          <a href="#demo" className="btn-primary">
            Open Live Demo →
          </a>
          <a href="https://github.com/masyago/validor" target="_blank" rel="noopener noreferrer" className="btn-ghost">
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
