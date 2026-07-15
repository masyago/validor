export default function PipelineDiagram() {
  return (
      <div className="section" id="pipeline">
        <div className="section-label">PIPELINE</div>
        <div className="section-title">From raw lab data to AI enrichment</div>
        <div className="section-sub">
          Every file travels one deterministic path, then it's handed to the 
          governed AI layer for triage and message drafting.
          No partial writes, no message sent without a clinician's approval.
        </div>

        <div className="vf-flow">
          <svg viewBox="0 0 680 400" role="img" aria-labelledby="vf-title vf-desc">
            <title id="vf-title">Validor data flow</title>
            <desc id="vf-desc">
              A deterministic pipeline parses, validates, normalizes and persists every lab result.
              De-identified data crosses into a governed AI layer, where retrieval of clinical
              guidelines and patient history feeds AI annotation and a patient message draft that
              needs clinician approval. An append-only, timestamped audit log records events from
              every stage of both lanes.
            </desc>

            <defs>
              <marker id="vf-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                <path d="M2 1L8 5L2 9" fill="none" stroke="context-stroke" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
              </marker>
              <marker id="vf-arrow-bold" viewBox="0 0 10 10" refX="7" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
                <path d="M1.5 1L8.5 5L1.5 9" fill="none" stroke="context-stroke" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
              </marker>
              <pattern id="vf-grid" width="40" height="40" patternUnits="userSpaceOnUse">
                <path d="M40 0V40M0 40H40" fill="none" stroke="var(--border)" strokeWidth=".5" />
              </pattern>
            </defs>

            <rect x="0" y="0" width="680" height="400" rx="12" fill="var(--navy)" />
            <rect x="0" y="0" width="680" height="400" rx="12" fill="url(#vf-grid)" />

            <text className="vf-eyebrow" x="40" y="56" fill="var(--blue3)">DETERMINISTIC PIPELINE: FROM RAW CSV</text>

            <rect className="vf-card" x="40" y="72" width="94" height="56" rx="10" />
            <text className="vf-title" x="87" y="94" textAnchor="middle" dominantBaseline="central">Parse</text>
            <text className="vf-sub" x="87" y="112" textAnchor="middle" dominantBaseline="central">CSV rows</text>
            <line className="vf-arrow" x1="136" y1="100" x2="153" y2="100" markerEnd="url(#vf-arrow)" />

            <rect className="vf-card" x="155" y="72" width="94" height="56" rx="10" />
            <text className="vf-title" x="202" y="94" textAnchor="middle" dominantBaseline="central">Validate</text>
            <text className="vf-sub" x="202" y="112" textAnchor="middle" dominantBaseline="central">Rule checks</text>
            <line className="vf-arrow" x1="251" y1="100" x2="268" y2="100" markerEnd="url(#vf-arrow)" />

            <rect className="vf-card" x="270" y="72" width="94" height="56" rx="10" />
            <text className="vf-title" x="317" y="94" textAnchor="middle" dominantBaseline="central">Normalize</text>
            <text className="vf-sub" x="317" y="112" textAnchor="middle" dominantBaseline="central">FHIR map</text>
            <line className="vf-arrow" x1="366" y1="100" x2="383" y2="100" markerEnd="url(#vf-arrow)" />

            <rect className="vf-card" x="385" y="72" width="94" height="56" rx="10" />
            <text className="vf-title" x="432" y="94" textAnchor="middle" dominantBaseline="central">Persist</text>
            <text className="vf-sub" x="432" y="112" textAnchor="middle" dominantBaseline="central">Reports, obs</text>

            <text className="vf-eyebrow" x="568" y="56" textAnchor="middle" fill="var(--warn)">AUDIT LOG</text>
            <rect className="vf-audit-card" x="496" y="72" width="144" height="250" rx="10" />
            <text className="vf-mono" x="568" y="100" textAnchor="middle" dominantBaseline="central" fill="#d6b98a">append-only</text>
            <text className="vf-mono" x="568" y="120" textAnchor="middle" dominantBaseline="central" fill="#d6b98a">timestamped</text>
            <line x1="516" y1="142" x2="620" y2="142" stroke="rgba(245,166,35,0.25)" strokeWidth=".5" />
            <text className="vf-audit-body" x="568" y="168" textAnchor="middle" dominantBaseline="central">Records events</text>
            <text className="vf-audit-body" x="568" y="186" textAnchor="middle" dominantBaseline="central">for every stage</text>
            <text className="vf-audit-body" x="568" y="204" textAnchor="middle" dominantBaseline="central">of the pipeline,</text>
            <text className="vf-audit-body" x="568" y="222" textAnchor="middle" dominantBaseline="central">end-to-end</text>
            <line className="vf-audit-arrow" x1="481" y1="100" x2="494" y2="100" markerEnd="url(#vf-arrow)" />
            <line className="vf-audit-arrow" x1="481" y1="290" x2="494" y2="290" markerEnd="url(#vf-arrow)" />

            <line x1="267" y1="142" x2="267" y2="244" stroke="#8a72c4" strokeWidth="3" strokeLinecap="round" markerEnd="url(#vf-arrow-bold)" />
            <rect x="282" y="179" width="150" height="28" rx="14" fill="rgba(164,143,214,0.12)" stroke="rgba(164,143,214,0.35)" strokeWidth=".5" />
            <text className="vf-mono" x="357" y="194" textAnchor="middle" dominantBaseline="central" fill="#c9b8ef">de-identified data</text>

            <text className="vf-eyebrow" x="40" y="240" fill="#a48fd6">GOVERNED AI LAYER</text>

            <rect className="vf-card" x="40" y="262" width="136" height="60" rx="10" />
            <text className="vf-title" x="108" y="281" textAnchor="middle" dominantBaseline="central">Retrieval</text>
            <text className="vf-sub" x="108" y="299" textAnchor="middle" dominantBaseline="central">RAG guidelines</text>
            <text className="vf-sub" x="108" y="314" textAnchor="middle" dominantBaseline="central">+ history query</text>
            <line className="vf-ai-arrow" x1="178" y1="290" x2="202" y2="290" markerEnd="url(#vf-arrow)" />

            <rect className="vf-ai-card" x="204" y="262" width="126" height="56" rx="10" />
            <text className="vf-ai-title" x="267" y="284" textAnchor="middle" dominantBaseline="central">AI annotation</text>
            <text className="vf-ai-sub" x="267" y="302" textAnchor="middle" dominantBaseline="central">Priority + trend</text>
            <line className="vf-ai-arrow" x1="332" y1="290" x2="356" y2="290" markerEnd="url(#vf-arrow)" />

            <rect className="vf-ai-card" x="358" y="262" width="121" height="56" rx="10" />
            <text className="vf-ai-title" x="418" y="284" textAnchor="middle" dominantBaseline="central">Message draft</text>
            <text className="vf-ai-sub" x="418" y="302" textAnchor="middle" dominantBaseline="central">Needs approval</text>

            <path d="M108 322 L108 338 L418 338 L418 320" fill="none" stroke="rgba(164,143,214,0.3)" strokeWidth=".5" strokeDasharray="3 3" markerEnd="url(#vf-arrow)" />

            <rect className="vf-chip" x="40" y="356" width="116" height="26" rx="6" />
            <text className="vf-mono vf-chip-text" x="98" y="370" textAnchor="middle" dominantBaseline="central">RAG guidelines</text>

            <rect className="vf-chip" x="168" y="356" width="110" height="26" rx="6" />
            <text className="vf-mono vf-chip-text" x="223" y="370" textAnchor="middle" dominantBaseline="central">History query</text>

            <rect className="vf-chip" x="290" y="356" width="123" height="26" rx="6" />
            <text className="vf-mono vf-chip-text" x="351" y="370" textAnchor="middle" dominantBaseline="central">Schema-verified</text>

            <rect className="vf-chip" x="425" y="356" width="136" height="26" rx="6" />
            <text className="vf-mono vf-chip-text" x="493" y="370" textAnchor="middle" dominantBaseline="central">Clinician sign-off</text>
          </svg>
        </div>

      </div>
  );
}
