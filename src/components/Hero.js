export default function Hero() {
  return (
    <>
      <div className="hero">
        <div className="hero-badge">
          <span className="hero-badge-dot" />
          Governed AI · FHIR R4 · Full Provenance
        </div>
        <h1>
          From raw lab results
          <br />
          to a patient message.
        </h1>
        <p className="hero-sub hero-sub-lead">
          Clinicians lose hours a day to lab results: reviewing them, spotting what matters, and explaining them to patients.
        </p>
        <p className="hero-sub">
          Validor ingests raw analyzer output, validates and normalizes it into FHIR R4, then runs a governed AI layer to surface priority findings, plot trends, and draft the patient message.
        </p>
        <p className="hero-sub hero-sub-gov">
          The AI augments the data, it never gates or alters it. Nothing reaches a patient without a clinician's approval.
        </p>
      </div>

      <div className="hero-features">
        <div className="hero-feature">
          <div className="hero-feature-icon">
            <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M6 3v18" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round"/>
              <path d="M6 4.2c2-1.1 4-1.1 6 0s4 1.1 6 0v7.6c-2 1.1-4 1.1-6 0s-4-1.1-6 0V4.2z" stroke="currentColor" strokeWidth="1.6" strokeLinejoin="round" strokeLinecap="round"/>
            </svg>
          </div>
          <h3>Priority findings</h3>
          <p>Not every result needs a flag. These do.</p>
        </div>
        <div className="hero-feature">
          <div className="hero-feature-icon">
            <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M3 20h18" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" opacity="0.35"/>
              <path d="M4 15.5l4-2.8 4 1.3 4-5.6 3.4-2.2" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/>
              <circle cx="19.4" cy="6.2" r="2.1" fill="#f0a500"/>
            </svg>
          </div>
          <h3>History under every flag</h3>
          <p>Plotted trend for every abnormal value.</p>
        </div>
        <div className="hero-feature">
          <div className="hero-feature-icon">
            <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <rect x="3" y="5.5" width="18" height="13" rx="2.2" stroke="currentColor" strokeWidth="1.6"/>
              <path d="M4 7.5l8 6 8-6" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          </div>
          <h3>Patient-ready drafts</h3>
          <p>Results explained in plain language. Approved by clinician before
            sending.</p>
        </div>
      </div>
    </>
  );
}
