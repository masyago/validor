export default function PersistedResources({
  reports,
  observations,
  showReports,
  setShowReports,
  showObservations,
  setShowObservations,
}) {
  return (
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
            DiagnosticReports ×{reports.length}
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
            Observations ×{observations.length}
          </button>
        </div>
        <div className="scenario-note">
          <p>
            Only lab results that pass validation and normalization are
            persisted as FHIR resources and reach the AI layer. Invalid lab
            reports are rejected, with details recorded in the audit
            log.
          </p>
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
              {JSON.stringify(reports, null, 2)}
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
              {JSON.stringify(observations.slice(0, 3), null, 2)}
              {observations.length > 3 && (
                <div style={{ marginTop: 8, color: "var(--muted)" }}>
                  ... and {observations.length - 3} more
                </div>
              )}
            </pre>
          </div>
        )}
      </>
  );
}
