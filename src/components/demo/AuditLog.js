export default function AuditLog({ events, showAudit, setShowAudit }) {
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
          AUDIT LOG
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 16 }}>
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
            See Audit Log
          </button>
        </div>
        {showAudit && (
          <div style={{ marginBottom: 16, maxHeight: 300, overflowY: "auto", background: "rgba(58,155,255,0.05)", border: "1px solid var(--border)", borderRadius: 6, padding: 12 }}>
            {(events || []).map((evt) => {
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
  );
}
