import { truncateIngestionId } from "../../lib/format";

export default function IngestionTrace({ ingestionId, copiedIngestionId, setCopiedIngestionId }) {
  return (
      <div className="run-trace">
        <span className="run-trace-id">
          ingestion_id: {truncateIngestionId(ingestionId)}
          <button
            type="button"
            className="copy-btn"
            aria-label="Copy full ingestion ID"
            title={ingestionId}
            onClick={() => {
              navigator.clipboard?.writeText(ingestionId).then(() => {
                setCopiedIngestionId(true);
                setTimeout(() => setCopiedIngestionId(false), 1500);
              });
            }}
          >
            {copiedIngestionId && <span className="copy-tooltip">Copied!</span>}
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <rect x="9" y="9" width="13" height="13" rx="2" />
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
            </svg>
          </button>
        </span>
      </div>
  );
}
