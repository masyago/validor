export default function ContactModal({
  open,
  message,
  setMessage,
  status,
  honeypot,
  setHoneypot,
  onClose,
  onCancel,
  onSend,
}) {
  if (!open) return null;
  return (
    <div className="contact-overlay" onClick={() => status !== "sending" && onClose()}>
      <div className="contact-modal" onClick={(e) => e.stopPropagation()}>
        <h3>Contact us</h3>
        <p className="contact-sub">Send a message to Validor team.</p>
        <input
          type="text"
          className="contact-honeypot"
          value={honeypot}
          onChange={(e) => setHoneypot(e.target.value)}
          tabIndex={-1}
          autoComplete="off"
          aria-hidden="true"
          name="company"
        />
        <textarea
          className="contact-textarea"
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              if (message.trim() && status !== "sending" && status !== "sent") {
                onSend();
              }
            }
          }}
          placeholder="Type your message…"
          disabled={status === "sending" || status === "sent"}
          autoFocus
        />
        {status === "error" && (
          <div className="contact-error">Something went wrong. Please try again.</div>
        )}
        <div className="contact-actions">
          <button
            type="button"
            className="draft-btn secondary"
            onClick={onCancel}
            disabled={status === "sending"}
          >
            Cancel
          </button>
          <button
            type="button"
            className="draft-btn primary"
            onClick={onSend}
            disabled={!message.trim() || status === "sending" || status === "sent"}
          >
            {status === "sending" ? "Sending…" : status === "sent" ? "Sent ✓" : "Send"}
          </button>
        </div>
      </div>
    </div>
  );
}
