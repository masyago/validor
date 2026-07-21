export default function SiteFooter({ onContact }) {
  return (
      <footer>
        <span className="footer-title">Validor</span>
        <div className="footer-links">
          <button type="button" className="footer-link" onClick={onContact}>
            Contact
          </button>
          <a
            className="footer-link"
            href="https://github.com/masyago/validor"
            target="_blank"
            rel="noopener noreferrer"
          >
            GitHub
          </a>
        </div>
      </footer>
  );
}
