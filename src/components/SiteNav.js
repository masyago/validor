export default function SiteNav() {
  return (
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
          <a href="#demo">Demo</a>
          <a href="https://github.com/masyago/validor" target="_blank" rel="noopener noreferrer">GitHub</a>
        </div>
        <a href="#demo" className="nav-cta">
          Try Demo →
        </a>
      </nav>
  );
}
