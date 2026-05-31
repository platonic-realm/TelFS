// TelFS web — small UI helpers. Kept deliberately tiny: no framework,
// no build, no bundler. Loaded via <script src="/static/ui.js" defer>.
(function () {
  'use strict';

  // ── theme toggle ────────────────────────────────────────────────
  // Persisted to localStorage; absent means "follow system pref"
  // (the @media (prefers-color-scheme: dark) block in style.css
  // handles that case). Explicit dark/light override is set on the
  // <html data-theme="..."> attribute; the matching CSS block wins.
  function applyStoredTheme() {
    try {
      const t = localStorage.getItem('telfs-theme');
      if (t === 'dark' || t === 'light') {
        document.documentElement.setAttribute('data-theme', t);
      } else {
        document.documentElement.removeAttribute('data-theme');
      }
    } catch (e) { /* localStorage disabled — silently follow system pref */ }
  }
  applyStoredTheme();

  document.addEventListener('DOMContentLoaded', function () {
    const btn = document.getElementById('theme-toggle');
    if (!btn) return;
    btn.addEventListener('click', function () {
      const cur = document.documentElement.getAttribute('data-theme');
      // Cycle: (system) -> light -> dark -> (system).
      let next = 'light';
      if (cur === 'light') next = 'dark';
      else if (cur === 'dark') next = '';
      try {
        if (next) localStorage.setItem('telfs-theme', next);
        else localStorage.removeItem('telfs-theme');
      } catch (e) { /* storage disabled */ }
      if (next) document.documentElement.setAttribute('data-theme', next);
      else document.documentElement.removeAttribute('data-theme');
    });
  });

  // ── close <details class="nav-group"> on outside click ──────────
  // Browsers don't natively close <details> on blur, so a click that
  // lands anywhere outside the open menu leaves it hanging. Wire one
  // delegated handler that closes any open nav-group whose subtree
  // doesn't contain the click target.
  document.addEventListener('click', function (e) {
    document.querySelectorAll('details.nav-group[open]').forEach(function (d) {
      if (!d.contains(e.target)) d.removeAttribute('open');
    });
  });
})();
