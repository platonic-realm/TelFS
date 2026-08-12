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
      // Toggle the EFFECTIVE theme, not the attribute. The old
      // (system) -> light -> dark -> (system) cycle had a dead click:
      // stepping from explicit-dark to (system) on a dark-preference
      // OS rendered identically, so dark -> light took two presses.
      // Now every click flips what's on screen; when the flip lands
      // on the system's own preference we drop the override entirely,
      // so "follow the OS" remains the steady state it can return to.
      const sysLight = window.matchMedia('(prefers-color-scheme: light)').matches;
      const attr = document.documentElement.getAttribute('data-theme');
      const curLight = attr ? attr === 'light' : sysLight;
      const next = curLight ? 'dark' : 'light';
      const matchesSystem = (next === 'light') === sysLight;
      try {
        if (matchesSystem) localStorage.removeItem('telfs-theme');
        else localStorage.setItem('telfs-theme', next);
      } catch (e) { /* storage disabled */ }
      if (matchesSystem) document.documentElement.removeAttribute('data-theme');
      else document.documentElement.setAttribute('data-theme', next);
    });
  });

  // ── busy feedback on slow form posts ────────────────────────────
  // Stop/unmount (and mount start) block until the daemon actually
  // finishes — worst case ~35 s when a FLOOD_WAIT'd shutdown runs to
  // the watchdog. With no feedback that reads as a dead click and
  // people press the button again. On submit (after any inline
  // confirm() has passed — a canceled confirm sets defaultPrevented),
  // disable the submit button and swap its label to data-busy.
  document.addEventListener('submit', function (e) {
    if (e.defaultPrevented) return;
    const btn = e.target.querySelector('button[type="submit"]');
    if (!btn || btn.disabled) return;
    if (btn.dataset.busy) btn.textContent = btn.dataset.busy;
    btn.disabled = true;
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
