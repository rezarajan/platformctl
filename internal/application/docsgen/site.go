package docsgen

import (
	"bytes"
	"fmt"
	"html"
	"regexp"
	"sort"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
)

// Site renders the whole reference as a single self-contained HTML page: a
// sidebar of Kinds, the rendered content, and a client-side search box — no
// server round-trips, no external assets, works offline from `docs build
// --html`. goldmark (the Markdown parser Hugo uses) does MD→HTML; everything
// else is inlined so the output is one portable file.
func Site() (string, error) {
	pages, err := Build()
	if err != nil {
		return "", err
	}

	md := goldmark.New(goldmark.WithExtensions(extension.GFM))

	// index.md first, then Kinds alphabetically.
	names := make([]string, 0, len(pages))
	for name := range pages {
		if name != "index.md" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	ordered := append([]string{"index.md"}, names...)

	var nav, sections strings.Builder
	for _, name := range ordered {
		id := sectionID(name)
		title := pageTitle(pages[name], name)

		var htmlBuf bytes.Buffer
		if err := md.Convert([]byte(pages[name]), &htmlBuf); err != nil {
			return "", fmt.Errorf("render %s: %w", name, err)
		}

		navClass := "nav-item"
		if name == "index.md" {
			navClass += " nav-home"
		}
		fmt.Fprintf(&nav, `<a class="%s" href="#%s" data-section="%s">%s</a>`+"\n", navClass, id, id, html.EscapeString(title))
		fmt.Fprintf(&sections, `<section id="%s" class="doc-section" data-title="%s">%s</section>`+"\n",
			id, html.EscapeString(strings.ToLower(title)), htmlBuf.String())
	}

	page := strings.ReplaceAll(siteTemplate, "{{NAV}}", nav.String())
	page = strings.ReplaceAll(page, "{{SECTIONS}}", sections.String())
	return page, nil
}

func sectionID(fileName string) string {
	return strings.TrimSuffix(fileName, ".md")
}

var h1Re = regexp.MustCompile(`(?m)^#\s+(.+)$`)

func pageTitle(markdown, fallback string) string {
	if m := h1Re.FindStringSubmatch(markdown); m != nil {
		return strings.TrimSpace(m[1])
	}
	return strings.TrimSuffix(fallback, ".md")
}

const siteTemplate = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>platformctl — resource reference</title>
<style>
  :root {
    --bg: #ffffff; --fg: #1a1f29; --muted: #5a6472; --accent: #1f4e79;
    --border: #e2e6ec; --sidebar: #f6f8fa; --code-bg: #f2f4f7; --hit: #fff3bf;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0f1420; --fg: #dfe4ee; --muted: #9aa4b2; --accent: #6ea8dc;
      --border: #232a38; --sidebar: #131a28; --code-bg: #1a2231; --hit: #4a3f1a;
    }
  }
  * { box-sizing: border-box; }
  body { margin: 0; font: 15px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
         color: var(--fg); background: var(--bg); }
  .layout { display: grid; grid-template-columns: 280px 1fr; min-height: 100vh; }
  aside { position: sticky; top: 0; align-self: start; height: 100vh; overflow-y: auto;
          background: var(--sidebar); border-right: 1px solid var(--border); padding: 20px 16px; }
  aside h1 { font-size: 15px; margin: 0 0 4px; letter-spacing: .5px; }
  aside .tagline { color: var(--muted); font-size: 12px; margin: 0 0 16px; }
  #search { width: 100%; padding: 8px 10px; margin-bottom: 6px; border: 1px solid var(--border);
            border-radius: 8px; background: var(--bg); color: var(--fg); font-size: 14px; }
  #search-meta { color: var(--muted); font-size: 12px; min-height: 16px; margin-bottom: 10px; }
  nav { display: flex; flex-direction: column; gap: 2px; }
  .nav-item { text-decoration: none; color: var(--fg); padding: 6px 10px; border-radius: 6px; font-size: 14px; }
  .nav-item:hover { background: var(--border); }
  .nav-item.nav-home { font-weight: 600; margin-bottom: 6px; }
  .nav-item.hidden { display: none; }
  main { padding: 32px 40px; max-width: 900px; overflow-x: hidden; }
  .doc-section { padding-bottom: 24px; border-bottom: 1px solid var(--border); margin-bottom: 24px; }
  .doc-section.hidden { display: none; }
  h1, h2, h3 { line-height: 1.25; }
  main h1 { font-size: 26px; border-bottom: 2px solid var(--accent); padding-bottom: 6px; }
  a { color: var(--accent); }
  table { border-collapse: collapse; width: 100%; margin: 12px 0; display: block; overflow-x: auto; }
  th, td { border: 1px solid var(--border); padding: 6px 10px; text-align: left; vertical-align: top; }
  th { background: var(--sidebar); }
  code { background: var(--code-bg); padding: 1px 5px; border-radius: 4px; font-size: 90%; }
  pre { background: var(--code-bg); padding: 12px; border-radius: 8px; overflow-x: auto; }
  pre code { background: none; padding: 0; }
  mark { background: var(--hit); color: inherit; }
  @media (max-width: 720px) {
    .layout { grid-template-columns: 1fr; }
    aside { position: static; height: auto; border-right: none; border-bottom: 1px solid var(--border); }
  }
</style>
</head>
<body>
<div class="layout">
  <aside>
    <h1>platformctl</h1>
    <p class="tagline">resource reference</p>
    <input id="search" type="search" placeholder="Search the reference…" autocomplete="off" aria-label="Search">
    <div id="search-meta"></div>
    <nav>{{NAV}}</nav>
  </aside>
  <main>{{SECTIONS}}</main>
</div>
<script>
(function () {
  var search = document.getElementById('search');
  var meta = document.getElementById('search-meta');
  var sections = Array.prototype.slice.call(document.querySelectorAll('.doc-section'));
  var navItems = Array.prototype.slice.call(document.querySelectorAll('.nav-item'));
  var originals = sections.map(function (s) { return s.innerHTML; });

  function clearHighlights() {
    sections.forEach(function (s, i) { s.innerHTML = originals[i]; });
  }

  function escapeRe(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }

  function highlight(section, re) {
    var walker = document.createTreeWalker(section, NodeFilter.SHOW_TEXT, null);
    var nodes = [];
    while (walker.nextNode()) {
      var n = walker.currentNode;
      if (n.parentNode && /^(SCRIPT|STYLE)$/.test(n.parentNode.nodeName)) continue;
      if (re.test(n.nodeValue)) nodes.push(n);
      re.lastIndex = 0;
    }
    nodes.forEach(function (n) {
      var span = document.createElement('span');
      span.innerHTML = n.nodeValue.replace(re, '<mark>$&</mark>');
      n.parentNode.replaceChild(span, n);
    });
  }

  function run() {
    var q = search.value.trim();
    clearHighlights();
    if (!q) {
      sections.forEach(function (s) { s.classList.remove('hidden'); });
      navItems.forEach(function (n) { n.classList.remove('hidden'); });
      meta.textContent = '';
      return;
    }
    var needle = q.toLowerCase();
    var re = new RegExp(escapeRe(q), 'gi');
    var shown = 0;
    sections.forEach(function (s) {
      var hit = s.textContent.toLowerCase().indexOf(needle) !== -1;
      s.classList.toggle('hidden', !hit);
      var nav = navItems.filter(function (n) { return n.getAttribute('data-section') === s.id; })[0];
      if (nav) nav.classList.toggle('hidden', !hit);
      if (hit) { shown++; highlight(s, re); }
    });
    meta.textContent = shown + ' section' + (shown === 1 ? '' : 's') + ' match "' + q + '"';
  }

  var t;
  search.addEventListener('input', function () { clearTimeout(t); t = setTimeout(run, 120); });
  // Deep-link support: /#provider highlights that section on load.
  if (location.hash) {
    var el = document.querySelector(location.hash);
    if (el) el.scrollIntoView();
  }
})();
</script>
</body>
</html>
`
