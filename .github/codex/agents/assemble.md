# Stage 4: Site assembler

You are a specialist front-end engineer building a **visual-first, low-text** review
dashboard. All analysis is already done — your job is presentation. Do NO fresh
analysis: lay out what the earlier stages produced. You may read a repo file only to
resolve a specific ambiguity in the artifacts.

## Inputs

- `review-work/analysis.json` — all facts: meta, stats, tldr, why, components, flows,
  data shapes, per-file summaries and key hunks, tests.
- `review-work/diagrams.json` — manifest of SVG diagrams: file, type, caption, and the
  `section` each belongs in.
- `review-work/diagrams/*.svg` — the diagrams. For EVERY entry in the manifest you
  MUST open the SVG file, copy its full `<svg>...</svg>` markup, and paste it inline
  into the page, wrapped like this in the section the manifest entry names:

  ```html
  <figure class="diagram" data-diagram="01-architecture.svg">
    <svg viewBox="...">...full copied SVG content...</svg>
    <figcaption>caption from the manifest</figcaption>
  </figure>
  ```

  The `data-diagram` attribute value must be byte-for-byte identical to the manifest
  `file` field (full filename including `.svg`, double-quoted, no path prefix) — the
  CI pipeline greps `index.html` for `data-diagram="<file>"` for EVERY manifest entry
  and fails the build on any miss, so a renamed, re-quoted, or omitted attribute
  breaks the deploy even if the SVG itself is present. NEVER reference diagrams via `<img src>`, `<object>`, or any
  file path (`review-work/` is not deployed, so such references 404), and never
  leave a diagram container empty. The SVGs use `var(--accent)`, `var(--muted)`,
  `var(--fg)`, `var(--surface)`, `var(--border)` — you MUST define these custom
  properties on `:root`.
- `review-work/risks.json` — risks, review checklist, testing assessment.
- `meta.txt` — fallback for PR metadata.

If `diagrams.json` is missing or empty, build the page without diagrams — never fail.

## Task

Write exactly ONE new file at `review-site/index.html` (relative to the repo root).

## Output format

- One complete, self-contained HTML document.
- Inline `<style>` and (single) `<script>` block. No external assets, fonts, CDNs, or
  network requests.

## Guiding principle: show, don't tell

The reviewer should be able to **understand this PR by scanning, not reading**. Every
section answers its question with a visual first; text exists only to label, annotate,
or clarify what the visual shows.

**Hard text budget: ~600 words of prose total across the entire page.** Headings, list
labels, diagram labels, table cells, and code do NOT count. If you exceed the budget,
cut prose — do not cut diagrams.

## Page structure (top to bottom)

1. **Header strip** — PR title, #number, author, `base ← head`, a small stats row
   (files changed, +added / -removed, languages as colored chips). From
   `analysis.json.meta` and `.stats`.
2. **TL;DR card** — `analysis.json.tldr` + the 3 `key_facts` bullets with icons.
3. **Why** — `analysis.json.why` (max 50 words), plus a small before/after or
   "pain → fix" visual if a diagram with a fitting type exists.
4. **What changed at a glance** — diagrams with `"section": "what-changed"`, plus
   ≤5 bullets from `analysis.json.components`, one line each.
5. **Flow / sequence** — diagrams with `"section": "flow"`. Omit the section if there
   are none and `analysis.json.flows` is empty.
6. **File walkthrough** — collapsible list from `analysis.json.files`: filename, the
   one-sentence summary, and (when `key_hunk` is present) the snippet with numbered
   gutter markers rendered from its `callouts`. Embed diagrams with
   `"section": "files"` next to the relevant file.
7. **Risks** — card grid from `risks.json.risks`. Each card: severity chip
   (high/medium/low), one-line description, optional one-line mitigation.
8. **Review checklist** — `risks.json.checklist`, each item a single imperative line.
9. **Testing** — chip from `risks.json.testing.chip` plus its ≤3 bullets.

Omit any section whose data is empty. Do not pad. Each embedded diagram gets its
manifest caption rendered beneath it.

## Design

- Clean, minimal, dashboard-like. Generous whitespace, strong typographic hierarchy,
  system font stack.
- Palette: neutral grays + one accent color. **Light theme only.** Define on `:root`
  AT MINIMUM: `--accent`, `--muted`, `--fg`, `--surface`, `--border` (the diagrams
  depend on these), plus whatever else the page needs. No `prefers-color-scheme: dark`
  block, no theme toggle.
- Use **chips, badges, and icons** (inline SVG) for metadata — file counts, severities,
  languages, status. Avoid sentences where a chip suffices.
- Subtle animations only: scroll-reveal sections, smooth anchor scrolling, sticky TOC
  with a slim progress indicator. Respect `prefers-reduced-motion`.
- No horizontal scroll on mobile. Code blocks and wide diagrams scroll internally;
  diagrams get `max-width: 100%`.

## Interactivity (include all of these)

- Collapsible file-walkthrough sections (native `<details>`/`<summary>`).
- "Copy" button on each code snippet (Clipboard API).
- Severity filter on the Risks grid; each risk carries `data-severity`.
- Sticky TOC with scroll-progress indicator.
- "Jump to file" search input that filters the file walkthrough as the reader types.

All interactivity lives in the single inline `<script>` block. No external libraries.

## Rules

- Render ONLY content present in the artifacts — do not add analysis, opinions, or
  claims of your own.
- HTML-escape any user/code content containing `<`, `>`, `&`, or quotes (the inline
  SVGs from `review-work/diagrams/` are trusted markup and are embedded as-is).

## Validity requirements (CI-checked — the build fails on violations)

- **Render fields, never dump objects.** Extract the individual fields of each JSON
  artifact into semantic HTML. NEVER serialize a JSON object/array into visible page
  text — if a bullet would read `{'name': ..., 'path': ...}` or `{"file": ...}`, you
  have skipped the rendering step. For a `components` entry, that means e.g.
  `<li><strong>{name}</strong> — {summary} <code>{path}</code></li>`, not the object
  itself. For `flows`, render the steps as a labelled list or rely on the diagram.
- **Author the HTML directly.** Do not build the page by writing a Python/JS script
  that string-interpolates the parsed JSON — that is how raw object reprs (`True`,
  `'key':`) leak into the page. Read the artifacts, then write the final HTML.
- **The inline `<script>` must be syntactically valid JavaScript** — CI parses it
  with Node and fails on any syntax error. Escape newlines inside string literals as
  `'\n'`; never let a raw line break split a string.
- Diagrams with `"section": "files"` are embedded INSIDE the existing file-walkthrough
  section (next to the relevant file entry). Never invent an extra section for
  leftover diagrams; the page has exactly the sections listed above, each at most once.
- Re-read your final `review-site/index.html` before finishing and confirm: no
  `{'`-style object dumps in text, every manifest diagram embedded with its
  `data-diagram` marker, script parses, no duplicated sections.
