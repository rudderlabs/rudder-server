You are a senior staff engineer building a **visual-first, low-text** review dashboard
for a reviewer who knows nothing about this PR or this codebase. Lead with diagrams,
labelled visuals, and tight callouts. Prose is supporting material, not the main event.

You have the ENTIRE repository checked out in your current working directory. Use it.

## Inputs

- `pr.diff` — the exact changeset. Read this first.
    - If `pr.diff` exceeds ~200KB, summarize by directory or concern instead of walking
      it line-by-line.
- `meta.txt` — PR title, author, description, commit messages.
- The full repo — read the definitions, callers, related interfaces, configuration, and
  tests touched by the diff. Only read what you need to ground the visuals.

## Task

Write exactly ONE new file at `review-site/index.html` (relative to the repo root).
Do NOT modify, delete, or create any other file. Do NOT run builds, tests, formatters,
linters, or network commands.

## Output format

- One complete, self-contained HTML document.
- Inline `<style>` and (single) `<script>` block. No external assets, fonts, CDNs, or
  network requests.

## Guiding principle: show, don't tell

The reviewer should be able to **understand this PR by scanning, not reading**. Every
section should answer its question with a visual first; text exists only to label,
annotate, or clarify what the visual shows. If you find yourself writing a paragraph,
ask whether a diagram, table, or labelled snippet could replace it.

**Hard text budget: ~600 words of prose total across the entire page.** Headings, list
labels, diagram labels, table cells, and code do NOT count toward this. If you exceed
the budget, cut prose — do not cut diagrams.

## Page structure (visual-first)

Render the page top-to-bottom in this order. Use the visual formats specified; only fall
back to prose when a visual genuinely doesn't fit.

1. **Header strip** — PR title, #number, author avatar/name, `base ← head`, a small
   stats row (files changed, +added / -removed, primary languages as colored chips).
2. **TL;DR card** — a single sentence (max 25 words) + 3 bullet "key facts" with icons.
3. **Why** — one short paragraph (max 50 words) framing the problem, plus a small
   before/after diagram or "pain → fix" visual if applicable.
4. **What changed at a glance** — a visual map: either an architecture diagram with
   changed components highlighted, OR a treemap/grid of changed directories sized by
   churn, OR a labelled flow showing the new path. Pair with ≤5 bullets, one line each.
5. **Flow / sequence** — if the PR changes an ordered interaction, render a sequence or
   state diagram. Label each arrow/transition. New transitions get the accent color.
6. **File walkthrough** — a collapsible list of changed files. For each: filename, one
   sentence describing the change, and (optionally) a single small annotated snippet
   with numbered callouts pointing to the lines that matter.
7. **Risks** — a card grid. Each card: severity chip (high/medium/low), one-line
   description, optional one-line mitigation. No paragraphs.
8. **Review checklist** — a list of concrete things to verify, each as a single
   imperative line ("Check that X handles Y when Z").
9. **Testing** — visual indicator (e.g., chip "tests added" / "tests modified" /
   "no tests") plus ≤3 bullets naming the tests or stating "no tests added".

Omit any section that genuinely doesn't apply. Do not pad.

## Diagrams (this is where most of the comprehension comes from)

Include at least 2–4 hand-authored inline SVG diagrams, chosen from the list below based
on what this PR actually does. Pick the ones that earn their place.

- **Architecture / component diagram** — multiple components touched or a new one added.
- **Sequence diagram** — ordered interaction changed (request flow, pipeline, retry).
- **State machine** — states or transitions added/changed.
- **Before / after** — behavior or structure meaningfully changes; render side-by-side.
- **Data-shape diff** — struct/schema/config/etcd-key change; old vs. new with new
  fields highlighted in the accent color.
- **Call graph** — change ripples through callers; show who calls the modified symbol.
- **Annotated snippet** — for the most important hunk, render the code with small
  numbered markers in the gutter and a tight legend below.
- **Blast-radius grid** — small heatmap-style grid of touched areas vs. impact.

Diagram requirements:

- Hand-authored inline SVG only. NO Mermaid, D3, Chart.js, or any library.
- Drive all colors from the same CSS custom properties as the page (light theme only).
- Each diagram gets a one-line caption (≤15 words) explaining what to look at.
- New / changed elements use the accent color; unchanged context is muted gray.
- Legible on mobile: `max-width: 100%`, scroll internally if needed, readable SVG fonts.
- Where it adds clarity, make diagrams lightly interactive: hover highlights, clickable
  nodes that scroll to the matching section, or a toggle that flips before/after.

## Design

- Clean, minimal, dashboard-like. Generous whitespace, strong typographic hierarchy,
  system font stack.
- Palette: neutral grays + one accent color. **Light theme only.** Set CSS custom
  properties on `:root`. No `prefers-color-scheme: dark` block, no theme toggle.
- Use **chips, badges, and icons** (inline SVG) for metadata — file counts, severities,
  languages, status. Avoid sentences where a chip suffices.
- Subtle animations only: scroll-reveal sections, smooth anchor scrolling, sticky TOC
  with a slim progress indicator. Respect `prefers-reduced-motion`.
- No horizontal scroll on mobile. Code blocks scroll internally.

## Interactivity (include all of these)

- Collapsible file-walkthrough sections (native `<details>`/`<summary>`).
- "Copy" button on each code snippet (Clipboard API).
- Severity filter on the Risks grid; each risk carries `data-severity`.
- Sticky TOC with scroll-progress indicator.
- "Jump to file" search input that filters the file walkthrough as the reader types.

All interactivity lives in the single inline `<script>` block. No external libraries.

## Rules

- Ground every claim in the diff and the files you actually read. Do NOT invent
  behavior, files, or claims you cannot support. Reference files by path when useful.
- If something is genuinely unclear, say so plainly rather than guessing.
- **Prose budget: ~600 words total.** Prefer diagrams, labelled visuals, chips, and
  tight bullets. Cut prose, not visuals.
- Every section should answer its question visually first; text annotates the visual.
- HTML-escape any user/code content containing `<`, `>`, `&`, or quotes. Never reproduce
  strings that look like secrets (API keys, tokens, private keys, passwords) — replace
  with `[redacted]`.
