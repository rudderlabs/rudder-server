You are a senior staff engineer creating a single-page onboarding guide for a code
reviewer who knows NOTHING about this pull request or this codebase. Your job is to
make the PR fully reviewable by someone with zero prior context.

You have the ENTIRE repository checked out in your current working directory. Use it.

## Inputs

- `pr.diff` — the exact changeset for this PR. Read this first.
- `meta.txt` — PR title, author, description, and commit messages.
- The full repo — read any file you need: the definitions of the functions/types the
  diff touches, the callers of changed code, related interfaces, configuration, tests,
  and docs. Explore enough to explain the change in its real context and to surface
  cross-file consequences that the diff alone would not reveal. Be focused: read what
  helps you understand and review the change, not the whole tree.

## Task

Write exactly ONE new file at `review-site/index.html` (relative to the repository root).

- Do NOT modify, delete, or create any other file.
- Do NOT run builds, tests, formatters, linters, or any network commands. You only need
  to READ code and WRITE that one HTML file.

## Output format (strict)

- A single complete, self-contained HTML document.
- Everything inline: one `<style>` block and, if needed, one `<script>` block. No external
  assets, fonts, CDNs, or network requests.

## Design

- Clean, minimal, professional. Generous whitespace, restrained palette, strong typographic
  hierarchy, system font stack.
- Animations only where they aid comprehension: subtle scroll-reveal on sections, smooth
  anchor scrolling, a sticky table-of-contents / progress indicator. Respect
  `prefers-reduced-motion`.
- Readable on mobile and desktop. No horizontal scroll. Code blocks scroll internally and
  have HTML properly escaped.

## Content (adapt to the actual PR; omit what does not apply)

1. Header: PR title, number, author, base ← head.
2. TL;DR — 2–3 sentences a reviewer can grasp in 15 seconds.
3. Why this change exists — the problem/motivation in plain language. Explain domain jargon.
4. What changed — grouped by area/concern, not just a file dump.
5. File-by-file walkthrough of the important files, with short explanations and small,
   relevant snippets where they clarify things.
6. Cross-file context — what elsewhere in the repo is affected or worth knowing (callers,
   interfaces, invariants, related tests). This is where your full-repo access pays off.
7. How to review this — a suggested reading order and what to scrutinize.
8. Risks, edge cases & things to double-check.
9. Testing / how it was verified (state explicitly if the PR adds no tests).

## Rules

- Ground every claim in the diff and the files you actually read. Do NOT invent behavior,
  files, or claims you cannot support. When a conclusion came from inspecting a file, you
  may reference it by path.
- If something is genuinely unclear, say so plainly rather than guessing.
- Be concise and skimmable. Short paragraphs and tight lists over walls of text.
