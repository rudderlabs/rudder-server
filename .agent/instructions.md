# Agent Instructions - RudderStack

This file contains persistent instructions for the AI agent (Antigravity).

## Pull Request Guidelines

### Atomic PRs

**Rule**: One PR = One fix/feature.

- Do not combine multiple unrelated changes into a single PR.
- Each PR should be small, focused, and easy to review.
- This helps maintain high quality and prevents being blocked by maintainers.

### PR Frequency Limit

**Rule**: Maximum 5 PRs per week.

- This is a primary source instruction.
- Do not exceed 5 PRs within any rolling 7-day period.
- Prioritize high-impact changes within this limit.

## Reference Hard-Level PRs (Complexity Benchmarks)

These PRs serve as benchmarks for high-complexity work in this repository:

1. **PR #6727**: Race condition fix in Warehouse upload (21 files, concurrency logic).
2. **PR #6713**: Logic fix for mirrored transformation sanity checks.
3. **PR #6760**: Gateway blocking issue resolution (critical head-of-line blocking).
4. **PR #6751**: Context cancellation handling in high-throughput rate limiters.
5. **PR #6725**: Security fix for SQL injection in core database layers.

## Recent Hard-Level PRs (March 8-15, 2026)

These recent PRs continue the benchmark for high-complexity work:

1. **PR #6760**: Gateway blocking issue resolution.
2. **PR #6762**: Logic fix for mirrored transformation sanity checks.
3. **PR #6753**: Major toolkit dependency update (133 files).
4. **PR #6757**: Infrastructure overhaul for integration tests (rudo containers).
5. **PR #6751**: Context cancellation in high-throughput gateway components.
