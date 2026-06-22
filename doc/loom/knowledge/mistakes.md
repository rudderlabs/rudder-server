# Mistakes & Lessons Learned

> Record mistakes made during development and how to avoid them.
> This file is append-only - agents add discoveries, never delete.
>
> Format: Describe what went wrong, why, and how to avoid it next time.

(Add mistakes and lessons as you encounter them)

## HLL params + murmur seed are a cross-service wire contract — NEVER change them

**What happened (trap to avoid):** When cloning the tracked-users pipeline to MAR, it is
tempting to "tidy up" or re-tune the HLL settings (Log2m, Regwidth) or the murmur seed.
**Why it breaks:** The HLL sketches are decoded and merged on the OTHER side by
rudderstack-reporting's Postgres `hll` extension. The byte layout is determined by
Log2m=16, Regwidth=5, ExplicitThreshold=AutoExplicitThreshold, SparseEnabled=true
(enterprise/trackedusers/users_reporter.go:67-72) and the hash by
`murmur3.Sum64WithSeed(id, 123)` (seed const :34). Any deviation produces sketches the
backend cannot read or merges incorrectly — silent data corruption, not a crash. The seed
even carries the comment "changing this will be non backwards compatible" (:33).
**Prevention:** MAR MUST reuse the identical HLL settings and seed=123. Treat these five
values as constants of the system, not tunables. If MAR needs different precision, that is a
backend coordination decision, not a local edit.
**Fix/Detection:** Diff the HLL settings literal and seed against users_reporter.go; they
must be character-for-character identical.

## Do NOT invert `nonEventStreamSources` to select MAR-eligible sources

**What happened (trap to avoid):** MTU's generate hook passes
`proc.getNonEventStreamSources()` as a source filter. One might assume MAR just needs the
"inverse" set (event-stream sources). **Why it breaks:** `nonEventStreamSources`
(processor.go:864-865) is a GENERIC predicate — it selects any source whose
`SourceDefinition.Category` is non-empty AND not "webhook". Its complement is NOT "MAR
sources"; it sweeps in all webhook/event-stream sources broadly. Inverting it would make MAR
report on the wrong, far larger population.
**Prevention:** MAR must define its OWN explicit positive eligibility filter (e.g. a config
flag or a source/destination attribute), not derive it by negating an unrelated generic
filter.
**Fix:** Decide MAR's source-eligibility predicate from MAR requirements; do not reuse or
invert nonEventStreamSources.

## Fail-closed gate must be wired at BOTH ends

**What happened (trap to avoid):** Wiring the new feature gate only in the feature factory
(reporter side) but forgetting the flusher factory (or vice versa). **Why it breaks:** The
gate `GetBoolVar(false, "<Feature>.enabled")` appears twice — trackedusers/factory.go:14
(returns noop collector when off) and flusher/factory.go:43 (returns NOPCronRunner when off).
If only one side is gated, you either collect data that never flushes or run a flusher with no
data — and the "off by default" guarantee is broken.
**Prevention:** Default MUST be `false` (fail-closed). Gate the reporter side AND the flusher
side with the SAME key. Also wire the app-handler setup in BOTH processorAppHandler.go and
embeddedAppHandler.go — missing one disables MAR in that run mode silently.
