package pytransformer_contract

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestGetattrAndModuleGuardContract pins the client↔server contract for the deliberate security
// tightenings the candidate rudder-pytransformer introduces on the attribute-access path:
//
//   - the dynamic getattr() builtin is removed;
//   - guarded_getattr refuses to return a module user code could not import (e.g. reaching os via
//     requests.utils.os);
//   - guarded_getattr refuses to hand back the raw attribute-lookup dunders __getattribute__/__getattr__, which
//     would otherwise bypass the guard entirely (e.g. requests.utils.__getattribute__('os') reaches os, and
//     type(os).__getattribute__(os, 'system') escalates to arbitrary calls);
//   - urllib3 and logging are dropped from the value guard's SAFE_TRAVERSAL_MODULES allow-list, so reaching them
//     by traversal (requests.packages.urllib3 -> PoolManager raw HTTP; requests.logging -> FileHandler file I/O)
//     is refused — capabilities the sandbox otherwise withholds;
//   - the import hook refuses `from X import Y` when Y resolves to a non-importable module (e.g.
//     `from requests.utils import socket`), which IMPORT_FROM would otherwise bind without ever hitting the
//     attribute guard.
//
// These are CANDIDATE-ONLY assertions on purpose. The baseline↔candidate equality tests
// (TestBaseContract etc.) cannot express a deliberate divergence: the baseline is a fixed released
// image that predates the change, so an equality assertion on this input would fail by construction.
//
// The contract being locked is the one rudder-server relies on: an escape attempt becomes a clean per-event failure
// (statusCode 400 + error), never a crash, a 5xx, or a silent success.
//
// When the change ships and the baseline advances past it, these can be complemented by a parity
// assertion (both versions block), the way TestLibrary already does for the urllib.request tightening.
func TestGetattrAndModuleGuardContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		getattrVersionID      = "hardening-getattr-removed-v1"
		osTraversalVID        = "hardening-os-traversal-blocked-v1"
		getattributeBypassVID = "hardening-getattribute-bypass-blocked-v1"
		getattrDunderVID      = "hardening-getattr-dunder-blocked-v1"
		urllib3TraversalVID   = "hardening-urllib3-traversal-blocked-v1"
		loggingTraversalVID   = "hardening-logging-traversal-blocked-v1"
		fromImportModuleVID   = "hardening-from-import-module-blocked-v1"
	)

	// getattr() is called at runtime inside transformEvent -> the withdrawal stub raises
	// NotImplementedError -> the event fails (it must not silently succeed).
	getattrCode := `
def transformEvent(event, metadata):
    return {'v': getattr(event, 'x', None)}
`
	// requests.utils.os reaches the os module by attribute traversal -> the value-guard raises
	// SecurityViolationError -> the event fails.
	osTraversalCode := `
def transformEvent(event, metadata):
    import requests
    return {'v': str(requests.utils.os)}
`
	// requests.utils.__getattribute__('os') reaches the os module through the raw attribute-lookup primitive,
	// which never re-enters guarded_getattr. This read is blocked unconditionally by the value guard -> the event
	// fails.
	getattributeBypassCode := `
def transformEvent(event, metadata):
    import requests
    return {'v': str(requests.utils.__getattribute__('os'))}
`
	// Reading the sibling primitive __getattr__ off any object is refused the same way.
	getattrDunderCode := `
def transformEvent(event, metadata):
    return {'v': str(event.__getattr__)}
`
	// requests.packages.urllib3 reaches urllib3 (PoolManager = raw HTTP that skips the managed session's
	// budget/metrics). Dropped from SAFE_TRAVERSAL_MODULES -> blocked unconditionally by the value guard.
	urllib3TraversalCode := `
def transformEvent(event, metadata):
    import requests
    return {'v': str(requests.packages.urllib3.PoolManager)}
`
	// requests.logging reaches the logging module (FileHandler = a file-write primitive the sandbox withholds).
	loggingTraversalCode := `
def transformEvent(event, metadata):
    import requests
    return {'v': str(requests.logging.FileHandler)}
`
	// `from requests.utils import socket` binds the socket module via IMPORT_FROM — raw bytecode that never reaches
	// the attribute guard. The import hook now rejects fromlist members that resolve to a non-importable module.
	fromImportModuleCode := `
def transformEvent(event, metadata):
    from requests.utils import socket
    return {'v': str(socket.socket)}
`

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		getattrVersionID:      {code: getattrCode},
		osTraversalVID:        {code: osTraversalCode},
		getattributeBypassVID: {code: getattributeBypassCode},
		getattrDunderVID:      {code: getattrDunderCode},
		urllib3TraversalVID:   {code: urllib3TraversalCode},
		loggingTraversalVID:   {code: loggingTraversalCode},
		fromImportModuleVID:   {code: fromImportModuleCode},
	})
	defer configBackend.Close()
	t.Logf("Config backend at %s", configBackend.URL)

	var (
		wg                                    sync.WaitGroup
		baselineURL, candidateURL, metricsURL string
	)
	wg.Go(func() {
		baselineURL = startBaselinePytransformer(t, pool, configBackend.URL)
	})
	wg.Go(func() {
		// getattr, import, and (once enforce ships) write guards no longer take an env flag — getattr and import
		// enforce by default. No candidate env override is needed to observe their blocks.
		//
		// Started WITH the Prometheus port so each block can be checked against the counter it feeds, not just
		// against the 400. transformer_security_violations_total is the series ops alerts on, and folding the
		// per-guard counters into it made every runtime block land here — so the block being observable is now
		// part of the contract, not an implementation detail.
		candidateURL, metricsURL = startRudderPytransformerWithMetrics(t, pool, configBackend.URL)
	})
	wg.Wait()

	env := newBCTestEnv(t, baselineURL, candidateURL)

	// requireSecurityViolation asserts the block for versionID reached the parent registry. Called per subtest with
	// that subtest's own hard-coded versionID — never a shared default — so a counter attributed to the wrong
	// transformation fails here instead of producing an un-attributable page.
	requireSecurityViolation := func(t *testing.T, versionID string) {
		t.Helper()
		requireMetricAtLeast(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": versionID}, 1,
			"a runtime block must surface on the parent /metrics as a security violation")
	}

	t.Run("getattr() builtin is withdrawn", func(t *testing.T) {
		// NotImplementedError("getattr() is not supported in transformations")
		assertBlockedOnCandidate(t, env, getattrVersionID, "getattr", "not supported")
		// Deliberately NOT a security violation: the builtin is withdrawn from the namespace, so this is an
		// unsupported call rather than a guard refusing an access. Pinned so the distinction stays a decision — if
		// getattr() should count as a violation, the change belongs in builtins.py's getattr_not_supported.
		requireMetricEquals(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": getattrVersionID}, 0,
			"a withdrawn builtin is not a blocked access")
	})

	t.Run("reaching os by attribute traversal is blocked", func(t *testing.T) {
		// SecurityViolationError("Accessing module 'os' via attribute access is not allowed")
		assertBlockedOnCandidate(t, env, osTraversalVID, "module 'os'", "attribute access", "not allowed")
		requireSecurityViolation(t, osTraversalVID)
	})

	t.Run("__getattribute__ cannot be used to bypass the guard", func(t *testing.T) {
		// SecurityViolationError("Accessing '__getattribute__' is not allowed: it is a raw attribute-access ...")
		assertBlockedOnCandidate(t, env, getattributeBypassVID, "__getattribute__", "not allowed")
		requireSecurityViolation(t, getattributeBypassVID)
	})

	t.Run("reading the __getattr__ primitive is refused", func(t *testing.T) {
		// SecurityViolationError("Accessing '__getattr__' is not allowed: it is a raw attribute-access ...")
		assertBlockedOnCandidate(t, env, getattrDunderVID, "__getattr__", "not allowed")
		requireSecurityViolation(t, getattrDunderVID)
	})

	t.Run("urllib3 raw-HTTP traversal is blocked", func(t *testing.T) {
		// SecurityViolationError("Accessing module 'urllib3' via attribute access is not allowed")
		assertBlockedOnCandidate(t, env, urllib3TraversalVID, "module 'urllib3'", "attribute access", "not allowed")
		requireSecurityViolation(t, urllib3TraversalVID)
	})

	t.Run("logging file-write traversal is blocked", func(t *testing.T) {
		// SecurityViolationError("Accessing module 'logging' via attribute access is not allowed")
		assertBlockedOnCandidate(t, env, loggingTraversalVID, "module 'logging'", "attribute access", "not allowed")
		requireSecurityViolation(t, loggingTraversalVID)
	})

	t.Run("from-import of a re-exported module is blocked", func(t *testing.T) {
		// SecurityViolationError("Importing 'socket' from 'requests.utils' is not allowed: it resolves to the
		// non-importable module 'socket'.")
		assertBlockedOnCandidate(t, env, fromImportModuleVID, "socket", "not allowed", "non-importable")
		requireSecurityViolation(t, fromImportModuleVID)
	})

	// Every violation above carried a real transformation_id. If the request-level context is ever lost, the
	// samples land under the "unknown" sentinel instead — which still pages, but with nothing to act on.
	t.Run("no violation is attributed to the unknown sentinel", func(t *testing.T) {
		requireMetricEquals(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": "unknown"}, 0,
			"a violation nobody can attribute to a transformation is an unactionable page")
	})
}

// assertBlockedOnCandidate sends one event through env's candidate and asserts the escape became a
// clean per-event failure whose error message identifies the *specific* block — every substring in
// wantErrSubstrs must appear. Locking the message (not just the 400) is what makes this a contract on
// the security violation itself rather than on any generic bad-request. Shared by every hardening
// contract test in this file so the assertion is defined once and cannot drift between them.
func assertBlockedOnCandidate(t *testing.T, env *bcTestEnv, versionID string, wantErrSubstrs ...string) {
	t.Helper()
	events := []types.TransformerEvent{makeEvent(versionID+"-msg", versionID)}

	resp := env.CandidateClient.Transform(context.Background(), events)

	require.Empty(t, resp.Events, "candidate must not produce a successful event for an escape attempt")
	require.Len(t, resp.FailedEvents, 1, "candidate must fail exactly the one event")
	fe := resp.FailedEvents[0]
	require.Equal(t, http.StatusBadRequest, fe.StatusCode,
		"escape must be a clean 400 per-event failure, not a 5xx/crash; error=%q", fe.Error)
	require.NotEmpty(t, fe.Error, "a security block must carry an error message")
	require.Nil(t, fe.Output, "a blocked event must not carry an output payload; got %v", fe.Output)
	low := strings.ToLower(fe.Error)
	for _, want := range wantErrSubstrs {
		require.Containsf(t, low, strings.ToLower(want),
			"failure error should identify the specific block (missing %q); got %q", want, fe.Error)
	}
}

// TestWriteGuardShadowContract pins the mode this release actually ships: SHADOW, the default, where the write
// guard records a flagged write on transformer_write_guard_unsafe_total and then ALLOWS it. If this test ever
// fails, the shadow release is not shadow — a customer whose transformation writes to a module or to a class it
// did not define is being broken by a rollout that promised to break nobody.
//
// The absence of SANDBOX_WRITE_GUARD_ENFORCE from the container env is itself an assertion: the default must be
// shadow, so nothing is passed here on purpose. Its sibling TestWriteGuardEnforceContract pins the other side.
//
// It also carries the only end-to-end proof that the counter survives the trip out of the sandbox. The samples are
// produced in the worker SUBPROCESS and hand-carried to the parent's registry over IPC (metrics_shim.py's
// DICT_BY_LABEL shim -> MetricsData.write_guard -> record_subprocess_metrics), a path the Python suite exercises
// only in-process. A counter that increments in a subprocess and never reaches /metrics is invisible exactly when
// it is being used to decide whether the enforce flip is safe.
//
// CANDIDATE-ONLY, matching its siblings: the write guard is new, so no released baseline image has it.
func TestWriteGuardShadowContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	// Distinct versionIDs from the enforce test so the two can never share a config-backend entry, and distinct
	// from each other so each `kind` can be attributed to exactly one transformation.
	const (
		shadowModuleVID       = "hardening-write-guard-module-shadow-v1"
		shadowForeignClassVID = "hardening-write-guard-foreign-class-shadow-v1"
	)

	// Same two shapes as the enforce test, so the pair differs only by the flag.
	shadowModuleCode := `
def transformEvent(event, metadata):
    import json
    json.loads = None
    return {'ok': True}
`
	shadowForeignClassCode := `
def transformEvent(event, metadata):
    import requests
    setattr(requests.exceptions.HTTPError, 'x', 1)
    return {'ok': True}
`

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		shadowModuleVID:       {code: shadowModuleCode},
		shadowForeignClassVID: {code: shadowForeignClassCode},
	})
	defer configBackend.Close()
	t.Logf("Config backend at %s", configBackend.URL)

	// No SANDBOX_WRITE_GUARD_ENFORCE: the default must be shadow.
	pyURL, metricsURL := startRudderPytransformerWithMetrics(t, pool, configBackend.URL)

	assertAllowed := func(t *testing.T, versionID string) {
		t.Helper()
		status, _, items := sendRawTransform(t, pyURL,
			[]types.TransformerEvent{makeEvent(versionID+"-msg", versionID)})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equalf(t, http.StatusOK, items[0].StatusCode,
			"shadow must ALLOW the write, not block it (error=%s)", items[0].Error)
		require.Equal(t, map[string]any{"ok": true}, items[0].Output,
			"the transformation must run to completion in shadow")
	}

	t.Run("a write to a module is allowed and recorded", func(t *testing.T) {
		assertAllowed(t, shadowModuleVID)
		requireMetricAtLeast(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": shadowModuleVID}, 1,
			"the shadow signal must cross the subprocess->parent IPC boundary and reach /metrics; "+
				"without it there is no data to decide the enforce flip on")
	})

	t.Run("a write to a foreign class is allowed and recorded", func(t *testing.T) {
		assertAllowed(t, shadowForeignClassVID)
		requireMetricAtLeast(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "foreign_class", "transformation_id": shadowForeignClassVID}, 1,
			"foreign_class must be reported under its own kind label")
	})

	// The two `kind` values must be distinct label values, not a collapsed set. Without these cross-checks a
	// regression that reported every write under one kind would pass both assertions above.
	t.Run("kinds are not collapsed into one another", func(t *testing.T) {
		requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "foreign_class", "transformation_id": shadowModuleVID}, 0,
			"the module-writing transformation must contribute nothing under kind=foreign_class")
		requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": shadowForeignClassVID}, 0,
			"the class-writing transformation must contribute nothing under kind=module")
	})

	// Shadow blocks nothing, so nothing may reach the security-violation series — that series is alerted on, and a
	// shadow release that pages on-call for writes it is still allowing would be worse than useless.
	t.Run("shadow records no security violation", func(t *testing.T) {
		for _, vid := range []string{shadowModuleVID, shadowForeignClassVID} {
			requireMetricEquals(t, metricsURL, "transformer_security_violations_total",
				map[string]string{"transformation_id": vid}, 0,
				"nothing is blocked in shadow, so nothing is a security violation")
		}
	})
}

// TestWriteGuardEnforceContract pins the client↔server contract for the write guard's enforce mode
// (SANDBOX_WRITE_GUARD_ENFORCE=true): a write (setattr / attribute-store) to a non-user-owned target — any
// module object, or a class the transformation/library did not define — is refused rather than silently
// applied. The guard defaults to shadow (record + allow) precisely so a customer cannot be broken by
// flipping it on unannounced; this test locks the OTHER side of that switch, the one this rollout is
// building towards.
//
// Both targets below are reached by a plain top-level import + attribute read (json, requests.exceptions),
// which the getattr/import guards already allow unconditionally, so the write itself — not the read — is
// what trips the block here. The targets are deliberately NOT socket.socket: requests.utils.socket is not
// on the value guard's reachability allow-list (SAFE_TRAVERSAL_MODULES), so a socket.socket write would be
// refused at the READ step by the getattr guard, carrying the getattr guard's error message instead of the
// write guard's — the wrong contract for this test to pin.
//
// CANDIDATE-ONLY on purpose, matching TestGetattrAndModuleGuardContract above: the write guard is new, so
// no baseline (a released image that predates it) is started here — there is nothing for it to compare
// against.
func TestWriteGuardEnforceContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		moduleWriteVID       = "hardening-write-guard-module-blocked-v1"
		foreignClassWriteVID = "hardening-write-guard-foreign-class-blocked-v1"
	)

	// json.loads = None rebinds an attribute on the json MODULE object itself -> write guard kind=module.
	moduleWriteCode := `
def transformEvent(event, metadata):
    import json
    json.loads = None
    return {'ok': True}
`
	// requests.exceptions.HTTPError is reached by plain traversal off the whitelisted 'requests' import — it is a
	// CLASS, not a module, so the getattr/value guard never fires reading it. setattr on it is a write to a class
	// the transformation does not own -> write guard kind=foreign_class.
	foreignClassWriteCode := `
def transformEvent(event, metadata):
    import requests
    setattr(requests.exceptions.HTTPError, 'x', 1)
    return {'ok': True}
`

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		moduleWriteVID:       {code: moduleWriteCode},
		foreignClassWriteVID: {code: foreignClassWriteCode},
	})
	defer configBackend.Close()
	t.Logf("Config backend at %s", configBackend.URL)

	// No baseline container: this test only ever talks to env.CandidateClient (see doc comment above). Started with
	// the Prometheus port so the block can be checked against the counters it feeds, not just against the 400.
	candidateURL, metricsURL := startRudderPytransformerWithMetrics(
		t, pool, configBackend.URL, "SANDBOX_WRITE_GUARD_ENFORCE=true")
	env := newBCTestEnv(t, "", candidateURL)

	t.Run("write to a module object is blocked", func(t *testing.T) {
		// `json.loads = None` is an attribute STORE, so it routes through guarded_write, not guarded_setattr:
		// SecurityViolationError("Modifying a non-user-owned module is not allowed"). guarded_write names no
		// attribute because RestrictedPython routes subscript stores through it too and it only sees the target.
		assertBlockedOnCandidate(t, env, moduleWriteVID, "not allowed", "non-user-owned")
	})

	t.Run("write to a foreign (non-user-owned) class is blocked", func(t *testing.T) {
		// SecurityViolationError("Setting attribute 'x' on a non-user-owned foreign class is not allowed")
		assertBlockedOnCandidate(t, env, foreignClassWriteVID, "not allowed", "non-user-owned")
	})

	// A refused write is a security violation like any other, so under enforce it must land on the SHARED counter
	// alongside the getattr and import blocks — otherwise the write guard would be the one guard that blocks
	// silently, invisible to the alerting every other block reaches. Its shadow sibling pins the inverse: nothing
	// on this series while the guard is only recording.
	t.Run("a refused write is recorded as a security violation", func(t *testing.T) {
		for _, vid := range []string{moduleWriteVID, foreignClassWriteVID} {
			requireMetricAtLeast(t, metricsURL, "transformer_security_violations_total",
				map[string]string{"transformation_id": vid}, 1,
				"an enforced write-guard block must be as visible as any other blocked access")
			requireMetricAtLeast(t, metricsURL, "transformer_write_guard_unsafe_total",
				map[string]string{"transformation_id": vid}, 1,
				"the write-guard counter must fire under enforce too, not only in shadow")
		}
	})
}

// TestWriteGuardOwnershipRegistryContract pins the two shapes that decide whether the ownership registry can be
// attacked or accidentally tripped, across the container boundary.
//
// The registry is keyed by object IDENTITY, and both properties below follow from that rather than from a rule
// written anywhere:
//
//   - a class whose metaclass makes it UNHASHABLE, or gives it a __hash__ that raises, must still be registered
//     and still be writable. A hash-keyed registry raises out of the class STATEMENT, so a transformation that
//     compiles and runs on a build with no write guard would 400 at the shipping default — the one thing this
//     release may not do, and not something any flag could turn off.
//   - a class whose metaclass claims __eq__ with everything must NOT launder ownership onto a foreign class. A
//     hash-keyed registry reads the forged class back as user-owned and enforce waves the write through, which is
//     a complete bypass of the guard.
//
// Both are executed in the real container because the registry lives entirely inside the worker and neither
// property is visible from the parent except through behaviour.
//
// CANDIDATE-ONLY: the registry is new, so no released baseline image has it.
func TestWriteGuardOwnershipRegistryContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		unhashableVID = "hardening-write-guard-unhashable-class-v1"
		forgedVID     = "hardening-write-guard-forged-ownership-v1"
	)

	// Meta defines __eq__ without __hash__, so CPython sets Base.__hash__ = None: Base and every subclass of it
	// are unhashable. Child goes through the __metaclass__ chokepoint like any class statement.
	unhashableCode := `
class Meta(type):
    def __eq__(self, other):
        return True

Base = Meta('Base', (), {})

def transformEvent(event, metadata):
    class Child(Base):
        pass
    Child.tag = 'landed'
    return {'tag': Child.tag}
`

	// Own is registered through the chokepoint, and its metaclass claims it hashes and compares equal to
	// requests.exceptions.HTTPError. A hash-keyed registry would then read HTTPError back as user-owned.
	forgedCode := `
def transformEvent(event, metadata):
    import requests
    target = requests.exceptions.HTTPError
    Meta = type('Meta', (type,), {
        '__hash__': lambda self: hash(target),
        '__eq__': lambda self, other: True,
    })
    Anchor = Meta('Anchor', (), {})
    class Own(Anchor):
        pass
    setattr(target, 'pwned', 'FORGED')
    return {'pwned': target.pwned}
`

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		unhashableVID: {code: unhashableCode},
		forgedVID:     {code: forgedCode},
	})
	defer configBackend.Close()

	// Enforce, deliberately: both properties are strictest with the flag on. The unhashable class must still be
	// OWNED (so the write is allowed), and the forged one must still be FOREIGN (so the write is refused).
	pyURL, metricsURL := startRudderPytransformerWithMetrics(
		t, pool, configBackend.URL, "SANDBOX_WRITE_GUARD_ENFORCE=true")

	t.Run("an unhashable class is registered and stays writable", func(t *testing.T) {
		status, _, items := sendRawTransform(t, pyURL,
			[]types.TransformerEvent{makeEvent(unhashableVID+"-msg", unhashableVID)})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equalf(t, http.StatusOK, items[0].StatusCode,
			"the class statement must not fail and the write must be allowed (error=%s)", items[0].Error)
		require.Equal(t, map[string]any{"tag": "landed"}, items[0].Output)

		for _, kind := range []string{"module", "foreign_class", "restricted_name"} {
			requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
				map[string]string{"kind": kind, "transformation_id": unhashableVID}, 0,
				"a class the sandbox built is not a flagged write target, whatever its metaclass does")
		}
	})

	t.Run("a forged __hash__/__eq__ cannot launder ownership of a foreign class", func(t *testing.T) {
		status, _, items := sendRawTransform(t, pyURL,
			[]types.TransformerEvent{makeEvent(forgedVID+"-msg", forgedVID)})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equalf(t, http.StatusBadRequest, items[0].StatusCode,
			"ownership was forged and enforce allowed the write: output=%v", items[0].Output)
		require.Contains(t, items[0].Error, "not allowed")

		requireMetricAtLeast(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "foreign_class", "transformation_id": forgedVID}, 1,
			"the refused write must be counted under its real kind")
		requireMetricAtLeast(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": forgedVID}, 1,
			"a refused write is a security violation like any other")
	})
}

// TestWriteGuardCounterUnitContract pins what a transformer_write_guard_unsafe_total sample MEANS, at the
// boundary rudder-server actually reads it from.
//
// The unit is REFUSED WRITES, not failed transformations, and the two are not the same number.
// SecurityViolationError subclasses Exception, so a transformation that wraps a flagged write in try/except is
// refused the write under enforce and then returns a perfectly ordinary 200. That is a sample with no failure
// behind it. It is not a defect: shadow raises nothing, so there is no exception for a handler to catch and no
// way for the guard to know the difference — the counter is exact about the refusal and silent about the outcome
// on purpose.
//
// This matters here rather than only in a unit test because the flip decision is made by reading this series off
// /metrics. Anyone reading it as "transformations that will break" over-counts by exactly the transformations
// that catch, and would either hold the flip back for no reason or, worse, stop trusting the number. Pinning the
// meaning at the container boundary is what stops the two readings from drifting apart.
//
// Both modes, same code, same versionId shape: the counter moves in both, and in NEITHER does the request fail.
//
// CANDIDATE-ONLY: the write guard is new, so no released baseline image has this counter at all.
func TestWriteGuardCounterUnitContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		shadowVID  = "hardening-write-guard-caught-shadow-v1"
		enforceVID = "hardening-write-guard-caught-enforce-v1"
	)

	// The write is flagged (kind=module) and the transformation swallows the refusal. Under enforce json.loads is
	// left intact and the event still succeeds; in shadow nothing is raised, so the except never runs and the
	// write lands. Either way the transformation returns {'ok': True}.
	caughtWriteCode := `
def transformEvent(event, metadata):
    import json
    try:
        json.loads = None
    except Exception:
        pass
    return {'ok': True}
`

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		shadowVID:  {code: caughtWriteCode},
		enforceVID: {code: caughtWriteCode},
	})
	defer configBackend.Close()
	t.Logf("Config backend at %s", configBackend.URL)

	assertSucceeds := func(t *testing.T, pyURL, versionID string) {
		t.Helper()
		status, _, items := sendRawTransform(t, pyURL,
			[]types.TransformerEvent{makeEvent(versionID+"-msg", versionID)})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equalf(t, http.StatusOK, items[0].StatusCode,
			"a caught write must not fail the transformation (error=%s)", items[0].Error)
		require.Equal(t, map[string]any{"ok": true}, items[0].Output)
	}

	t.Run("shadow counts the write and the transformation succeeds", func(t *testing.T) {
		pyURL, metricsURL := startRudderPytransformerWithMetrics(t, pool, configBackend.URL)
		assertSucceeds(t, pyURL, shadowVID)
		requireMetricAtLeast(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": shadowVID}, 1,
			"the flagged write must be recorded even though nothing was refused")
		requireMetricEquals(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": shadowVID}, 0,
			"shadow refuses nothing, so nothing is a security violation")
	})

	t.Run("enforce refuses the write and the transformation STILL succeeds", func(t *testing.T) {
		pyURL, metricsURL := startRudderPytransformerWithMetrics(
			t, pool, configBackend.URL, "SANDBOX_WRITE_GUARD_ENFORCE=true")
		// This is the whole point of the test: a sample on this transformation_id, a refused write behind it, and
		// a 200 in front of it. Reading the counter as "these will break" is wrong by exactly this case.
		assertSucceeds(t, pyURL, enforceVID)
		requireMetricAtLeast(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": enforceVID}, 1,
			"the refused write must be counted even though the transformation caught the refusal")
		// A refused write stays visible to alerting however user code reacts to it — the counter's silence about
		// the OUTCOME must never become silence about the REFUSAL.
		requireMetricAtLeast(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": enforceVID}, 1,
			"a swallowed block is still a block and must reach the security series")
	})
}

// TestWriteGuardLibraryOwnershipContract pins the half of the write guard that must NOT fire.
//
// Every other write-guard test proves the guard sees something. This one proves it stays quiet, and it is the more
// load-bearing direction of the two: the enforce flip is decided by reading
// transformer_write_guard_unsafe_total, so a FALSE POSITIVE on ordinary user code does not just annoy someone, it
// makes the number unreadable and the flip undecidable.
//
// A compiled library is the transformation's own code. `import mylib; mylib.api_key = ...` targets a
// types.ModuleType exactly like `json` does, and a class defined inside the library is a class like
// requests.exceptions.HTTPError is — so the only thing separating "user write" from "module poisoning" is the
// identity registry the import hook and the __metaclass__ chokepoint fill (imports.py user_modules,
// builtins.py user_classes). Those registries live entirely inside the worker subprocess; nothing about them is
// observable from the parent except the absence of samples, which is what this test reads.
//
// Both modes are exercised on purpose. Shadow proves the counter stays at zero; enforce proves the transformation
// still RUNS — a registry that failed to register would surface as a 400 there, and as silently inflated shadow
// numbers here.
//
// CANDIDATE-ONLY: user_modules is new, so no released baseline image has it.
func TestWriteGuardLibraryOwnershipContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		libOwnershipVID = "hardening-write-guard-library-ownership-v1"
		libVID          = "hardening-write-guard-library-v1"
	)

	// Writes to BOTH shapes the registries cover: the library module object itself (user_modules) and a class the
	// library defined (user_classes). One transformation covers both so a single zero-assertion per kind is enough.
	libCode := `
API_KEY = ''

class Helper:
    tag = 'initial'
`
	code := `
import mylib

def transformEvent(event, metadata):
    mylib.API_KEY = 'set-by-transformation'
    mylib.Helper.tag = 'set-by-transformation'
    return {'key': mylib.API_KEY, 'tag': mylib.Helper.tag}
`

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		libOwnershipVID: {
			code: code,
			libraries: []contractLibrary{
				{versionID: libVID, importName: "mylib", code: libCode},
			},
		},
	})
	defer configBackend.Close()
	t.Logf("Config backend at %s", configBackend.URL)

	want := map[string]any{"key": "set-by-transformation", "tag": "set-by-transformation"}

	runOnce := func(t *testing.T, metricsURL, pyURL string) {
		t.Helper()
		status, _, items := sendRawTransform(t, pyURL,
			makeEvents(libOwnershipVID, 1, libVID))
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equalf(t, http.StatusOK, items[0].StatusCode,
			"a transformation configuring its own library is an ordinary user write (error=%s)", items[0].Error)
		require.Equal(t, want, items[0].Output, "both writes must have landed")

		// The assertion that matters: not one sample, under any kind. `module` would mean user_modules missed the
		// library; `foreign_class` would mean the __metaclass__ chokepoint missed a class the library defined.
		for _, kind := range []string{"module", "foreign_class", "restricted_name"} {
			requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
				map[string]string{"kind": kind, "transformation_id": libOwnershipVID}, 0,
				"a write to the transformation's OWN library must never be flagged — a false positive here "+
					"inflates the very series the enforce flip is decided from")
		}
		requireMetricEquals(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": libOwnershipVID}, 0,
			"nothing was refused, so nothing is a security violation")
	}

	t.Run("shadow records nothing for a write to the transformation's own library", func(t *testing.T) {
		pyURL, metricsURL := startRudderPytransformerWithMetrics(t, pool, configBackend.URL)
		runOnce(t, metricsURL, pyURL)
	})

	// The flip is the real question: shadow staying at zero is only reassuring if enforce also lets the code run.
	// If ownership were decided by anything the registry cannot see, this subtest is where it turns into a 400.
	t.Run("enforce still allows it", func(t *testing.T) {
		pyURL, metricsURL := startRudderPytransformerWithMetrics(
			t, pool, configBackend.URL, "SANDBOX_WRITE_GUARD_ENFORCE=true")
		runOnce(t, metricsURL, pyURL)
	})
}

// TestWriteGuardDelattrContract covers the third write-guard kind, restricted_name, and the entry point that
// produces it — the guarded delattr() builtin.
//
// delattr deserves its own contract test for a reason none of the other kinds share: it was RAW CPYTHON before
// this guard existed. Every other guard was already in the request path, so "shadow changes nothing" is trivially
// true for them. Here it is a claim about new code sitting on a path every transformation can reach, and the whole
// rollout promise — nothing breaks until SANDBOX_WRITE_GUARD_ENFORCE=true — rests on it. The Python suite proves
// the parity in-process; this proves it survives the real container, the real subprocess pool and the real IPC hop.
//
// restricted_name is also the only kind the target predicate cannot see: it is a rule about the attribute NAME, so
// nothing in is_foreign_write_target produces it. If its sample were dropped anywhere between the worker and
// /metrics, a delete the flip will refuse would be invisible in the data the flip is decided from, and no other
// contract test would notice.
//
// CANDIDATE-ONLY, matching its siblings: the write guard is new, so no released baseline image has it.
func TestWriteGuardDelattrContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		shadowDelattrVID  = "hardening-write-guard-delattr-shadow-v1"
		enforceDelattrVID = "hardening-write-guard-delattr-enforce-v1"
		absentDelattrVID  = "hardening-write-guard-delattr-absent-v1"
	)

	// A freshly constructed requests.Response really carries _content, so this delete REMOVES something. The
	// target has to be a foreign INSTANCE: the sandbox cannot build a user object holding a private attribute
	// (`r._x = 1` is a compile error and setattr refuses the name), and an instance is not itself a flagged write
	// target, so restricted_name is the only kind that can be produced here — which is what makes the assertion
	// below unambiguous.
	reservedNameDelattrCode := `
def transformEvent(event, metadata):
    import requests
    r = requests.Response()
    name = '_content'
    delattr(r, name)
    return {'ok': True}
`
	// The same shape against an attribute that is NOT there. Raw delattr raises AttributeError, and so does every
	// mode of the guard — the flip cannot change this transformation's behaviour, so it must not be counted as one
	// the flip would break.
	absentNameDelattrCode := `
def transformEvent(event, metadata):
    import requests
    r = requests.Response()
    name = '_not_here_at_all'
    try:
        delattr(r, name)
    except AttributeError:
        pass
    return {'ok': True}
`

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		shadowDelattrVID:  {code: reservedNameDelattrCode},
		enforceDelattrVID: {code: reservedNameDelattrCode},
		absentDelattrVID:  {code: absentNameDelattrCode},
	})
	defer configBackend.Close()
	t.Logf("Config backend at %s", configBackend.URL)

	t.Run("shadow allows the delete and records restricted_name", func(t *testing.T) {
		// No SANDBOX_WRITE_GUARD_ENFORCE: the shipping default. delattr must behave exactly like the builtin it
		// replaced — the delete lands and the transformation runs to completion.
		pyURL, metricsURL := startRudderPytransformerWithMetrics(t, pool, configBackend.URL)

		status, _, items := sendRawTransform(t, pyURL,
			[]types.TransformerEvent{makeEvent(shadowDelattrVID+"-msg", shadowDelattrVID)})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equalf(t, http.StatusOK, items[0].StatusCode,
			"shadow must let a reserved-name delattr through, exactly as raw CPython did (error=%s)",
			items[0].Error)
		require.Equal(t, map[string]any{"ok": true}, items[0].Output)

		requireMetricAtLeast(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "restricted_name", "transformation_id": shadowDelattrVID}, 1,
			"the name rule's own sample must cross the subprocess->parent IPC boundary; it is the only "+
				"kind is_foreign_write_target cannot produce, so nothing else would carry it")

		// An instance is not a flagged target, so neither of the other kinds may appear for this transformation.
		// Without these the assertion above would still pass if every kind collapsed into one label value.
		for _, kind := range []string{"module", "foreign_class"} {
			requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
				map[string]string{"kind": kind, "transformation_id": shadowDelattrVID}, 0,
				"a delete on a foreign INSTANCE is a name-rule hit only, not a target-rule hit")
		}

		requireMetricEquals(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": shadowDelattrVID}, 0,
			"nothing is blocked in shadow, so nothing is a security violation")

		// A delete that removes nothing behaves identically before and after the flip, so it must produce no
		// sample at all — otherwise the counter reports breakage that cannot happen.
		status, _, items = sendRawTransform(t, pyURL,
			[]types.TransformerEvent{makeEvent(absentDelattrVID+"-msg", absentDelattrVID)})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode)
		for _, kind := range []string{"module", "foreign_class", "restricted_name"} {
			requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
				map[string]string{"kind": kind, "transformation_id": absentDelattrVID}, 0,
				"deleting an absent attribute raises AttributeError in every mode, so the flip changes "+
					"nothing for this transformation and it must not be counted")
		}
	})

	t.Run("enforce blocks the delete and records it", func(t *testing.T) {
		candidateURL, metricsURL := startRudderPytransformerWithMetrics(
			t, pool, configBackend.URL, "SANDBOX_WRITE_GUARD_ENFORCE=true")
		env := newBCTestEnv(t, "", candidateURL)

		// AttributeError('"_content" is an invalid attribute name because it starts with "_"')
		assertBlockedOnCandidate(t, env, enforceDelattrVID, "invalid attribute name")

		requireMetricAtLeast(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "restricted_name", "transformation_id": enforceDelattrVID}, 1,
			"the counter must fire under enforce too, not only in shadow")

		// This block raises AttributeError, not SecurityViolationError — but it is still a write the guard
		// refused, so it must reach the security series exactly like the module and foreign_class kinds do.
		// Without this the name rule would be the one refused write invisible to the alerting every other block
		// lands on, and the three kinds would disagree about what a block means.
		requireMetricAtLeast(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": enforceDelattrVID}, 1,
			"a refused reserved-name delete is a block, so it belongs on the shared security counter")
	})
}

// TestTestFlowSecurityMetricsContract pins where the inline preview flow's security signals land.
//
// Two things have to be true at once, and they pull in opposite directions. A guard that fires in a worker and is
// never replayed to the parent is a block nobody can see — and /test is where a customer FIRST runs the code the
// write-guard flip would refuse, so dropping it loses the earliest and best signal there is. But preview traffic is
// not production traffic: transformer_security_violations_total is alerted on, and a customer iterating on
// half-finished code in the preview box must not page on-call. transformer_write_guard_unsafe_total is the number
// the flip is decided from, and draft code that may never ship must not inflate it.
//
// The resolution is the transformer_test_flow_* twins. This test pins both halves: the twins move, and the
// production series with the SAME transformation_id stay at zero. The shared transformation_id is the point —
// previewing an already-deployed transformation reuses its real id, so "it would only ever land under unknown" is
// not a defence.
//
// CANDIDATE-ONLY: the twins are new, so no released baseline image exports them.
func TestTestFlowSecurityMetricsContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		previewWriteGuardVID   = "hardening-test-flow-write-guard-v1"
		previewForeignClassVID = "hardening-test-flow-foreign-class-v1"
		previewCompileVID      = "hardening-test-flow-compile-violation-v1"
		previewCleanVID        = "hardening-test-flow-clean-v1"
		previewTestRunVID      = "hardening-test-flow-testrun-v1"
		previewReservedNameVID = "hardening-test-flow-restricted-name-v1"
		previewRuntimeBlockVID = "hardening-test-flow-runtime-block-v1"
	)

	// The inline flow ships the code in the request body, so the config backend is only needed to stand the
	// container up.
	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{})
	defer configBackend.Close()

	pyURL, metricsURL := startRudderPytransformerWithMetrics(t, pool, configBackend.URL)

	t.Run("a shadow write-guard hit in a preview reaches the twin, not the production series", func(t *testing.T) {
		status, body := sendInlineTest(t, pyURL, previewWriteGuardVID, `
def transformEvent(event, metadata):
    import json
    json.loads = None
    return {'ok': True}
`)
		require.Equal(t, http.StatusOK, status, "shadow must allow the write in the preview flow too: %s", body)

		requireMetricAtLeast(t, metricsURL, "transformer_test_flow_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": previewWriteGuardVID}, 1,
			"a preview write-guard hit must survive the subprocess->parent IPC hop and reach /metrics")
		requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": previewWriteGuardVID}, 0,
			"preview traffic must not inflate the production series the enforce flip is decided from")
	})

	// The sibling above covers kind=module only. Without this one, a regression that replayed just that kind onto
	// the twin — dropping foreign_class and restricted_name — would leave every assertion in this test passing
	// while two thirds of the preview signal silently vanished.
	t.Run("a second write-guard kind reaches the twin under its own label", func(t *testing.T) {
		status, body := sendInlineTest(t, pyURL, previewForeignClassVID, `
def transformEvent(event, metadata):
    import requests
    setattr(requests.exceptions.HTTPError, 'x', 1)
    return {'ok': True}
`)
		require.Equal(t, http.StatusOK, status, "shadow must allow the write in the preview flow too: %s", body)

		requireMetricAtLeast(t, metricsURL, "transformer_test_flow_write_guard_unsafe_total",
			map[string]string{"kind": "foreign_class", "transformation_id": previewForeignClassVID}, 1,
			"the twin must carry every kind, not just the one the sibling test happens to exercise")
		requireMetricEquals(t, metricsURL, "transformer_test_flow_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": previewForeignClassVID}, 0,
			"the kinds must stay distinct on the twin, exactly as they are on the production series")
		requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "foreign_class", "transformation_id": previewForeignClassVID}, 0,
			"preview traffic must not inflate the production series the enforce flip is decided from")
	})

	// /testRun is the other half of the inline flow and shares _run_inline — and therefore shares the one call
	// that replays these counters. It differs only in how the request decodes ("codeRevision"/"input" instead of
	// "trRevCode"/"events"), and a drift there decodes to an EMPTY body rather than an error, which would run no
	// code and record nothing while still answering 200.
	t.Run("the /testRun endpoint replays onto the twins too", func(t *testing.T) {
		status, body := sendInlinePreview(t, pyURL, "/testRun", previewTestRunVID, `
def transformEvent(event, metadata):
    import json
    json.loads = None
    return {'ok': True}
`)
		require.Equal(t, http.StatusOK, status, "shadow must allow the write on /testRun as well: %s", body)
		require.Contains(t, body, "transformedEvent",
			"the code must actually have run — an empty decode would answer 200 having done nothing")

		requireMetricAtLeast(t, metricsURL, "transformer_test_flow_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": previewTestRunVID}, 1,
			"/testRun previews must be as visible as /test previews")
		requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": previewTestRunVID}, 0,
			"/testRun is preview traffic too, so it must stay off the production series")
	})

	t.Run("a compile-time block in a preview is still recorded", func(t *testing.T) {
		// Rejected by RestrictedPython at COMPILE time, so the worker returns a whole-execution failure rather
		// than a per-event error. That is the case the recording call is deliberately placed BEFORE the status
		// check for: after it, this sample would be dropped — and a preview that is refused outright is exactly
		// what an operator wants to see.
		status, body := sendInlineTest(t, pyURL, previewCompileVID, `
def transformEvent(event, metadata):
    return event._private
`)
		require.Equal(t, http.StatusBadRequest, status,
			"a compile-time security violation is a whole-execution failure: %s", body)

		requireMetricAtLeast(t, metricsURL, "transformer_test_flow_security_violations_total",
			map[string]string{"transformation_id": previewCompileVID}, 1,
			"a refused preview must not be a block nobody can see")
		requireMetricEquals(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": previewCompileVID}, 0,
			"a customer pasting probing code into the preview box must not page on-call")
	})

	// The third kind. module and foreign_class are covered above; without this one restricted_name appears in this
	// test only inside the all-kinds-zero loop below, so a regression that dropped it from the preview replay would
	// leave every assertion here passing.
	t.Run("the restricted_name kind reaches the twin from a preview too", func(t *testing.T) {
		status, body := sendInlineTest(t, pyURL, previewReservedNameVID, `
def transformEvent(event, metadata):
    import requests
    r = requests.Response()
    delattr(r, '_content')
    return {'ok': True}
`)
		// A 200 alone proves nothing here: /test returns 200 for a REFUSED event too, carrying the error inside
		// transformedEvents (TestTestFlowEnforceContract relies on exactly that). The body assertion is what makes
		// this the shadow half.
		require.Equal(t, http.StatusOK, status, "the preview request must succeed: %s", body)
		require.NotContains(t, body, "not allowed",
			"shadow must ALLOW the delete, not refuse it: %s", body)

		requireMetricAtLeast(t, metricsURL, "transformer_test_flow_write_guard_unsafe_total",
			map[string]string{"kind": "restricted_name", "transformation_id": previewReservedNameVID}, 1,
			"the name rule's kind must reach the preview twin under its own label")
		requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "restricted_name", "transformation_id": previewReservedNameVID}, 0,
			"preview traffic must not inflate the production series")
	})

	// The security twin has positive coverage only for a COMPILE-time violation below. A runtime block takes a
	// different path through _run_inline — the worker returns STATUS_OK with a per-event error — so it is a
	// separate assertion, and it is the shape a customer actually hits while iterating in the preview box.
	t.Run("a runtime block in a preview reaches the security twin", func(t *testing.T) {
		status, body := sendInlineTest(t, pyURL, previewRuntimeBlockVID, `
def transformEvent(event, metadata):
    import requests
    return {'v': str(requests.utils.os)}
`)
		require.Equal(t, http.StatusOK, status,
			"a runtime guard hit is a per-event error inside a 200, not a whole-execution failure: %s", body)
		require.Contains(t, body, "not allowed", "the event must carry the guard's refusal")

		requireMetricAtLeast(t, metricsURL, "transformer_test_flow_security_violations_total",
			map[string]string{"transformation_id": previewRuntimeBlockVID}, 1,
			"a runtime block previewed through /test must be as visible as a compile-time one")
		requireMetricEquals(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": previewRuntimeBlockVID}, 0,
			"a customer probing the sandbox from the preview box must not page on-call")
	})

	// The unhappy-path twin. Without it every assertion above would still pass if the replay fired
	// unconditionally, on every preview, guard or no guard.
	t.Run("a clean preview moves nothing at all", func(t *testing.T) {
		status, body := sendInlineTest(t, pyURL, previewCleanVID, `
def transformEvent(event, metadata):
    return {'ok': True}
`)
		require.Equal(t, http.StatusOK, status, "a clean preview must succeed: %s", body)

		for _, name := range []string{
			"transformer_test_flow_security_violations_total",
			"transformer_security_violations_total",
		} {
			requireMetricEquals(t, metricsURL, name,
				map[string]string{"transformation_id": previewCleanVID}, 0,
				"no guard fired, so nothing may be recorded")
		}
		for _, kind := range []string{"module", "foreign_class", "restricted_name"} {
			requireMetricEquals(t, metricsURL, "transformer_test_flow_write_guard_unsafe_total",
				map[string]string{"kind": kind, "transformation_id": previewCleanVID}, 0,
				"no flagged write happened, so no kind may be recorded")
		}
	})
}

// TestTestFlowEnforceContract is the preview flow's enforce half — the one mode combination every other inline
// test leaves uncovered.
//
// TestTestFlowSecurityMetricsContract runs the preview endpoints at the shipping default, so nothing pinned what
// the flip does to a preview. It has to do three things at once, and they pull against each other: refuse the write
// as a per-event error inside a 200 (the preview UI shows the customer their own error, it is not a request
// failure), record BOTH twins so an operator watching preview traffic can see the refusal, and leave both
// production series at zero so a customer previewing a draft neither pages on-call nor moves the number the flip is
// decided from.
//
// This is also where a customer meets the flip first: they paste code into the preview box long before it is
// deployed. Covering only shadow here would mean the endpoint most likely to show a refusal is the one endpoint
// whose refusal behaviour nobody had executed.
//
// CANDIDATE-ONLY: the flag and both twins are new, so no released baseline image has them.
func TestTestFlowEnforceContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const previewEnforceVID = "hardening-test-flow-enforce-v1"

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{})
	defer configBackend.Close()

	pyURL, metricsURL := startRudderPytransformerWithMetrics(
		t, pool, configBackend.URL, "SANDBOX_WRITE_GUARD_ENFORCE=true")

	status, body := sendInlineTest(t, pyURL, previewEnforceVID, `
def transformEvent(event, metadata):
    import json
    json.loads = None
    return {'ok': True}
`)
	// A runtime refusal is a per-event error, so the request itself still succeeds — the same shape /customTransform
	// uses, and the reason this cannot be asserted as a 400.
	require.Equal(t, http.StatusOK, status, "a refused write is a per-event error, not a failed request: %s", body)
	require.Contains(t, body, "not allowed", "the previewed event must carry the guard's refusal: %s", body)

	t.Run("both preview twins record the refusal", func(t *testing.T) {
		requireMetricAtLeast(t, metricsURL, "transformer_test_flow_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": previewEnforceVID}, 1,
			"the refused write must still be counted under enforce, not only in shadow")
		requireMetricAtLeast(t, metricsURL, "transformer_test_flow_security_violations_total",
			map[string]string{"transformation_id": previewEnforceVID}, 1,
			"a refused write is a block, and a block in a preview must be visible on the preview twin")
	})

	t.Run("neither production series moves", func(t *testing.T) {
		requireMetricEquals(t, metricsURL, "transformer_write_guard_unsafe_total",
			map[string]string{"kind": "module", "transformation_id": previewEnforceVID}, 0,
			"preview traffic must not reach the series the flip is decided from, in either mode")
		requireMetricEquals(t, metricsURL, "transformer_security_violations_total",
			map[string]string{"transformation_id": previewEnforceVID}, 0,
			"a refused preview must not page on-call, in either mode")
	})
}

// sendInlineTest posts one event to the inline /test preview endpoint and returns the status code and raw body.
//
// The transformationId in the event metadata is what /test labels its security samples with, so it must be set
// explicitly: without it the sample lands under the "unknown" sentinel and no per-transformation assertion is
// possible.
func sendInlineTest(t *testing.T, baseURL, transformationID, code string) (int, string) {
	t.Helper()
	return sendInlinePreview(t, baseURL, "/test", transformationID, code)
}

// sendInlinePreview posts one event to either inline preview endpoint.
//
// The two differ on the wire in both directions — /test carries the draft under "trRevCode" with the events under
// "events", /testRun uses "codeRevision" and "input" — but they share _run_inline, and therefore share the single
// call that replays the security counters. Covering only one would leave the other's decode path free to drift
// (a rename on either key decodes to an empty body, not an error) while every metrics assertion still passed.
func sendInlinePreview(t *testing.T, baseURL, path, transformationID, code string) (int, string) {
	t.Helper()
	event := map[string]any{
		"message": map[string]any{"messageId": "m1"},
		"metadata": map[string]any{
			"messageId":        "m1",
			"transformationId": transformationID,
		},
	}
	revision := map[string]any{
		"code":        code,
		"codeVersion": "1",
		"language":    "python",
	}

	var payload map[string]any
	switch path {
	case "/test":
		payload = map[string]any{"trRevCode": revision, "events": []any{event}}
	case "/testRun":
		payload = map[string]any{"codeRevision": revision, "input": []any{event}}
	default:
		t.Fatalf("unknown inline preview path %q", path)
	}

	body, err := jsonrs.Marshal(payload)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(respBody)
}

// TestFactoryReprRiskContract pins the shadow-only telemetry that will size the future __module__ hardening: a
// transformation that BOTH builds a factory-made class (namedtuple / three-argument type()) AND renders it to text
// is the exact shape that hardening could change, and it must surface on transformer_factory_class_repr_risk_total —
// while a factory class that is never rendered must not. The signal changes no behaviour, so it runs at the shadow
// default with no env flag.
//
// CANDIDATE-ONLY: the metric is new, so there is no released baseline image that exports it.
func TestFactoryReprRiskContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		riskyVID = "hardening-factory-repr-risk-v1"
		cleanVID = "hardening-factory-repr-risk-clean-v1"
	)

	// Builds a namedtuple AND renders it with str() — the __module__ stamp would change that string.
	riskyCode := `
def transformEvent(event, metadata):
    import collections
    P = collections.namedtuple('P', ['x'])
    return {'t': str(P)}
`
	// Builds a namedtuple but never renders text, so the stamp could not affect it — must stay quiet.
	cleanCode := `
def transformEvent(event, metadata):
    import collections
    P = collections.namedtuple('P', ['x'])
    return {'x': P(1).x}
`

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		riskyVID: {code: riskyCode},
		cleanVID: {code: cleanCode},
	})
	defer configBackend.Close()
	t.Logf("Config backend at %s", configBackend.URL)

	pyURL, metricsURL := startRudderPytransformerWithMetrics(t, pool, configBackend.URL)

	t.Run("factory build plus text render fires the shadow signal", func(t *testing.T) {
		status, _, items := sendRawTransform(t, pyURL,
			[]types.TransformerEvent{makeEvent(riskyVID+"-msg", riskyVID)})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode)

		requireMetricAtLeast(t, metricsURL, "transformer_factory_class_repr_risk_total",
			map[string]string{"transformation_id": riskyVID}, 1,
			"a transformation that builds a namedtuple and renders it must be flagged so the __module__ blast "+
				"radius is measurable before the flip")
	})

	t.Run("factory build without text render stays quiet", func(t *testing.T) {
		status, _, items := sendRawTransform(t, pyURL,
			[]types.TransformerEvent{makeEvent(cleanVID+"-msg", cleanVID)})
		require.Equal(t, http.StatusOK, status)
		require.Len(t, items, 1)
		require.Equal(t, http.StatusOK, items[0].StatusCode)

		requireMetricEquals(t, metricsURL, "transformer_factory_class_repr_risk_total",
			map[string]string{"transformation_id": cleanVID}, 0,
			"a factory class that is never rendered to text cannot be affected by the __module__ stamp")
	})
}
