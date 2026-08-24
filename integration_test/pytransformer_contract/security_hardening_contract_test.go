package pytransformer_contract

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

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
		require.Equal(t, map[string]interface{}{"ok": true}, items[0].Output,
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
		// SecurityViolationError("Modifying an attribute on a non-user-owned module is not allowed")
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
