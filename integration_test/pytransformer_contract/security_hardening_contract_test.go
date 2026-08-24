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
		require.Equal(t, map[string]interface{}{"ok": true}, items[0].Output)

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
		previewWriteGuardVID = "hardening-test-flow-write-guard-v1"
		previewCompileVID    = "hardening-test-flow-compile-violation-v1"
		previewCleanVID      = "hardening-test-flow-clean-v1"
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

// sendInlineTest posts one event to the inline /test preview endpoint and returns the status code and raw body.
//
// The transformationId in the event metadata is what /test labels its security samples with, so it must be set
// explicitly: without it the sample lands under the "unknown" sentinel and no per-transformation assertion is
// possible.
func sendInlineTest(t *testing.T, baseURL, transformationID, code string) (int, string) {
	t.Helper()
	payload := map[string]any{
		"trRevCode": map[string]any{
			"code":        code,
			"codeVersion": "1",
			"language":    "python",
		},
		"events": []any{
			map[string]any{
				"message": map[string]any{"messageId": "m1"},
				"metadata": map[string]any{
					"messageId":        "m1",
					"transformationId": transformationID,
				},
			},
		},
	}
	body, err := jsonrs.Marshal(payload)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, baseURL+"/test", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(respBody)
}
