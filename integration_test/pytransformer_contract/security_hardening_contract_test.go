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
		wg                        sync.WaitGroup
		baselineURL, candidateURL string
	)
	wg.Go(func() {
		baselineURL = startBaselinePytransformer(t, pool, configBackend.URL)
	})
	wg.Go(func() {
		// getattr, import, and (once enforce ships) write guards no longer take an env flag — getattr and import
		// enforce by default. No candidate env override is needed to observe their blocks.
		candidateURL = startCandidatePytransformer(t, pool, configBackend.URL)
	})
	wg.Wait()

	env := newBCTestEnv(t, baselineURL, candidateURL)

	t.Run("getattr() builtin is withdrawn", func(t *testing.T) {
		// NotImplementedError("getattr() is not supported in transformations")
		assertBlockedOnCandidate(t, env, getattrVersionID, "getattr", "not supported")
	})

	t.Run("reaching os by attribute traversal is blocked", func(t *testing.T) {
		// SecurityViolationError("Accessing module 'os' via attribute access is not allowed")
		assertBlockedOnCandidate(t, env, osTraversalVID, "module 'os'", "attribute access", "not allowed")
	})

	t.Run("__getattribute__ cannot be used to bypass the guard", func(t *testing.T) {
		// SecurityViolationError("Accessing '__getattribute__' is not allowed: it is a raw attribute-access ...")
		assertBlockedOnCandidate(t, env, getattributeBypassVID, "__getattribute__", "not allowed")
	})

	t.Run("reading the __getattr__ primitive is refused", func(t *testing.T) {
		// SecurityViolationError("Accessing '__getattr__' is not allowed: it is a raw attribute-access ...")
		assertBlockedOnCandidate(t, env, getattrDunderVID, "__getattr__", "not allowed")
	})

	t.Run("urllib3 raw-HTTP traversal is blocked", func(t *testing.T) {
		// SecurityViolationError("Accessing module 'urllib3' via attribute access is not allowed")
		assertBlockedOnCandidate(t, env, urllib3TraversalVID, "module 'urllib3'", "attribute access", "not allowed")
	})

	t.Run("logging file-write traversal is blocked", func(t *testing.T) {
		// SecurityViolationError("Accessing module 'logging' via attribute access is not allowed")
		assertBlockedOnCandidate(t, env, loggingTraversalVID, "module 'logging'", "attribute access", "not allowed")
	})

	t.Run("from-import of a re-exported module is blocked", func(t *testing.T) {
		// SecurityViolationError("Importing 'socket' from 'requests.utils' is not allowed: it resolves to the
		// non-importable module 'socket'.")
		assertBlockedOnCandidate(t, env, fromImportModuleVID, "socket", "not allowed", "non-importable")
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

	// No baseline container: this test only ever talks to env.CandidateClient (see doc comment above).
	candidateURL := startCandidatePytransformer(t, pool, configBackend.URL, "SANDBOX_WRITE_GUARD_ENFORCE=true")
	env := newBCTestEnv(t, "", candidateURL)

	t.Run("write to a module object is blocked", func(t *testing.T) {
		// SecurityViolationError("Setting attribute 'loads' on a non-user-owned module is not allowed")
		assertBlockedOnCandidate(t, env, moduleWriteVID, "not allowed", "non-user-owned")
	})

	t.Run("write to a foreign (non-user-owned) class is blocked", func(t *testing.T) {
		// SecurityViolationError("Setting attribute 'x' on a non-user-owned foreign class is not allowed")
		assertBlockedOnCandidate(t, env, foreignClassWriteVID, "not allowed", "non-user-owned")
	})
}
