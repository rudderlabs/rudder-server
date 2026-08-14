package pytransformer_contract

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
)

// TestSubmoduleTraversalContract locks the contract that user code reaching a name through a whitelisted-package
// *submodule* by attribute traversal ("dateutil.parser.parse", "urllib.parse.quote",
// "requests.adapters.HTTPAdapter") produces identical results on the baseline and candidate.
//
// Why this matters: guarded_getattr gained a value-guard that refuses to return a module user code could not import
// (so "requests.utils.os" / "requests.utils.socket" are blocked).
// Each "X.Y.Z" above resolves the intermediate submodule "X.Y" through the runtime "_getattr_" guard, so the
// value-guard runs on it.
// Because "dateutil" / "urllib" / "requests" are import-allowed top-levels, these submodules must stay reachable.
// The requests.* traversal path is already covered by TestRequestsModuleWrapperContract;
// this adds the other two common top-levels, dateutil and urllib, plus the attribute-form of requests.adapters.
func TestSubmoduleTraversalContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		dateutilVersionID = "submodule-dateutil-parser-v1"
		urllibVersionID   = "submodule-urllib-parse-v1"
		adaptersVersionID = "submodule-requests-adapters-v1"
	)

	// dateutil.parser.parse — resolves _getattr_(dateutil, "parser") -> module (top-level dateutil
	// whitelisted) -> .parse.
	dateutilCode := `
import dateutil.parser

def transformEvent(event, metadata):
    dt = dateutil.parser.parse("2024-01-02T03:04:05Z")
    event["year"] = dt.year
    event["iso"] = dt.isoformat()
    return event
`
	// urllib.parse.quote / urlparse — resolves _getattr_(urllib, "parse") -> module (top-level urllib
	// whitelisted; urllib.request stays blocked) -> .quote / .urlparse.
	urllibCode := `
import urllib.parse

def transformEvent(event, metadata):
    event["encoded"] = urllib.parse.quote("a b/c?d=e")
    event["path"] = urllib.parse.urlparse("https://x.io/p?q=1").path
    return event
`
	// requests.adapters.HTTPAdapter — attribute-traversal form (import requests auto-imports the
	// adapters submodule). Dunder reads are avoided (the guard blocks __name__ etc.); ``is not None``
	// keeps the check dunder-free.
	adaptersCode := `
import requests

def transformEvent(event, metadata):
    event["adapter_ok"] = requests.adapters.HTTPAdapter() is not None
    return event
`

	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		dateutilVersionID: {code: dateutilCode},
		urllibVersionID:   {code: urllibCode},
		adaptersVersionID: {code: adaptersCode},
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
		candidateURL = startCandidatePytransformer(t, pool, configBackend.URL)
	})
	wg.Wait()

	env := newBCTestEnv(t, baselineURL, candidateURL)

	cases := []struct {
		name      string
		versionID string
		assertOut func(t *testing.T, out map[string]any)
	}{
		{"dateutil.parser.parse", dateutilVersionID, func(t *testing.T, out map[string]any) {
			require.EqualValues(t, 2024, out["year"])
			require.Equal(t, "2024-01-02T03:04:05+00:00", out["iso"])
		}},
		{"urllib.parse.quote", urllibVersionID, func(t *testing.T, out map[string]any) {
			require.Equal(t, "a%20b/c%3Fd%3De", out["encoded"])
			require.Equal(t, "/p", out["path"])
		}},
		{"requests.adapters.HTTPAdapter", adaptersVersionID, func(t *testing.T, out map[string]any) {
			require.Equal(t, true, out["adapter_ok"])
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := []types.TransformerEvent{makeEvent(tc.versionID+"-msg", tc.versionID)}

			baselineResp := env.BaselineClient.Transform(context.Background(), events)
			candidateResp := env.CandidateClient.Transform(context.Background(), events)

			diff, equal := baselineResp.Equal(&candidateResp)
			require.True(t, equal, "%s: baseline and candidate responses differ:\n%s", tc.name, diff)
			// Both must actually transform this event (no drift to a failure): a whitelisted-submodule
			// traversal is legitimate and must not be caught by the value-guard.
			require.Empty(t, candidateResp.FailedEvents,
				"%s: candidate reported failed events — submodule traversal was wrongly blocked", tc.name)
			require.Empty(t, baselineResp.FailedEvents, "%s: baseline reported failed events", tc.name)
			// Lock the actual transformed output, not just baseline↔candidate parity: a change that
			// altered the value on BOTH versions would still pass Equal but must be caught here.
			require.Len(t, candidateResp.Events, 1, "%s: expected exactly one transformed event", tc.name)
			tc.assertOut(t, candidateResp.Events[0].Output)
		})
	}
}

// legitTraversalCode wraps a boolean reachability expression into a transformation whose output is
// {"ok": <expr>}. Each expression reaches a module (or a class on one) by attribute traversal.
func legitTraversalCode(imports, okExpr string) string {
	return fmt.Sprintf("%s\n\ndef transformEvent(event, metadata):\n    event[\"ok\"] = %s\n    return event\n",
		imports, okExpr)
}

// TestLegitimateModuleTraversalContract pins the must-fix-1 regression and its neighbours: attribute
// traversal into a whitelisted package must stay reachable whenever the module it lands on is genuinely
// harmless — whether that module is a *re-export* of a non-whitelisted stdlib module (warnings, binascii,
// struct) or a plain *submodule* (requests.exceptions/sessions/cookies). The value guard is a fail-closed
// allow-list (SAFE_TRAVERSAL_MODULES + the import allow-list), so every case here must produce identical
// results on baseline and candidate. An earlier allow-list-on-the-result blocked all of these.
//
// Note: urllib3 and logging were dropped from SAFE_TRAVERSAL_MODULES (they hand out raw HTTP / file I/O), so
// their traversal is now an enforce-mode block covered by TestGetattrAndModuleGuardContract — not a legit case.
func TestLegitimateModuleTraversalContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	cases := []struct {
		name string
		code string
	}{
		// Re-exports of NON-whitelisted stdlib modules through a whitelisted parent (the must-fix-1 core).
		// (urllib3 / logging are deliberately absent — they are excluded from SAFE_TRAVERSAL_MODULES; see the note above.)
		{"requests.warnings.filterwarnings", legitTraversalCode("import requests", "callable(requests.warnings.filterwarnings)")},
		{"base64.binascii.Error", legitTraversalCode("import base64", "issubclass(base64.binascii.Error, Exception)")},
		{"base64.struct.pack", legitTraversalCode("import base64", "callable(base64.struct.pack)")},
		// Plain submodules of a whitelisted package (top-level whitelisted, not dangerous).
		{"requests.exceptions.HTTPError", legitTraversalCode("import requests", "issubclass(requests.exceptions.HTTPError, Exception)")},
		{"requests.sessions.Session", legitTraversalCode("import requests", "requests.sessions.Session is not None")},
		{"requests.cookies.RequestsCookieJar", legitTraversalCode("import requests", "requests.cookies.RequestsCookieJar is not None")},
	}

	entries := make(map[string]configBackendEntry, len(cases))
	versionIDs := make([]string, len(cases))
	for i, tc := range cases {
		versionIDs[i] = fmt.Sprintf("legit-traversal-%d-v1", i)
		entries[versionIDs[i]] = configBackendEntry{code: tc.code}
	}

	configBackend := newContractConfigBackend(t, entries)
	defer configBackend.Close()

	var (
		wg                        sync.WaitGroup
		baselineURL, candidateURL string
	)
	wg.Go(func() {
		baselineURL = startBaselinePytransformer(t, pool, configBackend.URL)
	})
	wg.Go(func() {
		candidateURL = startCandidatePytransformer(t, pool, configBackend.URL)
	})
	wg.Wait()

	env := newBCTestEnv(t, baselineURL, candidateURL)

	for i, tc := range cases {
		versionID := versionIDs[i]
		t.Run(tc.name, func(t *testing.T) {
			events := []types.TransformerEvent{makeEvent(versionID+"-msg", versionID)}

			baselineResp := env.BaselineClient.Transform(context.Background(), events)
			candidateResp := env.CandidateClient.Transform(context.Background(), events)

			diff, equal := baselineResp.Equal(&candidateResp)
			require.True(t, equal, "%s: baseline and candidate responses differ:\n%s", tc.name, diff)
			require.Empty(t, candidateResp.FailedEvents,
				"%s: candidate reported failed events — a legitimate traversal was wrongly blocked", tc.name)
			require.Len(t, candidateResp.Events, 1, "%s: expected exactly one transformed event", tc.name)
			require.Equal(t, true, candidateResp.Events[0].Output["ok"],
				"%s: reachability expression should be true", tc.name)
		})
	}
}

// TestInterLibraryTraversalContract pins review.md must-fix-2: re-exporting a sibling library through the
// parent library's namespace (`liba.libb.double(3)`) must keep working. `libb` is compiled into a
// `ModuleType` reached from `liba` via `_getattr_`, so the value guard runs on it — a library module is not
// dangerous to hold, so it must be returned, and baseline and candidate must agree. This is the case the old
// contract harness could not reach (it 404'd every library fetch); the harness now serves libraries.
func TestInterLibraryTraversalContract(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	const (
		transformVID = "interlib-transform-v1"
		libaVID      = "interlib-liba-v1"
		libbVID      = "interlib-libb-v1"
	)

	code := `
import liba

def transformEvent(event, metadata):
    event["v"] = liba.libb.double(3)
    return event
`
	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		transformVID: {
			code: code,
			libraries: []contractLibrary{
				{versionID: libaVID, importName: "liba", code: "import libb"},
				{versionID: libbVID, importName: "libb", code: "def double(x):\n    return x * 2"},
			},
		},
	})
	defer configBackend.Close()

	var (
		wg                        sync.WaitGroup
		baselineURL, candidateURL string
	)
	wg.Go(func() {
		baselineURL = startBaselinePytransformer(t, pool, configBackend.URL)
	})
	wg.Go(func() {
		candidateURL = startCandidatePytransformer(t, pool, configBackend.URL)
	})
	wg.Wait()

	env := newBCTestEnv(t, baselineURL, candidateURL)

	// makeEvents carries both library version ids in the event's Libraries, so pytransformer fetches both.
	events := makeEvents(transformVID, 1, libaVID, libbVID)

	baselineResp := env.BaselineClient.Transform(context.Background(), events)
	candidateResp := env.CandidateClient.Transform(context.Background(), events)

	diff, equal := baselineResp.Equal(&candidateResp)
	require.True(t, equal, "baseline and candidate responses differ:\n%s", diff)
	require.Empty(t, candidateResp.FailedEvents, "candidate failed — inter-library traversal was wrongly blocked")
	require.Len(t, candidateResp.Events, 1, "expected exactly one transformed event")
	require.EqualValues(t, 6, candidateResp.Events[0].Output["v"], "liba.libb.double(3) should be 6")
}
