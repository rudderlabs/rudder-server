package pytransformer_contract

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

// TestConfigBackendRedirectResponse pins what pytransformer puts on the wire when the config
// backend answers a transformation-code fetch with a 3xx — a redirecting proxy or gateway
// sitting in front of it.
//
// pytransformer does not follow redirects, so the 3xx reaches its response_status_handler and
// raises ConfigBackendRedirectError: HTTP 503 plus the retry headers, the same treatment as a
// 401/403, because a redirect is our misconfiguration and not the customer's code.
//
// Deliberately a raw POST rather than usertransformer.Client: the client retries 503 +
// X-Rudder-Should-Retry indefinitely by design, which is exactly the behaviour
// TestConfigBackendRedirectIsRetriedNotDropped depends on and exactly what would hang here.
func TestConfigBackendRedirectResponse(t *testing.T) {
	const versionID = "redirect-test-v1"

	for _, redirectStatus := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprintf("status_%d", redirectStatus), func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			cb := newRedirectingConfigBackend(t, redirectStatus)
			pyURL := startRudderPytransformer(t, pool, cb.backend.URL)

			status, headers, items := sendRawTransformWithHeaders(t, pyURL, makeEvents(versionID, 1))

			t.Logf("pytransformer returned HTTP %d, should-retry=%q reason=%q",
				status, headers.Get("X-Rudder-Should-Retry"), headers.Get("X-Rudder-Error-Reason"))

			require.Positive(t, cb.backendHits.Load(),
				"config backend was never asked for the transformation code")
			require.Zero(t, cb.targetHits.Load(),
				"redirect was followed: the target server received a request")

			// The whole point: retryable, so rudder-server holds the events instead of
			// aborting them. A 200-level batch here is silent data loss.
			require.Equal(t, http.StatusServiceUnavailable, status)
			require.Equal(t, "true", headers.Get("X-Rudder-Should-Retry"))
			require.Equal(t, "config_backend_redirect", headers.Get("X-Rudder-Error-Reason"))

			require.Len(t, items, 1)
			require.Equal(t, http.StatusServiceUnavailable, items[0].StatusCode)
			// The message must name the redirect. The old one said "Transformation not
			// found", which sent operators hunting for a bad versionId.
			require.Contains(t, items[0].Error, "redirect")
		})
	}
}

// TestConfigBackendRedirectIsRetriedNotDropped is the regression test for the data loss.
//
// Before ConfigBackendRedirectError existed, a 3xx was terminal, and this same scenario
// produced `pu=user_transformer status=aborted status_code=302 count=5` with zero events
// delivered and no dead-letter table to replay them from. One misconfigured proxy destroyed
// every event for every Python transformation, while JS transformations sailed through.
//
// The load-bearing assertion is the recovery: hold the config backend broken, then repair it
// and show the *original* events come out the far end transformed. Aborted events would be
// gone, so phase 2 could not pass. The aborted-count check in phase 1 is there to fail early
// with a clear message rather than as a 60s timeout in phase 2.
func TestConfigBackendRedirectIsRetriedNotDropped(t *testing.T) {
	const (
		redirectStatus = http.StatusFound // 302
		eventsCount    = 5
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer webhookServer.Close()

	cb := newRedirectingConfigBackend(t, redirectStatus)
	pyTransformerURL := startRudderPytransformer(t, pool, cb.backend.URL)

	trServer := transformertest.NewBuilder().Build()
	defer trServer.Close()

	bcServer := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(
			backendconfigtest.NewConfigBuilder().
				WithSource(
					backendconfigtest.NewSourceBuilder().
						WithID("source-1").
						WithWriteKey("writekey-1").
						WithConnection(
							backendconfigtest.NewDestinationBuilder("WEBHOOK").
								WithID("destination-1").
								WithUserTransformation("transformation-1", "version-1").
								WithConfigOption("webhookUrl", webhookServer.URL).
								Build()).
						Build()).
				Build()).
		Build()
	defer bcServer.Close()

	// Keep the retry backoff short so the pipeline recovers promptly once the config
	// backend is repaired. Retries stay unbounded — that is the behaviour under test.
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix,
		"Transformer.Client.UserTransformer.retryRudderErrors.maxInterval"), "100ms")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		err := runRudderServer(
			t, ctx, cancel, gwPort, postgresContainer, bcServer.URL, trServer.URL, pyTransformerURL, t.TempDir(),
		)
		if err != nil {
			t.Logf("rudder-server exited with error: %v", err)
		}
		return err
	})

	url := fmt.Sprintf("http://localhost:%d", gwPort)
	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

	t.Logf("Sending %d identify events while the config backend redirects...", eventsCount)
	require.NoError(t, sendEvents(eventsCount, "identify", "writekey-1", url))

	// Phase 1 — broken. Nothing may be aborted.
	//
	// The gateway jobs are deliberately NOT asserted succeeded here: the processor is
	// blocked retrying the fetch, so they stay in flight. That they are still in flight
	// rather than resolved is the point — under the old terminal behaviour they would
	// already have been marked done, with the events gone.
	t.Log("--- phase 1: config backend redirecting ---")
	hitsBefore := cb.backendHits.Load()
	requireNoAbortedUserTransformations(t, ctx, postgresContainer.DB)
	require.Greater(t, cb.backendHits.Load(), hitsBefore,
		"pytransformer should still be retrying the fetch, so the config backend keeps being hit")

	// Phase 2 — repaired. The same events must come out transformed.
	t.Log("--- phase 2: config backend repaired ---")
	cb.serveCode()

	requireJobsCount(t, ctx, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
	requireJobsCount(t, ctx, postgresContainer.DB, "rt", jobsdb.Succeeded.State, eventsCount)
	requireTransformationApplied(t, ctx, postgresContainer.DB, eventsCount)

	require.Zero(t, cb.targetHits.Load(),
		"redirect was followed: the target server received a request")

	t.Logf("RESULT: all %d events survived the misconfiguration and were delivered transformed",
		eventsCount)

	cancel()
	require.NoError(t, wg.Wait())
}

// redirectingConfigBackend is a config backend that answers /transformation/getByVersionId
// with a redirect until serveCode is called, after which it serves the transformation
// normally. The flip is what lets a test show that events were held rather than destroyed.
//
// newContractConfigBackend can return an arbitrary status, but not a Location header and not
// a mid-test flip, so this is its own fixture. It counts hits on both ends: the target
// counter is the load-bearing one, distinguishing "the redirect was refused" from "the
// redirect was followed", which changes what the test is actually observing. The Location is
// built with toContainerURL so the target is genuinely reachable from inside the pytransformer
// container — otherwise a followed redirect would surface as a connection error and look like
// a refusal.
type redirectingConfigBackend struct {
	backend     *httptest.Server
	target      *httptest.Server
	backendHits atomic.Int64
	targetHits  atomic.Int64
	redirecting atomic.Bool
}

func newRedirectingConfigBackend(t *testing.T, redirectStatus int) *redirectingConfigBackend {
	t.Helper()

	cb := &redirectingConfigBackend{}
	cb.redirecting.Store(true)

	writeCode := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"code":%q,"language":"pythonfaas","codeVersion":"1"}`, `
def transformEvent(event, metadata):
    event['foo'] = 'bar'
    return event
`)
	}

	// Where the redirect points. Serves a perfectly valid transformation, so if the
	// redirect were followed the fetch would succeed and the test would fail loudly on
	// targetHits rather than silently passing for the wrong reason.
	cb.target = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cb.targetHits.Add(1)
		t.Logf("REDIRECT TARGET was contacted: %s %s", r.Method, r.URL.Path)
		writeCode(w)
	}))
	t.Cleanup(cb.target.Close)

	cb.backend = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/transformation/getByVersionId", "/transformationLibrary/getByVersionId":
			cb.backendHits.Add(1)
			if !cb.redirecting.Load() {
				writeCode(w)
				return
			}
			w.Header().Set("Location", toContainerURL(cb.target.URL)+r.URL.Path+"?"+r.URL.RawQuery)
			w.WriteHeader(redirectStatus)
		default:
			t.Logf("CONFIG BACKEND: unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(cb.backend.Close)

	return cb
}

// serveCode repairs the config backend: subsequent fetches get the transformation.
func (cb *redirectingConfigBackend) serveCode() {
	cb.redirecting.Store(false)
}

// requireNoAbortedUserTransformations holds for a few seconds and fails the moment anything
// is aborted at the user transformation stage. require.Never rather than a point-in-time
// check, so a pipeline that has simply not got there yet cannot pass.
func requireNoAbortedUserTransformations(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	require.Never(t, func() bool {
		var aborted int
		err := db.QueryRowContext(ctx, `
			SELECT coalesce(sum(count), 0) FROM reports
			WHERE pu = $1 AND status = 'aborted'
		`, reportingtypes.USER_TRANSFORMER).Scan(&aborted)
		if err != nil {
			return false // reporting not up yet; phase 2 is the real proof either way
		}
		if aborted > 0 {
			t.Logf("ABORTED: %d events aborted at the user transformation stage", aborted)
		}
		return aborted > 0
	},
		10*time.Second,
		500*time.Millisecond,
		"events must be held for retry, not aborted, while the config backend redirects",
	)
}
