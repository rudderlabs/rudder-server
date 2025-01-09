package processor_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

func TestProcessorGeolocation(t *testing.T) {
	const (
		boxfordIP = "2.125.160.216"
		londonIP  = "81.2.69.142"
		unknownIP = "22.125.160.216"
		invalidIP = "invalid"
	)

	t.Run("feature disabled", func(t *testing.T) {
		new(geolocationScenario).
			WithGeolocationFeatureEnabled(false).
			WithGeolocationEnabledAtSource(true).
			WithClientIP(boxfordIP).
			WithContextIP(londonIP).
			Run(t, func(t *testing.T, event string) {
				require.Empty(t, gjson.Get(event, "context.geo").Raw, "no geolocation information should be present when the feature is disabled")
			})
	})

	t.Run("feature enabled but geolocation disabled at source", func(t *testing.T) {
		new(geolocationScenario).
			WithGeolocationFeatureEnabled(true).
			WithGeolocationEnabledAtSource(false).
			WithClientIP(boxfordIP).
			WithContextIP(londonIP).
			Run(t, func(t *testing.T, event string) {
				require.Empty(t, gjson.Get(event, "context.geo").Raw, "no geolocation information should be present when geolocation is disabled at source")
			})
	})

	t.Run("enabled with known clientIP and context.ip", func(t *testing.T) {
		new(geolocationScenario).
			WithGeolocationFeatureEnabled(true).
			WithGeolocationEnabledAtSource(true).
			WithClientIP(boxfordIP).
			WithContextIP(londonIP).
			Run(t, func(t *testing.T, event string) {
				require.NotEmpty(t, gjson.Get(event, "context.geo").Raw, event, "geolocation information should be present")
				require.Equal(t, londonIP, gjson.Get(event, "context.geo.ip").String(), "contex.ip should take precedence over clientIP")
				require.Equal(t, "London", gjson.Get(event, "context.geo.city").String(), "contex.ip should take precedence over clientIP")
			})
	})

	t.Run("enabled with known clientIP without context.ip", func(t *testing.T) {
		new(geolocationScenario).
			WithGeolocationFeatureEnabled(true).
			WithGeolocationEnabledAtSource(true).
			WithClientIP(boxfordIP).
			WithContextIP("").
			Run(t, func(t *testing.T, event string) {
				require.NotEmpty(t, gjson.Get(event, "context.geo").Raw, event, "geolocation information should be present")
				require.Equal(t, boxfordIP, gjson.Get(event, "context.geo.ip").String(), "clientIP should be used by the geolocation service")
				require.Equal(t, "Boxford", gjson.Get(event, "context.geo.city").String(), "clientIP should be used by the geolocation service")
			})
	})

	t.Run("enabled with invalid context.ip but valid clientIP", func(t *testing.T) {
		new(geolocationScenario).
			WithGeolocationFeatureEnabled(true).
			WithGeolocationEnabledAtSource(true).
			WithClientIP(londonIP).
			WithContextIP(invalidIP).
			Run(t, func(t *testing.T, event string) {
				require.NotEmpty(t, gjson.Get(event, "context.geo").Raw, event, "geolocation information should be present")
				require.Equal(t, invalidIP, gjson.Get(event, "context.geo.ip").String(), "geolocation service should use the first non blank context.ip even if invalid")
				require.Equal(t, "", gjson.Get(event, "context.geo.city").String(), "geolocation service should use the first non blank context.ip even if invalid")
			})
	})
}

type geolocationScenario struct {
	gelocationEnabledAtSource bool
	geolocationFeatureEnabled bool
	contextIP                 string
	clientIP                  string
}

func (s *geolocationScenario) WithGeolocationFeatureEnabled(enabled bool) *geolocationScenario {
	s.geolocationFeatureEnabled = enabled
	return s
}

func (s *geolocationScenario) WithGeolocationEnabledAtSource(enabled bool) *geolocationScenario {
	s.gelocationEnabledAtSource = enabled
	return s
}

func (s *geolocationScenario) WithContextIP(ip string) *geolocationScenario {
	s.contextIP = ip
	return s
}

func (s *geolocationScenario) WithClientIP(ip string) *geolocationScenario {
	s.clientIP = ip
	return s
}

func (s *geolocationScenario) Run(t *testing.T, verification func(t *testing.T, event string)) {
	config.Reset()
	defer config.Reset()
	writeKey := "writekey-1"

	gatewayUrl, db, cancel, wg := s.startAll(t, writeKey)
	defer func() {
		cancel()
		_ = wg.Wait()
	}()
	require.NoError(t, s.sendEvent(gatewayUrl, "writekey-1"))

	s.requireJobsCount(t, db, "gw", "succeeded", 1)
	s.requireJobsCount(t, db, "rt", "aborted", 1)
	var payload string
	require.NoError(t, db.QueryRow("SELECT event_payload FROM rt_jobs_1").Scan(&payload))
	verification(t, payload)
}

func (s *geolocationScenario) startAll(t *testing.T, writeKey string) (gatewayUrl string, db *sql.DB, cancel context.CancelFunc, wg *errgroup.Group) {
	ctx, cancel := context.WithCancel(context.Background())
	wg, ctx = errgroup.WithContext(ctx)

	bcserver := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(
			backendconfigtest.NewConfigBuilder().
				WithSource(
					backendconfigtest.NewSourceBuilder().
						WithID("source-1").
						WithWriteKey(writeKey).
						WithGeoenrichmentEnabled(s.gelocationEnabledAtSource).
						WithConnection(
							backendconfigtest.NewDestinationBuilder("WEBHOOK").
								WithID("destination-1").
								Build()).
						Build()).
				Build()).
		Build()

	trServer := transformertest.NewBuilder().Build()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	wg.Go(func() error {
		err := s.runRudderServer(ctx, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir())
		if err != nil {
			t.Logf("rudder-server exited with error: %v", err)
		} else {
			t.Log("rudder-server stopped")
		}
		bcserver.Close()
		trServer.Close()
		return err
	})
	url := fmt.Sprintf("http://localhost:%d", gwPort)
	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	return url, postgresContainer.DB, cancel, wg
}

func (s *geolocationScenario) runRudderServer(ctx context.Context, port int, postgresContainer *postgres.Resource, cbURL, transformerURL, tmpDir string) (err error) {
	config.Set("enableStats", false)
	config.Set("CONFIG_BACKEND_URL", cbURL)
	config.Set("WORKSPACE_TOKEN", "token")
	config.Set("DB.host", postgresContainer.Host)
	config.Set("DB.port", postgresContainer.Port)
	config.Set("DB.user", postgresContainer.User)
	config.Set("DB.name", postgresContainer.Database)
	config.Set("DB.password", postgresContainer.Password)
	config.Set("DEST_TRANSFORM_URL", transformerURL)

	config.Set("Warehouse.mode", "off")
	config.Set("DestinationDebugger.disableEventDeliveryStatusUploads", true)
	config.Set("SourceDebugger.disableEventUploads", true)
	config.Set("TransformationDebugger.disableTransformationStatusUploads", true)
	config.Set("JobsDB.backup.enabled", false)
	config.Set("JobsDB.migrateDSLoopSleepDuration", "60m")
	config.Set("archival.Enabled", false)
	config.Set("Reporting.syncer.enabled", false)
	config.Set("BatchRouter.mainLoopFreq", "1s")
	config.Set("BatchRouter.uploadFreq", "1s")
	config.Set("Gateway.webPort", strconv.Itoa(port))
	config.Set("RUDDER_TMPDIR", tmpDir)
	config.Set("recovery.storagePath", path.Join(tmpDir, "/recovery_data.json"))
	config.Set("recovery.enabled", false)
	config.Set("Profiler.Enabled", false)
	config.Set("Router.toAbortDestinationIDs", "destination-1")
	config.Set("Gateway.enableSuppressUserFeature", false)
	config.Set("Router.readSleep", "10ms")
	config.Set("Processor.pingerSleep", "10ms")
	config.Set("Processor.readLoopSleep", "10ms")
	config.Set("Processor.maxLoopSleep", "10ms")
	config.Set("AdminServer.enabled", false)

	if s.geolocationFeatureEnabled {
		// copy file to tmpDir/geolocation/geolite2City.mmdb
		if err := func() error {
			source, err := os.Open("./testdata/geolite2City.mmdb")
			if err != nil {
				return fmt.Errorf("opening geolocation db file: %w", err)
			}
			defer func() { _ = source.Close() }()
			if err := os.MkdirAll(tmpDir+"/geolocation", os.ModePerm); err != nil {
				return fmt.Errorf("creating folder: %w", err)
			}
			destination, err := os.Create(tmpDir + "/geolocation/geolite2City.mmdb")
			if err != nil {
				return fmt.Errorf("creating geolocation db file: %w", err)
			}
			defer func() { _ = destination.Close() }()
			if _, err = io.Copy(destination, source); err != nil {
				return fmt.Errorf("copying file: %w", err)
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	config.Set("GeoEnrichment.enabled", s.geolocationFeatureEnabled)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "TOKEN"})
	c := r.Run(ctx, []string{"proc-geolocation-test-rudder-server"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return
}

func (s *geolocationScenario) sendEvent(url, writeKey string) error {
	payload := []byte(fmt.Sprintf(`{"batch": [{
			"userId": %[1]q,
			"type": "identify",
			"context":
			{
				"traits":
				{
					"trait1": "new-val"
				},
				"ip": "%[2]s",
				"library":
				{
					"name": "http"
				}
			},
			"timestamp": "2020-02-02T00:23:09.544Z"
			}]}`,
		rand.String(10), s.contextIP))
	req, err := http.NewRequest("POST", url+"/v1/batch", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("X-Forwarded-For", s.clientIP)
	req.SetBasicAuth(writeKey, "password")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to send event to rudder server, status code: %d: %s", resp.StatusCode, string(b))
	}
	func() { kithttputil.CloseResponse(resp) }()

	return nil
}

func (s *geolocationScenario) requireJobsCount(t *testing.T, db *sql.DB, queue, state string, expectedCount int) {
	require.Eventually(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT count(*) FROM unionjobsdbmetadata('%s',10) WHERE job_state = '%s'", queue, state)).Scan(&jobsCount))
		t.Logf("%s %s Count: %d", queue, state, jobsCount)
		return jobsCount == expectedCount
	}, 20*time.Second, 1*time.Second, fmt.Sprintf("%d %s events should be in %s state", expectedCount, queue, state))
}
