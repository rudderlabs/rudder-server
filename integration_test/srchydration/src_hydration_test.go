package srchydration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"

	"github.com/rudderlabs/rudder-server/processor/isolation"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"

	"github.com/rudderlabs/rudder-go-kit/jsonparser"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	webhookutil "github.com/rudderlabs/rudder-server/testhelper/webhook"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/samber/lo"

	proctypes "github.com/rudderlabs/rudder-server/processor/types"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

const (
	writeKey1            = "writekey-1"
	sourceID1            = "source-1"
	writeKey2            = "writekey-2"
	sourceID2            = "source-2"
	writeKey3            = "writekey-3"
	sourceID3            = "source-3"
	workspaceID          = "workspace-1"
	srcDefName           = "FACEBOOK_LEAD_ADS_NATIVE"
	failingSourceDefName = "Source-Def-Name-2"
)

type reportRow struct {
	WorkspaceID    string
	InstanceID     string
	SourceID       string
	DestinationID  string
	InPU           string
	PU             string
	StatusCode     int
	Status         string
	Count          int64
	TerminalState  bool
	InitialState   bool
	SourceCategory string
	EventType      string
}

func TestSrcHydration(t *testing.T) {
	t.Run("with actual transformer", func(t *testing.T) {
		envKey := "FB_TEST_PAGE_ACCESS_TOKEN"
		if v, exists := os.LookupEnv(envKey); !exists || v == "" {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatalf("%s environment variable not set", envKey)
			}
			t.Skipf("Skipping %s as %s is not set", t.Name(), envKey)
		}
		pageAccessToken := os.Getenv("FB_TEST_PAGE_ACCESS_TOKEN")
		require.NotNil(t, pageAccessToken, "pageAccessToken is not set")
		internalSecret := json.RawMessage(fmt.Sprintf(`{"pageAccessToken": "%s"}`, pageAccessToken))

		webhook := webhookutil.NewRecorder()
		t.Cleanup(webhook.Close)
		webhookURL := webhook.Server.URL

		bcServer := prepareBackendConfigServer(t, webhookURL, internalSecret)
		t.Cleanup(bcServer.Close)

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		tr, err := transformer.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(t, ctx, gwPort, postgresContainer, bcServer.URL, tr.TransformerURL, t.TempDir(), nil)
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		numEvents := 6
		var eventsCount int

		err = sendEvents(numEvents, "identify", writeKey1, url)
		require.NoError(t, err)
		eventsCount += numEvents

		err = sendEvents(numEvents, "identify", writeKey2, url)
		require.NoError(t, err)
		eventsCount += numEvents

		requireJobsCount(t, ctx, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, ctx, postgresContainer.DB, "rt", jobsdb.Succeeded.State, eventsCount)

		require.Eventually(t, func() bool {
			return webhook.RequestsCount() == eventsCount
		}, 2*time.Minute, 10*time.Second, "unexpected number of events received, count of events: %d", webhook.RequestsCount())

		expectedReports := append(prepareExpectedReports(t, sourceID1, false, numEvents), prepareExpectedReports(t, sourceID2, false, numEvents)...)
		requireReports(t, ctx, postgresContainer.DB, expectedReports)
	})

	t.Run("with test transformer", func(t *testing.T) {
		tests := []struct {
			name                   string
			procIsolation          string
			failOnHydrationFailure bool
		}{
			{
				name:                   "procIsolation=source, failOnHydrationFailure=false",
				procIsolation:          string(isolation.ModeSource),
				failOnHydrationFailure: false,
			},
			{
				name:                   "procIsolation=workspace, failOnHydrationFailure=false",
				procIsolation:          string(isolation.ModeWorkspace),
				failOnHydrationFailure: false,
			},
			{
				name:                   "procIsolation=none, failOnHydrationFailure=false",
				procIsolation:          string(isolation.ModeNone),
				failOnHydrationFailure: false,
			},
			{
				name:                   "procIsolation=source, failOnHydrationFailure=true",
				procIsolation:          string(isolation.ModeSource),
				failOnHydrationFailure: true,
			},
			{
				name:                   "procIsolation=workspace, failOnHydrationFailure=true",
				procIsolation:          string(isolation.ModeWorkspace),
				failOnHydrationFailure: true,
			},
			{
				name:                   "procIsolation=none, failOnHydrationFailure=true",
				procIsolation:          string(isolation.ModeNone),
				failOnHydrationFailure: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.isolationMode"), tt.procIsolation)
				t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.SourceHydration.failOnError"),
					strconv.FormatBool(tt.failOnHydrationFailure))

				webhook := webhookutil.NewRecorder()
				t.Cleanup(webhook.Close)
				webhookURL := webhook.Server.URL

				bcServer := prepareBackendConfigServer(t, webhookURL, nil)
				t.Cleanup(bcServer.Close)

				trServer := transformertest.NewBuilder().
					// we are only adding a route for srcDefName, so that it will be called only for srcDefName
					// for other source definitions we will return a 404
					WithSrcHydrationHandler(strings.ToLower(srcDefName), func(request proctypes.SrcHydrationRequest) (proctypes.SrcHydrationResponse, error) {
						lo.ForEach(request.Batch, func(event proctypes.SrcHydrationEvent, index int) {
							event.Event["source_hydration_test"] = "success"
						})
						return proctypes.SrcHydrationResponse{
							Batch: request.Batch,
						}, nil
					}).
					WithSrcHydrationHandler(strings.ToLower(failingSourceDefName), func(request proctypes.SrcHydrationRequest) (proctypes.SrcHydrationResponse, error) {
						return proctypes.SrcHydrationResponse{}, errors.New("failed to hydrate, not hydration source")
					}).
					WithDestTransformHandler(
						"WEBHOOK",
						transformertest.RESTJSONDestTransformerHandler(http.MethodPost, webhookURL),
					).
					Build()
				t.Cleanup(trServer.Close)

				pool, err := dockertest.NewPool("")
				require.NoError(t, err)

				postgresContainer, err := postgres.Setup(pool, t)
				require.NoError(t, err)

				minioResource, err := minio.Setup(pool, t)
				require.NoError(t, err)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				gwPort, err := kithelper.GetFreePort()
				require.NoError(t, err)

				wg, ctx := errgroup.WithContext(ctx)
				wg.Go(func() error {
					err := runRudderServer(t, ctx, gwPort, postgresContainer, bcServer.URL, trServer.URL, t.TempDir(), minioResource)
					if err != nil {
						t.Logf("rudder-server exited with error: %v", err)
					}
					return err
				})

				url := fmt.Sprintf("http://localhost:%d", gwPort)
				health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

				numEvents := 4
				var successfulEventCount int
				var totalEventCount int

				err = sendEvents(numEvents, "identify", writeKey1, url)
				require.NoError(t, err)
				successfulEventCount += numEvents
				totalEventCount += numEvents

				err = sendEvents(numEvents, "identify", writeKey2, url)
				require.NoError(t, err)
				successfulEventCount += numEvents
				totalEventCount += numEvents

				if tt.failOnHydrationFailure {
					err = sendEvents(numEvents, "identify", writeKey3, url)
					require.NoError(t, err)
					totalEventCount += numEvents
				}

				requireJobsCount(t, ctx, postgresContainer.DB, "gw", jobsdb.Succeeded.State, totalEventCount)

				requireJobsCount(t, ctx, postgresContainer.DB, "rt", jobsdb.Succeeded.State, successfulEventCount)

				require.Eventuallyf(t, func() bool {
					return webhook.RequestsCount() == 8
				}, 1*time.Minute, 5*time.Second, "unexpected number of events received, count of events: %d", webhook.RequestsCount())

				lo.ForEach(webhook.Requests(), func(req *http.Request, _ int) {
					reqBody, err := io.ReadAll(req.Body)
					require.NoError(t, err)
					require.Equal(t, jsonparser.GetStringOrEmpty(reqBody, "source_hydration_test"), "success")
				})

				expectedReports := append(
					prepareExpectedReports(t, sourceID1, false, numEvents),
					prepareExpectedReports(t, sourceID2, false, numEvents)...,
				)
				if tt.failOnHydrationFailure {
					expectedReports = append(expectedReports, prepareSrcHydrationFailedReports(t, sourceID3, numEvents)...)
					expectedReports = append(expectedReports, prepareExpectedReports(t, sourceID3, true, numEvents)...)
					requireJobsCount(t, ctx, postgresContainer.DB, "err_idx", jobsdb.Succeeded.State, numEvents)
					requireMessagesCount(t, ctx, minioResource, numEvents, []lo.Tuple2[string, string]{
						{A: "source_id", B: sourceID3},
						{A: "failed_stage", B: "source_hydration"},
						{A: "event_type", B: "identify"},
					}...)
				}
				requireReports(t, ctx, postgresContainer.DB, expectedReports)

				cancel()
				require.NoError(t, wg.Wait())
			})
		}
	})
}

func prepareSrcHydrationFailedReports(t *testing.T, sourceID string, numEvents int) []reportRow {
	t.Helper()
	return []reportRow{
		{
			WorkspaceID:    workspaceID,
			InstanceID:     "1",
			SourceID:       sourceID,
			DestinationID:  "",
			InPU:           "destination_filter",
			PU:             "source_hydration",
			StatusCode:     500,
			Status:         "aborted",
			Count:          int64(numEvents),
			TerminalState:  false,
			InitialState:   false,
			SourceCategory: "webhook",
			EventType:      "identify",
		},
	}
}

func prepareExpectedReports(t *testing.T, sourceId string, gwOnly bool, numEvents int) []reportRow {
	t.Helper()
	if gwOnly {
		return []reportRow{
			{
				WorkspaceID:    workspaceID,
				InstanceID:     "1",
				SourceID:       sourceId,
				DestinationID:  "",
				InPU:           "",
				PU:             "gateway",
				StatusCode:     0,
				Status:         "succeeded",
				Count:          int64(numEvents),
				TerminalState:  false,
				InitialState:   true,
				SourceCategory: "webhook",
				EventType:      "identify",
			},
		}
	}
	return []reportRow{
		{
			WorkspaceID:    workspaceID,
			InstanceID:     "1",
			SourceID:       sourceId,
			DestinationID:  "",
			InPU:           "",
			PU:             "gateway",
			StatusCode:     0,
			Status:         "succeeded",
			Count:          int64(numEvents),
			TerminalState:  false,
			InitialState:   true,
			SourceCategory: "webhook",
			EventType:      "identify",
		},
		{
			WorkspaceID:    workspaceID,
			InstanceID:     "1",
			SourceID:       sourceId,
			DestinationID:  "destination-1",
			InPU:           "source_hydration",
			PU:             "event_filter",
			StatusCode:     200,
			Status:         "succeeded",
			Count:          int64(numEvents),
			TerminalState:  false,
			InitialState:   false,
			SourceCategory: "webhook",
			EventType:      "identify",
		},
		{
			WorkspaceID:    workspaceID,
			InstanceID:     "1",
			SourceID:       sourceId,
			DestinationID:  "destination-1",
			InPU:           "event_filter",
			PU:             "dest_transformer",
			StatusCode:     200,
			Status:         "succeeded",
			Count:          int64(numEvents),
			TerminalState:  false,
			InitialState:   false,
			SourceCategory: "webhook",
			EventType:      "identify",
		},
		{
			WorkspaceID:    workspaceID,
			InstanceID:     "1",
			SourceID:       sourceId,
			DestinationID:  "destination-1",
			InPU:           "dest_transformer",
			PU:             "router",
			StatusCode:     200,
			Status:         "succeeded",
			Count:          int64(numEvents),
			TerminalState:  true,
			InitialState:   false,
			SourceCategory: "webhook",
			EventType:      "identify",
		},
	}
}

func requireReports(t *testing.T, ctx context.Context, db *sql.DB, expectedReports []reportRow) {
	t.Helper()
	query := `
					SELECT
					  workspace_id, instance_id, source_id, destination_id,
					  in_pu, pu, status_code, status, count,
					  terminal_state, initial_state, source_category, event_type
					FROM
					  reports
					ORDER BY
					  source_id, id;
				`
	require.Eventuallyf(t, func() bool {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			t.Logf("error querying reports: %v", err)
			return false
		}
		defer rows.Close()

		actualReports := make([]reportRow, 0)
		for rows.Next() {
			var r reportRow
			var destID sql.NullString
			err := rows.Scan(
				&r.WorkspaceID, &r.InstanceID, &r.SourceID, &destID,
				&r.InPU, &r.PU, &r.StatusCode, &r.Status, &r.Count,
				&r.TerminalState, &r.InitialState, &r.SourceCategory, &r.EventType,
			)
			if err != nil {
				t.Logf("error scanning report row: %v", err)
				return false
			}

			r.DestinationID = destID.String
			actualReports = append(actualReports, r)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, expectedReports, actualReports)
		return true
	}, 1*time.Minute, 5*time.Second, "reporting data mismatch")
}

func prepareBackendConfigServer(t *testing.T, webhookURL string, internalSecret json.RawMessage) *httptest.Server {
	t.Helper()
	return backendconfigtest.NewBuilder().
		WithWorkspaceConfig(backendconfigtest.NewConfigBuilder().
			WithWorkspaceID(workspaceID).
			WithSource(
				backendconfigtest.NewSourceBuilder().
					WithWorkspaceID(workspaceID).
					WithID(sourceID1).
					WithWriteKey(writeKey1).
					WithSourceCategory("webhook").
					WithSourceType(srcDefName).
					WithSourceDefOptions(backendconfig.SourceDefinitionOptions{
						Hydration: struct {
							Enabled bool
						}{Enabled: true},
					}).
					WithInternalSecrets(internalSecret).
					WithConnection(
						backendconfigtest.NewDestinationBuilder("WEBHOOK").
							WithID("destination-1").
							WithConfigOption("webhookMethod", "POST").
							WithConfigOption("webhookUrl", webhookURL).
							Build()).
					Build()).
			WithSource(
				backendconfigtest.NewSourceBuilder().
					WithWorkspaceID(workspaceID).
					WithID(sourceID2).
					WithWriteKey(writeKey2).
					WithSourceCategory("webhook").
					WithSourceType(srcDefName).
					WithSourceDefOptions(backendconfig.SourceDefinitionOptions{
						Hydration: struct {
							Enabled bool
						}{Enabled: true},
					}).
					WithInternalSecrets(internalSecret).
					WithConnection(
						backendconfigtest.NewDestinationBuilder("WEBHOOK").
							WithID("destination-1").
							WithConfigOption("webhookMethod", "POST").
							WithConfigOption("webhookUrl", webhookURL).
							Build()).
					Build()).
			WithSource(
				backendconfigtest.NewSourceBuilder().
					WithWorkspaceID(workspaceID).
					WithID(sourceID3).
					WithWriteKey(writeKey3).
					WithSourceCategory("webhook").
					WithSourceType(failingSourceDefName).
					WithSourceDefOptions(backendconfig.SourceDefinitionOptions{
						Hydration: struct {
							Enabled bool
						}{Enabled: true},
					}).
					WithConnection(
						backendconfigtest.NewDestinationBuilder("WEBHOOK").
							WithID("destination-1").
							WithConfigOption("webhookMethod", "POST").
							WithConfigOption("webhookUrl", webhookURL).
							Build()).
					Build()).
			Build()).
		Build()
}

func runRudderServer(t testing.TB, ctx context.Context, port int, postgresContainer *postgres.Resource, cbURL, transformerURL, tmpDir string, minioResource *minio.Resource) (err error) {
	config.Reset()
	t.Setenv("CONFIG_BACKEND_URL", cbURL)
	t.Setenv("WORKSPACE_TOKEN", "token")
	t.Setenv("DEST_TRANSFORM_URL", transformerURL)

	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.host"), postgresContainer.Host)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.port"), postgresContainer.Port)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.user"), postgresContainer.User)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.name"), postgresContainer.Database)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.password"), postgresContainer.Password)

	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Warehouse.mode"), "off")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DestinationDebugger.disableEventDeliveryStatusUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "SourceDebugger.disableEventUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "TransformationDebugger.disableTransformationStatusUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.backup.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.migrateDSLoopSleepDuration"), "60m")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "archival.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.syncer.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BatchRouter.pingFrequency"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BatchRouter.uploadFreq"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.webPort"), strconv.Itoa(port))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RUDDER_TMPDIR"), os.TempDir())
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.storagePath"), path.Join(tmpDir, "/recovery_data.json"))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Profiler.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.enableSuppressUserFeature"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.archiveInPreProcess"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Processor.SourceHydration.maxRetry"), "2")
	if minioResource != nil {
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.errorIndexReporting.enabled"), "true")
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.errorIndexReporting.SleepDuration"), "1s")
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.errorIndexReporting.minWorkerSleep"), "1s")
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.errorIndexReporting.uploadFrequency"), "1s")
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "ErrorIndex.storage.Bucket"), minioResource.BucketName)
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "ErrorIndex.storage.Endpoint"), fmt.Sprintf("http://%s", minioResource.Endpoint))
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "ErrorIndex.storage.AccessKey"), minioResource.AccessKeyID)
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "ErrorIndex.storage.SecretAccessKey"), minioResource.AccessKeySecret)
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "ErrorIndex.storage.S3ForcePathStyle"), "true")
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "ErrorIndex.storage.DisableSSL"), "true")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, []string{"src-hydration"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return err
}

// nolint: unparam, bodyclose
func sendEvents(
	num int,
	eventType, writeKey,
	url string,
) error {
	for i := 0; i < num; i++ {
		payload := []byte(fmt.Sprintf(`
			{
			  "batch": [
				{
				  "anonymousId": "1201391398519677",
				  "type": %[1]q,
				  "context": {
					"traits": {
					  "trait1": "new-val"
					},
					"ip": "14.5.67.21",
					"library": {
					  "name": "http"
					}
				  },
				  "timestamp": "2020-02-02T00:23:09.544Z"
				}
			  ]
			}`,
			eventType,
		))
		req, err := http.NewRequest(http.MethodPost, url+"/v1/batch", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.SetBasicAuth(writeKey, "password")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to send event to rudder server, status code: %d: %s", resp.StatusCode, string(b))
		}
		kithttputil.CloseResponse(resp)
	}
	return nil
}

// nolint: unparam
func requireJobsCount(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	queue, state string,
	expectedCount int,
) {
	t.Helper()

	query := fmt.Sprintf(`
		SELECT
		  count(*)
		FROM
		  unionjobsdbmetadata('%s', 1)
		WHERE
		  job_state = '%s';
	`,
		queue,
		state,
	)
	require.Eventuallyf(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&jobsCount))
		t.Logf("%s %sJobCount: %d", queue, state, jobsCount)
		return jobsCount == expectedCount
	},
		30*time.Second,
		1*time.Second,
		"%d %s events should be in %s state", expectedCount, queue, state,
	)
}

// nolint: unparam
func requireMessagesCount(
	t *testing.T,
	ctx context.Context,
	mr *minio.Resource,
	expectedCount int,
	filters ...lo.Tuple2[string, string],
) {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	_, err = db.Exec(fmt.Sprintf(`INSTALL parquet; LOAD parquet; INSTALL httpfs; LOAD httpfs;SET s3_region='%s';SET s3_endpoint='%s';SET s3_access_key_id='%s';SET s3_secret_access_key='%s';SET s3_use_ssl= false;SET s3_url_style='path';`,
		mr.Region,
		mr.Endpoint,
		mr.AccessKeyID,
		mr.AccessKeySecret,
	))
	require.NoError(t, err)

	query := fmt.Sprintf("SELECT count(*) FROM read_parquet('%s') WHERE 1 = 1", fmt.Sprintf("s3://%s/**/**/**/*.parquet", mr.BucketName))
	query += strings.Join(lo.Map(filters, func(t lo.Tuple2[string, string], _ int) string {
		return fmt.Sprintf(" AND %s = '%s'", t.A, t.B)
	}), "")

	require.Eventually(t, func() bool {
		var messagesCount int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&messagesCount))
		t.Logf("messagesCount: %d", messagesCount)
		return messagesCount == expectedCount
	},
		10*time.Second,
		1*time.Second,
		fmt.Sprintf("%d messages should be in the bucket", expectedCount),
	)
}
