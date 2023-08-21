package redshift_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/compose-test/compose"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"

	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"

	"github.com/lib/pq"
	"github.com/ory/dockertest/v3"

	"github.com/rudderlabs/compose-test/testcompose"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type testCredentials struct {
	Host        string `json:"host"`
	Port        string `json:"port"`
	UserName    string `json:"userName"`
	Password    string `json:"password"`
	DbName      string `json:"dbName"`
	BucketName  string `json:"bucketName"`
	AccessKeyID string `json:"accessKeyID"`
	AccessKey   string `json:"accessKey"`
}

const testKey = "REDSHIFT_INTEGRATION_TEST_CREDENTIALS"

func rsTestCredentials() (*testCredentials, error) {
	cred, exists := os.LookupEnv(testKey)
	if !exists {
		return nil, errors.New("redshift test credentials not found")
	}

	var credentials testCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal redshift test credentials: %w", err)
	}
	return &credentials, nil
}

func testCredentialsAvailable() bool {
	_, err := rsTestCredentials()
	return err == nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if !testCredentialsAvailable() {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testKey)
	}

	c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml"}))
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()

	jobsDBPort := c.Port("jobsDb", 5432)

	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()
	sourcesSourceID := warehouseutils.RandHex()
	sourcesDestinationID := warehouseutils.RandHex()
	sourcesWriteKey := warehouseutils.RandHex()

	destType := warehouseutils.RS

	namespace := testhelper.RandSchema(destType)
	sourcesNamespace := testhelper.RandSchema(destType)

	rsTestCredentials, err := rsTestCredentials()
	require.NoError(t, err)

	templateConfigurations := map[string]any{
		"workspaceID":          workspaceID,
		"sourceID":             sourceID,
		"destinationID":        destinationID,
		"writeKey":             writeKey,
		"sourcesSourceID":      sourcesSourceID,
		"sourcesDestinationID": sourcesDestinationID,
		"sourcesWriteKey":      sourcesWriteKey,
		"host":                 rsTestCredentials.Host,
		"port":                 rsTestCredentials.Port,
		"user":                 rsTestCredentials.UserName,
		"password":             rsTestCredentials.Password,
		"database":             rsTestCredentials.DbName,
		"bucketName":           rsTestCredentials.BucketName,
		"accessKeyID":          rsTestCredentials.AccessKeyID,
		"accessKey":            rsTestCredentials.AccessKey,
		"namespace":            namespace,
		"sourcesNamespace":     sourcesNamespace,
	}
	workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	testhelper.EnhanceWithDefaultEnvs(t)
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_ENABLE_DELETE_BY_JOBS", "true")
	t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_DEDUP_WINDOW", "true")
	t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_DEDUP_WINDOW_IN_HOURS", "5")
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_SLOW_QUERY_THRESHOLD", "0s")

	svcDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"redshift-integration-test"})

		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		rsTestCredentials.UserName,
		rsTestCredentials.Password,
		rsTestCredentials.Host,
		rsTestCredentials.Port,
		rsTestCredentials.DbName,
	)

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())

	t.Run("Event flow", func(t *testing.T) {
		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		testcase := []struct {
			name                  string
			writeKey              string
			schema                string
			sourceID              string
			destinationID         string
			tables                []string
			stagingFilesEventsMap testhelper.EventsCountMap
			loadFilesEventsMap    testhelper.EventsCountMap
			tableUploadsEventsMap testhelper.EventsCountMap
			warehouseEventsMap    testhelper.EventsCountMap
			asyncJob              bool
			stagingFilePrefix     string
		}{
			{
				name:              "Upload Job",
				writeKey:          writeKey,
				schema:            namespace,
				tables:            []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				sourceID:          sourceID,
				destinationID:     destinationID,
				stagingFilePrefix: "testdata/upload-job",
			},
			{
				name:                  "Async Job",
				writeKey:              sourcesWriteKey,
				schema:                sourcesNamespace,
				tables:                []string{"tracks", "google_sheet"},
				sourceID:              sourcesSourceID,
				destinationID:         sourcesDestinationID,
				stagingFilesEventsMap: testhelper.SourcesStagingFilesEventsMap(),
				loadFilesEventsMap:    testhelper.SourcesLoadFilesEventsMap(),
				tableUploadsEventsMap: testhelper.SourcesTableUploadsEventsMap(),
				warehouseEventsMap:    testhelper.SourcesWarehouseEventsMap(),
				asyncJob:              true,
				stagingFilePrefix:     "testdata/sources-job",
			},
		}

		for _, tc := range testcase {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				t.Cleanup(func() {
					require.Eventually(t, func() bool {
						if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, tc.schema)); err != nil {
							t.Logf("error deleting schema: %v", err)
							return false
						}
						return true
					},
						time.Minute,
						time.Second,
					)
				})

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketName":       rsTestCredentials.BucketName,
					"accessKeyID":      rsTestCredentials.AccessKeyID,
					"accessKey":        rsTestCredentials.AccessKey,
					"enableSSE":        false,
					"useRudderStorage": false,
				}

				t.Log("verifying test case 1")
				ts1 := testhelper.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-1.json",
					UserID:                testhelper.GetUserId(destType),
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := testhelper.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					AsyncJob:              tc.asyncJob,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-1.json",
					UserID:                testhelper.GetUserId(destType),
				}
				if tc.asyncJob {
					ts2.UserID = ts1.UserID
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validation", func(t *testing.T) {
		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				if _, err := db.Exec(fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, namespace)); err != nil {
					t.Logf("error deleting schema: %v", err)
					return false
				}
				return true
			},
				time.Minute,
				time.Second,
			)
		})

		dest := backendconfig.DestinationT{
			ID: destinationID,
			Config: map[string]interface{}{
				"host":             rsTestCredentials.Host,
				"port":             rsTestCredentials.Port,
				"user":             rsTestCredentials.UserName,
				"password":         rsTestCredentials.Password,
				"database":         rsTestCredentials.DbName,
				"bucketName":       rsTestCredentials.BucketName,
				"accessKeyID":      rsTestCredentials.AccessKeyID,
				"accessKey":        rsTestCredentials.AccessKey,
				"namespace":        namespace,
				"syncFrequency":    "30",
				"enableSSE":        false,
				"useRudderStorage": false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1UVZiJF7OgLaiIY2Jts8XOQE3M6",
				Name:        "RS",
				DisplayName: "Redshift",
			},
			Name:       "redshift-demo",
			Enabled:    true,
			RevisionID: "29HgOWobrn0RYZLpaSwPIbN2987",
		}
		testhelper.VerifyConfigurationTest(t, dest)
	})
}

func TestCheckAndIgnoreColumnAlreadyExistError(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			expected: true,
		},
		{
			name: "column already exists error",
			err: &pq.Error{
				Code: "42701",
			},
			expected: true,
		},
		{
			name:     "other error",
			err:      errors.New("other error"),
			expected: false,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, redshift.CheckAndIgnoreColumnAlreadyExistError(tc.err))
		})
	}
}

func TestRedshift_AlterColumn(t *testing.T) {
	var (
		bigString      = strings.Repeat("a", 1024)
		smallString    = strings.Repeat("a", 510)
		testNamespace  = "test_namespace"
		testTable      = "test_table"
		testColumn     = "test_column"
		testColumnType = "text"
	)

	testCases := []struct {
		name       string
		createView bool
	}{
		{
			name: "success",
		},
		{
			name:       "view/rule",
			createView: true,
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			pgResource, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)

			t.Log("db:", pgResource.DBDsn)

			rs := redshift.New(config.Default, logger.NOP, stats.Default)

			rs.DB = sqlmiddleware.New(pgResource.DB)
			rs.Namespace = testNamespace

			_, err = rs.DB.Exec(
				fmt.Sprintf("CREATE SCHEMA %s;",
					testNamespace,
				),
			)
			require.NoError(t, err)

			_, err = rs.DB.Exec(
				fmt.Sprintf("CREATE TABLE %q.%q (%s VARCHAR(512));",
					testNamespace,
					testTable,
					testColumn,
				),
			)
			require.NoError(t, err)

			if tc.createView {
				_, err = rs.DB.Exec(
					fmt.Sprintf("CREATE VIEW %[1]q.%[2]q AS SELECT * FROM %[1]q.%[3]q;",
						testNamespace,
						fmt.Sprintf("%s_view", testTable),
						testTable,
					),
				)
				require.NoError(t, err)
			}

			_, err = rs.DB.Exec(
				fmt.Sprintf("INSERT INTO %q.%q (%s) VALUES ('%s');",
					testNamespace,
					testTable,
					testColumn,
					smallString,
				),
			)
			require.NoError(t, err)

			_, err = rs.DB.Exec(
				fmt.Sprintf("INSERT INTO %q.%q (%s) VALUES ('%s');",
					testNamespace,
					testTable,
					testColumn,
					bigString,
				),
			)
			require.ErrorContains(t, err, errors.New("pq: value too long for type character varying(512)").Error())

			res, err := rs.AlterColumn(ctx, testTable, testColumn, testColumnType)
			require.NoError(t, err)

			if tc.createView {
				require.True(t, res.IsDependent)
				require.NotEmpty(t, res.Query)
			}

			_, err = rs.DB.Exec(
				fmt.Sprintf("INSERT INTO %q.%q (%s) VALUES ('%s');",
					testNamespace,
					testTable,
					testColumn,
					bigString,
				),
			)
			require.NoError(t, err)
		})
	}
}
