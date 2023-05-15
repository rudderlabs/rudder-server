package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/tunnelling"

	"github.com/rudderlabs/compose-test/testcompose"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	c := testcompose.New(t, "testdata/docker-compose.yml", "../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.minio.yml")

	t.Cleanup(func() {
		c.Stop(context.Background())
	})
	c.Start(context.Background())

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	encoding.Init()

	jobsDBPort := c.Port("jobsDb", 5432)
	minioPort := c.Port("minio", 9000)
	postgresPort := c.Port("postgres", 5432)
	sshPort := c.Port("ssh-server", 2222)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	workspaceID := warehouseutils.RandHex()
	sourceID := warehouseutils.RandHex()
	destinationID := warehouseutils.RandHex()
	writeKey := warehouseutils.RandHex()
	sourcesSourceID := warehouseutils.RandHex()
	sourcesDestinationID := warehouseutils.RandHex()
	sourcesWriteKey := warehouseutils.RandHex()
	tunnelledWriteKey := warehouseutils.RandHex()
	tunnelledSourceID := warehouseutils.RandHex()
	tunnelledDestinationID := warehouseutils.RandHex()

	destType := warehouseutils.POSTGRES

	namespace := testhelper.RandSchema(destType)
	sourcesNamespace := testhelper.RandSchema(destType)
	tunnelledNamespace := testhelper.RandSchema(destType)

	host := "localhost"
	database := "rudderdb"
	user := "rudder"
	password := "rudder-password"

	tunnelledHost := "db-private-postgres"
	tunnelledDatabase := "postgres"
	tunnelledPassword := "postgres"
	tunnelledUser := "postgres"
	tunnelledPort := "5432"

	tunnelledSSHUser := "rudderstack"
	tunnelledSSHHost := "localhost"
	tunnelledPrivateKey := "-----BEGIN OPENSSH PRIVATE KEY-----\\n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\\nbG9jYWwBAgMEBQYH\\n-----END OPENSSH PRIVATE KEY-----"

	bucketName := "testbucket"
	accessKeyID := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"

	minioEndpoint := fmt.Sprintf("localhost:%d", minioPort)

	templateConfigurations := map[string]any{
		"workspaceID":            workspaceID,
		"sourceID":               sourceID,
		"destinationID":          destinationID,
		"writeKey":               writeKey,
		"sourcesSourceID":        sourcesSourceID,
		"sourcesDestinationID":   sourcesDestinationID,
		"sourcesWriteKey":        sourcesWriteKey,
		"tunnelledWriteKey":      tunnelledWriteKey,
		"tunnelledSourceID":      tunnelledSourceID,
		"tunnelledDestinationID": tunnelledDestinationID,
		"host":                   host,
		"database":               database,
		"user":                   user,
		"password":               password,
		"port":                   strconv.Itoa(postgresPort),
		"namespace":              namespace,
		"sourcesNamespace":       sourcesNamespace,
		"tunnelledNamespace":     tunnelledNamespace,
		"tunnelledSSHUser":       tunnelledSSHUser,
		"tunnelledSSHPort":       strconv.Itoa(sshPort),
		"tunnelledSSHHost":       tunnelledSSHHost,
		"tunnelledPrivateKey":    tunnelledPrivateKey,
		"tunnelledHost":          tunnelledHost,
		"tunnelledDatabase":      tunnelledDatabase,
		"tunnelledPort":          tunnelledPort,
		"tunnelledUser":          tunnelledUser,
		"tunnelledPassword":      tunnelledPassword,
		"bucketName":             bucketName,
		"accessKeyID":            accessKeyID,
		"secretAccessKey":        secretAccessKey,
		"endPoint":               minioEndpoint,
	}
	workspaceConfigPath := workspaceConfig.CreateTempFile(t, "testdata/template.json", templateConfigurations)

	testhelper.EnhanceWithDefaultEnvs(t)
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("MINIO_ACCESS_KEY_ID", accessKeyID)
	t.Setenv("MINIO_SECRET_ACCESS_KEY", secretAccessKey)
	t.Setenv("MINIO_MINIO_ENDPOINT", minioEndpoint)
	t.Setenv("MINIO_SSL", "false")
	t.Setenv("RSERVER_WAREHOUSE_POSTGRES_MAX_PARALLEL_LOADS", "8")
	t.Setenv("RSERVER_WAREHOUSE_POSTGRES_SKIP_COMPUTING_USER_LATEST_TRAITS_WORKSPACE_IDS", workspaceID)
	t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_SQLSTATEMENT_EXECUTION_PLAN_WORKSPACE_IDS", workspaceID)
	t.Setenv("RSERVER_WAREHOUSE_POSTGRES_ENABLE_DELETE_BY_JOBS", "true")
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)
	t.Setenv("RSERVER_WAREHOUSE_POSTGRES_SLOW_QUERY_THRESHOLD", "0s")

	svcDone := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		r := runner.New(runner.ReleaseInfo{})
		_ = r.Run(ctx, []string{"postgres-integration-test"})

		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t, serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint")

	t.Run("Events flow", func(t *testing.T) {
		dsn := fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s?sslmode=disable",
			"rudder", "rudder-password", "localhost", strconv.Itoa(postgresPort), "rudderdb",
		)
		db, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		require.NoError(t, db.Ping())

		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		testCases := []struct {
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

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider":   "MINIO",
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
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
					StagingFilePath:       tc.stagingFilePrefix + ".staging-2.json",
					UserID:                testhelper.GetUserId(destType),
				}
				if tc.asyncJob {
					ts2.UserID = ts1.UserID
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Events flow with ssh tunnel", func(t *testing.T) {
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			tunnelledUser,
			tunnelledPassword,
			tunnelledHost,
			tunnelledPort,
			tunnelledDatabase,
		)
		tunnelInfo := &tunnelling.TunnelInfo{
			Config: map[string]interface{}{
				"sshUser":       tunnelledSSHUser,
				"sshPort":       strconv.Itoa(sshPort),
				"sshHost":       tunnelledSSHHost,
				"sshPrivateKey": strings.ReplaceAll(tunnelledPrivateKey, "\\n", "\n"),
			},
		}

		db, err := tunnelling.SQLConnectThroughTunnel(dsn, tunnelInfo.Config)
		require.NoError(t, err)
		require.NoError(t, db.Ping())

		jobsDB := testhelper.JobsDB(t, jobsDBPort)

		testcases := []struct {
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
			stagingFilePrefix     string
		}{
			{
				name:              "upload job through ssh tunnelling",
				writeKey:          tunnelledWriteKey,
				schema:            tunnelledNamespace,
				tables:            []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				sourceID:          tunnelledSourceID,
				destinationID:     tunnelledDestinationID,
				stagingFilePrefix: "testdata/upload-ssh-job",
			},
		}

		for _, tc := range testcases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				sqlClient := &client.Client{
					SQL:  db,
					Type: client.SQLClient,
				}

				conf := map[string]interface{}{
					"bucketProvider":   "MINIO",
					"bucketName":       bucketName,
					"accessKeyID":      accessKeyID,
					"secretAccessKey":  secretAccessKey,
					"useSSL":           false,
					"endPoint":         minioEndpoint,
					"useRudderStorage": false,
				}

				t.Log("verifying test case 1")
				ts1 := testhelper.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					Tables:                tc.tables,
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
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					Tables:                tc.tables,
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
					StagingFilePath:       tc.stagingFilePrefix + ".staging-2.json",
					UserID:                testhelper.GetUserId(destType),
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		dest := backendconfig.DestinationT{
			ID: destinationID,
			Config: map[string]interface{}{
				"host":             host,
				"database":         database,
				"user":             user,
				"password":         password,
				"port":             strconv.Itoa(postgresPort),
				"sslMode":          "disable",
				"namespace":        "",
				"bucketProvider":   "MINIO",
				"bucketName":       bucketName,
				"accessKeyID":      accessKeyID,
				"secretAccessKey":  secretAccessKey,
				"useSSL":           false,
				"endPoint":         minioEndpoint,
				"syncFrequency":    "30",
				"useRudderStorage": false,
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1bJ4YC7INdkvBTzotNh0zta5jDm",
				Name:        "POSTGRES",
				DisplayName: "Postgres",
			},
			Name:       "postgres-demo",
			Enabled:    true,
			RevisionID: "29eeuu9kywWsRAybaXcxcnTVEl8",
		}
		testhelper.VerifyConfigurationTest(t, dest)
	})
}
