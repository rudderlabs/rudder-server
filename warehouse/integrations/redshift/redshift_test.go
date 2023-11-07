package redshift_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	th "github.com/rudderlabs/rudder-server/testhelper"

	"github.com/golang/mock/gomock"
	"github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
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

	namespace := whth.RandSchema(destType)
	sourcesNamespace := whth.RandSchema(destType)

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

	whth.EnhanceWithDefaultEnvs(t)
	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)

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
		t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_MAX_PARALLEL_LOADS", "8")
		t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_ENABLE_DELETE_BY_JOBS", "true")
		t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_SLOW_QUERY_THRESHOLD", "0s")
		t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_DEDUP_WINDOW", "true")
		t.Setenv("RSERVER_WAREHOUSE_REDSHIFT_DEDUP_WINDOW_IN_HOURS", "5")

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testcase := []struct {
			name                  string
			writeKey              string
			schema                string
			sourceID              string
			destinationID         string
			tables                []string
			stagingFilesEventsMap whth.EventsCountMap
			loadFilesEventsMap    whth.EventsCountMap
			tableUploadsEventsMap whth.EventsCountMap
			warehouseEventsMap    whth.EventsCountMap
			sourceJob             bool
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
				name:                  "Source Job",
				writeKey:              sourcesWriteKey,
				schema:                sourcesNamespace,
				tables:                []string{"tracks", "google_sheet"},
				sourceID:              sourcesSourceID,
				destinationID:         sourcesDestinationID,
				stagingFilesEventsMap: whth.SourcesStagingFilesEventsMap(),
				loadFilesEventsMap:    whth.SourcesLoadFilesEventsMap(),
				tableUploadsEventsMap: whth.SourcesTableUploadsEventsMap(),
				warehouseEventsMap:    whth.SourcesWarehouseEventsMap(),
				sourceJob:             true,
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
				ts1 := whth.TestConfig{
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
					UserID:                whth.GetUserId(destType),
				}
				ts1.VerifyEvents(t)

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:              tc.writeKey,
					Schema:                tc.schema,
					Tables:                tc.tables,
					SourceID:              tc.sourceID,
					DestinationID:         tc.destinationID,
					StagingFilesEventsMap: tc.stagingFilesEventsMap,
					LoadFilesEventsMap:    tc.loadFilesEventsMap,
					TableUploadsEventsMap: tc.tableUploadsEventsMap,
					WarehouseEventsMap:    tc.warehouseEventsMap,
					SourceJob:             tc.sourceJob,
					Config:                conf,
					WorkspaceID:           workspaceID,
					DestinationType:       destType,
					JobsDB:                jobsDB,
					HTTPPort:              httpPort,
					Client:                sqlClient,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StagingFilePath:       tc.stagingFilePrefix + ".staging-1.json",
					UserID:                whth.GetUserId(destType),
				}
				if tc.sourceJob {
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
		whth.VerifyConfigurationTest(t, dest)
	})

	t.Run("Load Table", func(t *testing.T) {
		const (
			sourceID      = "test_source_id"
			destinationID = "test_destination_id"
			workspaceID   = "test_workspace_id"
		)

		namespace := whth.RandSchema(destType)

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

		schemaInUpload := model.TableSchema{
			"test_bool":     "boolean",
			"test_datetime": "datetime",
			"test_float":    "float",
			"test_int":      "int",
			"test_string":   "string",
			"id":            "string",
			"received_at":   "datetime",
		}
		schemaInWarehouse := model.TableSchema{
			"test_bool":           "boolean",
			"test_datetime":       "datetime",
			"test_float":          "float",
			"test_int":            "int",
			"test_string":         "string",
			"id":                  "string",
			"received_at":         "datetime",
			"extra_test_bool":     "boolean",
			"extra_test_datetime": "datetime",
			"extra_test_float":    "float",
			"extra_test_int":      "int",
			"extra_test_string":   "string",
		}

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: sourceID,
			},
			Destination: backendconfig.DestinationT{
				ID: destinationID,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
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
			},
			WorkspaceID: workspaceID,
			Namespace:   namespace,
		}

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: warehouseutils.S3,
			Config: map[string]any{
				"bucketName":     rsTestCredentials.BucketName,
				"accessKeyID":    rsTestCredentials.AccessKeyID,
				"accessKey":      rsTestCredentials.AccessKey,
				"bucketProvider": warehouseutils.S3,
			},
		})
		require.NoError(t, err)

		t.Run("schema does not exists", func(t *testing.T) {
			tableName := "schema_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv)

			rs := redshift.New(config.New(), logger.NOP, memstats.New())
			err := rs.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := rs.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exists", func(t *testing.T) {
			tableName := "table_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv)

			rs := redshift.New(config.New(), logger.NOP, memstats.New())
			err := rs.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = rs.CreateSchema(ctx)
			require.NoError(t, err)

			loadTableStat, err := rs.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("merge", func(t *testing.T) {
			t.Run("without dedup", func(t *testing.T) {
				tableName := "merge_without_dedup_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv)

				d := redshift.New(config.New(), logger.NOP, memstats.New())
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)

				err = d.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
					fmt.Sprintf(
						`SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  test_float,
						  test_int,
						  test_string
						FROM %s.%s
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, whth.DedupTwiceTestRecords(), records)
			})
			t.Run("with dedup", func(t *testing.T) {
				tableName := "merge_with_dedup_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv)

				d := redshift.New(config.New(), logger.NOP, memstats.New())
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)

				err = d.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
					fmt.Sprintf(
						`SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  test_float,
						  test_int,
						  test_string
						FROM %s.%s
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, whth.DedupTestRecords(), records)
			})
			t.Run("with dedup window", func(t *testing.T) {
				tableName := "merge_with_dedup_window_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv)

				c := config.New()
				c.Set("Warehouse.redshift.dedupWindow", true)
				c.Set("Warehouse.redshift.dedupWindowInHours", 999999)

				d := redshift.New(c, logger.NOP, memstats.New())
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)

				err = d.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(0))
				require.Equal(t, loadTableStat.RowsUpdated, int64(14))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
					fmt.Sprintf(
						`SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  test_float,
						  test_int,
						  test_string
						FROM %s.%s
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, whth.DedupTestRecords(), records)
			})
			t.Run("with short dedup window", func(t *testing.T) {
				tableName := "merge_with_short_dedup_window_test_table"
				uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/dedup.csv.gz", tableName)

				loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
				mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv)

				c := config.New()
				c.Set("Warehouse.redshift.dedupWindow", true)
				c.Set("Warehouse.redshift.dedupWindowInHours", 0)

				d := redshift.New(c, logger.NOP, memstats.New())
				err := d.Setup(ctx, warehouse, mockUploader)
				require.NoError(t, err)

				err = d.CreateSchema(ctx)
				require.NoError(t, err)

				err = d.CreateTable(ctx, tableName, schemaInWarehouse)
				require.NoError(t, err)

				loadTableStat, err := d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				loadTableStat, err = d.LoadTable(ctx, tableName)
				require.NoError(t, err)
				require.Equal(t, loadTableStat.RowsInserted, int64(14))
				require.Equal(t, loadTableStat.RowsUpdated, int64(0))

				records := whth.RetrieveRecordsFromWarehouse(t, d.DB.DB,
					fmt.Sprintf(
						`SELECT
						  id,
						  received_at,
						  test_bool,
						  test_datetime,
						  test_float,
						  test_int,
						  test_string
						FROM %s.%s
						ORDER BY id;`,
						namespace,
						tableName,
					),
				)
				require.Equal(t, whth.DedupTwiceTestRecords(), records)
			})
		})
		t.Run("append", func(t *testing.T) {
			tableName := "append_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv)

			c := config.New()
			c.Set("Warehouse.redshift.skipDedupDestinationIDs", []string{destinationID})

			appendWarehouse := th.Clone(t, warehouse)
			appendWarehouse.Destination.Config[string(model.PreferAppendSetting)] = true

			rs := redshift.New(c, logger.NOP, memstats.New())
			err := rs.Setup(ctx, appendWarehouse, mockUploader)
			require.NoError(t, err)

			err = rs.CreateSchema(ctx)
			require.NoError(t, err)

			err = rs.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := rs.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			loadTableStat, err = rs.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, rs.DB.DB,
				fmt.Sprintf(`
					SELECT
					  id,
					  received_at,
					  test_bool,
					  test_datetime,
					  test_float,
					  test_int,
					  test_string
					FROM
					  %q.%q
					ORDER BY
					  id;
					`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.AppendTestRecords())
		})
		t.Run("load file does not exists", func(t *testing.T) {
			tableName := "load_file_not_exists_test_table"

			loadFiles := []warehouseutils.LoadFile{{
				Location: "https://bucket.s3.amazonaws.com/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/0ef75cb0-3fd0-4408-98b9-2bea9e476916-load_file_not_exists_test_table/load.csv.gz",
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv)

			rs := redshift.New(config.New(), logger.NOP, memstats.New())
			err := rs.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = rs.CreateSchema(ctx)
			require.NoError(t, err)

			err = rs.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := rs.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := "mismatch_columns_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv)

			rs := redshift.New(config.New(), logger.NOP, memstats.New())
			err := rs.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = rs.CreateSchema(ctx)
			require.NoError(t, err)

			err = rs.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := rs.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := "mismatch_schema_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-schema.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse, warehouseutils.LoadFileTypeCsv)

			rs := redshift.New(config.New(), logger.NOP, memstats.New())
			err := rs.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = rs.CreateSchema(ctx)
			require.NoError(t, err)

			err = rs.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := rs.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("discards", func(t *testing.T) {
			tableName := warehouseutils.DiscardsTable

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/discards.csv.gz", tableName)

			loadFiles := []warehouseutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, warehouseutils.DiscardsSchema, warehouseutils.DiscardsSchema, warehouseutils.LoadFileTypeCsv)

			rs := redshift.New(config.New(), logger.NOP, memstats.New())
			err := rs.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = rs.CreateSchema(ctx)
			require.NoError(t, err)

			err = rs.CreateTable(ctx, tableName, warehouseutils.DiscardsSchema)
			require.NoError(t, err)

			loadTableStat, err := rs.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(6))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, rs.DB.DB,
				fmt.Sprintf(`
					SELECT
					  column_name,
					  column_value,
					  received_at,
					  row_id,
					  table_name,
					  uuid_ts
					FROM
					  %q.%q
					ORDER BY row_id ASC;
					`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.DiscardTestRecords())
		})
		t.Run("parquet", func(t *testing.T) {
			tableName := "parquet_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.parquet", tableName)

			fileStat, err := os.Stat("../testdata/load.parquet")
			require.NoError(t, err)

			loadFiles := []warehouseutils.LoadFile{{
				Location: uploadOutput.Location,
				Metadata: json.RawMessage(fmt.Sprintf(`{"content_length": %d}`, fileStat.Size())),
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInUpload, warehouseutils.LoadFileTypeParquet)

			rs := redshift.New(config.New(), logger.NOP, memstats.New())
			err = rs.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = rs.CreateSchema(ctx)
			require.NoError(t, err)

			err = rs.CreateTable(ctx, tableName, schemaInUpload)
			require.NoError(t, err)

			loadTableStat, err := rs.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := whth.RetrieveRecordsFromWarehouse(t, rs.DB.DB,
				fmt.Sprintf(`
					SELECT
					  id,
					  received_at,
					  test_bool,
					  test_datetime,
					  test_float,
					  test_int,
					  test_string
					FROM
					  %q.%q
					ORDER BY
					  id;
					`,
					namespace,
					tableName,
				),
			)
			require.Equal(t, records, whth.SampleTestRecords())
		})
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

			rs := redshift.New(config.New(), logger.NOP, memstats.New())

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

func newMockUploader(
	t testing.TB,
	loadFiles []warehouseutils.LoadFile,
	tableName string,
	schemaInUpload model.TableSchema,
	schemaInWarehouse model.TableSchema,
	loadFileType string,
) warehouseutils.Uploader {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockUploader := mockuploader.NewMockUploader(ctrl)
	mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, options warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
			return slices.Clone(loadFiles), nil
		},
	).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()
	mockUploader.EXPECT().GetLoadFileType().Return(loadFileType).AnyTimes()
	mockUploader.EXPECT().CanAppend().Return(true).AnyTimes()

	return mockUploader
}
