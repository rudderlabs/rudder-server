package bigquery_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/samber/lo"
	"go.uber.org/mock/gomock"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	whbigquery "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	bqhelper "github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery/testhelper"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if _, exists := os.LookupEnv(bqhelper.TestKey); !exists {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", bqhelper.TestKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), bqhelper.TestKey)
	}

	misc.Init()
	validations.Init()
	whutils.Init()

	destType := whutils.BQ

	credentials, err := bqhelper.GetBQTestCredentials()
	require.NoError(t, err)

	t.Run("Event flow", func(t *testing.T) {
		httpPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		c := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml"}))
		c.Start(context.Background())

		workspaceID := whutils.RandHex()
		jobsDBPort := c.Port("jobsDb", 5432)

		jobsDB := whth.JobsDB(t, jobsDBPort)

		testcase := []struct {
			name                                string
			tables                              []string
			stagingFilesEventsMap               whth.EventsCountMap
			stagingFilesModifiedEventsMap       whth.EventsCountMap
			loadFilesEventsMap                  whth.EventsCountMap
			tableUploadsEventsMap               whth.EventsCountMap
			warehouseEventsMap                  whth.EventsCountMap
			sourceJob                           bool
			skipModifiedEvents                  bool
			setup                               func(testing.TB, context.Context, *bigquery.Client, string)
			checkTablesPostLoading              func(testing.TB, context.Context, *bigquery.Client, string)
			customPartitionsEnabledWorkspaceIDs string
			stagingFilePrefix                   string
			configOverride                      map[string]any
		}{
			{
				name:   "Source Job",
				tables: []string{"tracks", "google_sheet"},
				stagingFilesEventsMap: whth.EventsCountMap{
					"wh_staging_files": 9, // 8 + 1 (merge events because of ID resolution)
				},
				stagingFilesModifiedEventsMap: whth.EventsCountMap{
					"wh_staging_files": 8, // 8 (de-duped by encounteredMergeRuleMap)
				},
				loadFilesEventsMap:    whth.SourcesLoadFilesEventsMap(),
				tableUploadsEventsMap: whth.SourcesTableUploadsEventsMap(),
				warehouseEventsMap:    whth.SourcesWarehouseEventsMap(),
				sourceJob:             true,
				stagingFilePrefix:     "testdata/sources-job",
			},
			{
				name: "Append mode",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilesEventsMap:         stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap: stagingFilesEventsMap(),
				loadFilesEventsMap:            loadFilesEventsMap(),
				tableUploadsEventsMap:         tableUploadsEventsMap(),
				warehouseEventsMap:            appendEventsMap(),
				skipModifiedEvents:            true,
				checkTablesPostLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Empty(t, table.TimePartitioning.Field) // If empty, the table is partitioned by pseudo column '_PARTITIONTIME'
						require.Equal(t, bigquery.DayPartitioningType, table.TimePartitioning.Type)
					}

					verifyEventsUsingView(t, ctx, db, namespace, whth.EventsCountMap{
						"identifies":    1,
						"users":         1,
						"tracks":        1,
						"product_track": 1,
						"pages":         1,
						"screens":       1,
						"aliases":       1,
						"groups":        1,
						"_groups":       1,
					})
				},
				stagingFilePrefix: "testdata/upload-job-append-mode",
			},
			{
				name: "Append mode with default config (partitionColumn: _PARTITIONTIME, partitionType: day)",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				checkTablesPostLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Empty(t, table.TimePartitioning.Field) // If empty, the table is partitioned by pseudo column '_PARTITIONTIME'
						require.Equal(t, bigquery.DayPartitioningType, table.TimePartitioning.Type)
					}

					verifyEventsUsingView(t, ctx, db, namespace, whth.EventsCountMap{
						"identifies":    1,
						"users":         1,
						"tracks":        1,
						"product_track": 1,
						"pages":         1,
						"screens":       1,
						"aliases":       1,
						"groups":        1,
						"_groups":       1,
					})
				},
				stagingFilePrefix: "testdata/upload-job-append-mode-custom-partition",
				configOverride: map[string]any{
					"partitionColumn": "_PARTITIONTIME",
					"partitionType":   "day",
				},
			},
			{
				name: "Append mode with ingestion-time hour partitioning (partitionColumn: _PARTITIONTIME, partitionType: hour)",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				checkTablesPostLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Empty(t, table.TimePartitioning.Field) // If empty, the table is partitioned by pseudo column '_PARTITIONTIME'
						require.Equal(t, bigquery.HourPartitioningType, table.TimePartitioning.Type)
					}

					verifyEventsUsingView(t, ctx, db, namespace, whth.EventsCountMap{
						"identifies":    1,
						"users":         1,
						"tracks":        1,
						"product_track": 1,
						"pages":         1,
						"screens":       1,
						"aliases":       1,
						"groups":        1,
						"_groups":       1,
					})
				},
				stagingFilePrefix: "testdata/upload-job-append-mode-custom-partition",
				configOverride: map[string]any{
					"partitionColumn": "_PARTITIONTIME",
					"partitionType":   "hour",
				},
			},
			{
				name: "Append mode with ingestion-time monthly partitioning (partitionColumn: _PARTITIONTIME, partitionType: month)",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				checkTablesPostLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Empty(t, table.TimePartitioning.Field) // If empty, the table is partitioned by pseudo column '_PARTITIONTIME'
						require.Equal(t, bigquery.MonthPartitioningType, table.TimePartitioning.Type)
					}

					verifyEventsUsingView(t, ctx, db, namespace, whth.EventsCountMap{
						"identifies":    1,
						"users":         1,
						"tracks":        1,
						"product_track": 1,
						"pages":         1,
						"screens":       1,
						"aliases":       1,
						"groups":        1,
						"_groups":       1,
					})
				},
				stagingFilePrefix: "testdata/upload-job-append-mode-custom-partition",
				configOverride: map[string]any{
					"partitionColumn": "_PARTITIONTIME",
					"partitionType":   "month",
				},
			},
			{
				name: "Append mode with ingestion-time monthly partitioning (partitionColumn: _PARTITIONTIME, partitionType: year)",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				checkTablesPostLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Empty(t, table.TimePartitioning.Field) // If empty, the table is partitioned by pseudo column '_PARTITIONTIME'
						require.Equal(t, bigquery.YearPartitioningType, table.TimePartitioning.Type)
					}

					verifyEventsUsingView(t, ctx, db, namespace, whth.EventsCountMap{
						"identifies":    1,
						"users":         1,
						"tracks":        1,
						"product_track": 1,
						"pages":         1,
						"screens":       1,
						"aliases":       1,
						"groups":        1,
						"_groups":       1,
					})
				},
				stagingFilePrefix: "testdata/upload-job-append-mode-custom-partition",
				configOverride: map[string]any{
					"partitionColumn": "_PARTITIONTIME",
					"partitionType":   "year",
				},
			},
			{
				name: "Append mode (partitionColumn: received_at, partitionType: hour)",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				checkTablesPostLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Equal(t, "received_at", table.TimePartitioning.Field)
						require.Equal(t, bigquery.HourPartitioningType, table.TimePartitioning.Type)
					}
					partitions := listPartitions(t, ctx, db, namespace)
					filteredPartitions := lo.Filter(partitions, func(table lo.Tuple2[string, string], _ int) bool {
						return lo.Contains(checkTables, table.A)
					})
					for _, partition := range filteredPartitions {
						require.Equal(t, partition.B, "2023051204")
					}
				},
				stagingFilePrefix: "testdata/upload-job-append-mode-custom-partition",
				configOverride: map[string]any{
					"partitionColumn": "received_at",
					"partitionType":   "hour",
				},
			},
			{
				name: "Append mode (partitionColumn: received_at, partitionType: day)",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				checkTablesPostLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Equal(t, "received_at", table.TimePartitioning.Field)
						require.Equal(t, bigquery.DayPartitioningType, table.TimePartitioning.Type)
					}
					partitions := listPartitions(t, ctx, db, namespace)
					filteredPartitions := lo.Filter(partitions, func(table lo.Tuple2[string, string], _ int) bool {
						return lo.Contains(checkTables, table.A)
					})
					for _, partition := range filteredPartitions {
						require.Equal(t, partition.B, "20230512")
					}
				},
				stagingFilePrefix: "testdata/upload-job-append-mode-custom-partition",
				configOverride: map[string]any{
					"partitionColumn": "received_at",
					"partitionType":   "day",
				},
			},
			{
				name: "Append mode (partitionColumn: received_at, partitionType: month)",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				checkTablesPostLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Equal(t, "received_at", table.TimePartitioning.Field)
						require.Equal(t, bigquery.MonthPartitioningType, table.TimePartitioning.Type)
					}
					partitions := listPartitions(t, ctx, db, namespace)
					filteredPartitions := lo.Filter(partitions, func(table lo.Tuple2[string, string], _ int) bool {
						return lo.Contains(checkTables, table.A)
					})
					for _, partition := range filteredPartitions {
						require.Equal(t, partition.B, "202305")
					}
				},
				stagingFilePrefix: "testdata/upload-job-append-mode-custom-partition",
				configOverride: map[string]any{
					"partitionColumn": "received_at",
					"partitionType":   "month",
				},
			},
			{
				name: "Append mode (partitionColumn: received_at, partitionType: year)",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				checkTablesPostLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Equal(t, "received_at", table.TimePartitioning.Field)
						require.Equal(t, bigquery.YearPartitioningType, table.TimePartitioning.Type)
					}
					partitions := listPartitions(t, ctx, db, namespace)
					filteredPartitions := lo.Filter(partitions, func(table lo.Tuple2[string, string], _ int) bool {
						return lo.Contains(checkTables, table.A)
					})
					for _, partition := range filteredPartitions {
						require.Equal(t, partition.B, "2023")
					}
				},
				stagingFilePrefix: "testdata/upload-job-append-mode-custom-partition",
				configOverride: map[string]any{
					"partitionColumn": "received_at",
					"partitionType":   "year",
				},
			},
			{
				name: "Append mode with table already created (partitionColumn: received_at, partitionType: day)",
				tables: []string{
					"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups",
				},
				stagingFilesEventsMap:               stagingFilesEventsMap(),
				stagingFilesModifiedEventsMap:       stagingFilesEventsMap(),
				loadFilesEventsMap:                  loadFilesEventsMap(),
				tableUploadsEventsMap:               tableUploadsEventsMap(),
				warehouseEventsMap:                  appendEventsMap(),
				skipModifiedEvents:                  true,
				customPartitionsEnabledWorkspaceIDs: workspaceID,
				setup: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					err = db.Dataset(namespace).Create(context.Background(), &bigquery.DatasetMetadata{
						Location: "US",
					})
					require.NoError(t, err)

					for _, table := range []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"} {
						err = db.Dataset(namespace).Table(table).Create(context.Background(), &bigquery.TableMetadata{
							Schema: []*bigquery.FieldSchema{
								{Name: "received_at", Type: bigquery.TimestampFieldType},
							},
							TimePartitioning: &bigquery.TimePartitioning{
								Field: "received_at",
								Type:  bigquery.DayPartitioningType,
							},
						})
						require.NoError(t, err)
					}
				},
				checkTablesPostLoading: func(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) {
					t.Helper()

					checkTables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

					tables := listTables(t, ctx, db, namespace)
					filteredTables := lo.Filter(tables, func(table *bigquery.TableMetadata, _ int) bool {
						return lo.Contains(checkTables, table.Name)
					})
					for _, table := range filteredTables {
						require.NotNil(t, table.TimePartitioning)
						require.Equal(t, "received_at", table.TimePartitioning.Field)
						require.Equal(t, bigquery.DayPartitioningType, table.TimePartitioning.Type)
					}
					partitions := listPartitions(t, ctx, db, namespace)
					filteredPartitions := lo.Filter(partitions, func(table lo.Tuple2[string, string], _ int) bool {
						return lo.Contains(checkTables, table.A)
					})
					for _, partition := range filteredPartitions {
						require.Equal(t, partition.B, "20230512")
					}
				},
				stagingFilePrefix: "testdata/upload-job-append-mode-custom-partition",
				configOverride: map[string]any{
					"partitionColumn": "received_at",
					"partitionType":   "day",
				},
			},
		}

		for _, tc := range testcase {
			t.Run(tc.name, func(t *testing.T) {
				var (
					sourceID      = whutils.RandHex()
					destinationID = whutils.RandHex()
					writeKey      = whutils.RandHex()
					namespace     = whth.RandSchema(destType)
				)

				destinationBuilder := backendconfigtest.NewDestinationBuilder(destType).
					WithID(destinationID).
					WithRevisionID(destinationID).
					WithConfigOption("project", credentials.ProjectID).
					WithConfigOption("location", credentials.Location).
					WithConfigOption("bucketName", credentials.BucketName).
					WithConfigOption("credentials", credentials.Credentials).
					WithConfigOption("namespace", namespace).
					WithConfigOption("syncFrequency", "30")
				for k, v := range tc.configOverride {
					destinationBuilder = destinationBuilder.WithConfigOption(k, v)
				}

				workspaceConfig := backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID(sourceID).
							WithWriteKey(writeKey).
							WithWorkspaceID(workspaceID).
							WithConnection(destinationBuilder.Build()).
							Build(),
					).
					WithWorkspaceID(workspaceID).
					Build()

				t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_ENABLE_DELETE_BY_JOBS", "true")
				t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_MAX_PARALLEL_LOADS", "8")
				t.Setenv("RSERVER_WAREHOUSE_BIGQUERY_SLOW_QUERY_THRESHOLD", "0s")

				whth.BootstrapSvc(t, workspaceConfig, httpPort, jobsDBPort)

				ctx := context.Background()

				db, err := bigquery.NewClient(
					ctx,
					credentials.ProjectID,
					option.WithCredentialsJSON([]byte(credentials.Credentials)),
				)
				require.NoError(t, err)
				t.Cleanup(func() { _ = db.Close() })
				t.Cleanup(func() {
					dropSchema(t, db, namespace)
				})

				if tc.setup != nil {
					tc.setup(t, ctx, db, namespace)
				}

				sqlClient := &client.Client{
					BQ:   db,
					Type: client.BQClient,
				}

				conf := map[string]any{
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
				}

				t.Log("verifying test case 1")
				ts1 := whth.TestConfig{
					WriteKey:              writeKey,
					Schema:                namespace,
					Tables:                tc.tables,
					SourceID:              sourceID,
					DestinationID:         destinationID,
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

				if tc.checkTablesPostLoading != nil {
					tc.checkTablesPostLoading(t, ctx, db, namespace)
				}
				if tc.skipModifiedEvents {
					return
				}

				t.Log("verifying test case 2")
				ts2 := whth.TestConfig{
					WriteKey:              writeKey,
					Schema:                namespace,
					Tables:                tc.tables,
					SourceID:              sourceID,
					DestinationID:         destinationID,
					StagingFilesEventsMap: tc.stagingFilesModifiedEventsMap,
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
					StagingFilePath:       tc.stagingFilePrefix + ".staging-2.json",
					UserID:                whth.GetUserId(destType),
				}
				if tc.sourceJob {
					ts2.UserID = ts1.UserID
				}
				ts2.VerifyEvents(t)
			})
		}
	})

	t.Run("Validations", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		db, err := bigquery.NewClient(ctx,
			credentials.ProjectID,
			option.WithCredentialsJSON([]byte(credentials.Credentials)),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		dest := backendconfig.DestinationT{
			ID: "test_destination_id",
			Config: map[string]interface{}{
				"project":       credentials.ProjectID,
				"location":      credentials.Location,
				"bucketName":    credentials.BucketName,
				"credentials":   credentials.Credentials,
				"prefix":        "",
				"namespace":     namespace,
				"syncFrequency": "30",
			},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				ID:          "1UmeD7xhVGHsPDEHoCiSPEGytS3",
				Name:        "BQ",
				DisplayName: "BigQuery",
			},
			Name:       "bigquery-integration",
			Enabled:    true,
			RevisionID: "test_destination_id",
		}
		whth.VerifyConfigurationTest(t, dest)
	})

	t.Run("Load Table", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		db, err := bigquery.NewClient(ctx,
			credentials.ProjectID,
			option.WithCredentialsJSON([]byte(credentials.Credentials)),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
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
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
					"project":     credentials.ProjectID,
					"location":    credentials.Location,
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
					"namespace":   namespace,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		fm, err := filemanager.New(&filemanager.Settings{
			Provider: whutils.GCS,
			Config: map[string]any{
				"project":     credentials.ProjectID,
				"location":    credentials.Location,
				"bucketName":  credentials.BucketName,
				"credentials": credentials.Credentials,
			},
		})
		require.NoError(t, err)

		t.Run("schema does not exist", func(t *testing.T) {
			tableName := "schema_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("table does not exist", func(t *testing.T) {
			tableName := "table_not_exists_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("append", func(t *testing.T) {
			tableName := "append_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			loadTableStat, err = bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := bqhelper.RetrieveRecordsFromWarehouse(t, db,
				fmt.Sprintf(`
					SELECT
					  id,
					  received_at,
					  test_bool,
					  test_datetime,
					  test_float,
					  test_int,
					  test_string
					FROM %s.%s
					WHERE _PARTITIONTIME BETWEEN TIMESTAMP('%s') AND TIMESTAMP('%s')
					ORDER BY id;`,
					namespace,
					tableName,
					time.Now().Add(-24*time.Hour).Format("2006-01-02"),
					time.Now().Add(+24*time.Hour).Format("2006-01-02"),
				),
			)
			require.Equal(t, records, whth.AppendTestRecords())
		})
		t.Run("load file does not exists", func(t *testing.T) {
			tableName := "load_file_not_exists_test_table"

			loadFiles := []whutils.LoadFile{{
				Location: "https://storage.googleapis.com/project/rudder-warehouse-load-objects/load_file_not_exists_test_table/test_source_id/2e04b6bd-8007-461e-a338-91224a8b7d3d-load_file_not_exists_test_table/load.json.gz",
			}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in number of columns", func(t *testing.T) {
			tableName := "mismatch_columns_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-columns.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("mismatch in schema", func(t *testing.T) {
			tableName := "mismatch_schema_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/mismatch-schema.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, schemaInUpload, schemaInWarehouse)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.Error(t, err)
			require.Nil(t, loadTableStat)
		})
		t.Run("discards", func(t *testing.T) {
			tableName := whutils.DiscardsTable

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/discards.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(t, loadFiles, tableName, whutils.DiscardsSchema, whutils.DiscardsSchema)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, whutils.DiscardsSchema)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(6))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := bqhelper.RetrieveRecordsFromWarehouse(t, db,
				fmt.Sprintf(
					`SELECT
						column_name,
						column_value,
						received_at,
						row_id,
						table_name,
						uuid_ts
					FROM %s
					ORDER BY row_id ASC;`,
					fmt.Sprintf("`%s`.`%s`", namespace, tableName),
				),
			)
			require.Equal(t, records, whth.DiscardTestRecords())
		})
		t.Run("custom partition", func(t *testing.T) {
			tableName := "partition_test_table"

			uploadOutput := whth.UploadLoadFile(t, fm, "../testdata/load.json.gz", tableName)

			loadFiles := []whutils.LoadFile{{Location: uploadOutput.Location}}
			mockUploader := newMockUploader(
				t, loadFiles, tableName, schemaInUpload,
				schemaInWarehouse,
			)

			c := config.New()
			c.Set("Warehouse.bigquery.customPartitionsEnabled", true)
			c.Set("Warehouse.bigquery.customPartitionsEnabledWorkspaceIDs", []string{"test_workspace_id"})

			bq := whbigquery.New(c, logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			err = bq.CreateTable(ctx, tableName, schemaInWarehouse)
			require.NoError(t, err)

			loadTableStat, err := bq.LoadTable(ctx, tableName)
			require.NoError(t, err)
			require.Equal(t, loadTableStat.RowsInserted, int64(14))
			require.Equal(t, loadTableStat.RowsUpdated, int64(0))

			records := bqhelper.RetrieveRecordsFromWarehouse(t, db,
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
					WHERE _PARTITIONTIME BETWEEN TIMESTAMP('%s') AND TIMESTAMP('%s')
					ORDER BY id;`,
					namespace,
					tableName,
					time.Now().Add(-24*time.Hour).Format("2006-01-02"),
					time.Now().Add(+24*time.Hour).Format("2006-01-02"),
				),
			)
			require.Equal(t, records, whth.SampleTestRecords())
		})
	})

	t.Run("Fetch schema", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		db, err := bigquery.NewClient(ctx,
			credentials.ProjectID,
			option.WithCredentialsJSON([]byte(credentials.Credentials)),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
					"project":     credentials.ProjectID,
					"location":    credentials.Location,
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
					"namespace":   namespace,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		t.Run("should not contain staging like schema", func(t *testing.T) {
			tableName := "test_table"
			stagingTableName := whutils.StagingTableName(destType, whutils.UsersTable, 127)

			ctrl := gomock.NewController(t)
			mockUploader := mockuploader.NewMockUploader(ctrl)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			for _, table := range []string{tableName, stagingTableName} {
				require.NoError(t, db.Dataset(namespace).Table(table).Create(ctx, &bigquery.TableMetadata{
					Schema: []*bigquery.FieldSchema{
						{Name: "id", Type: bigquery.StringFieldType},
					},
					TimePartitioning: &bigquery.TimePartitioning{
						Type: bigquery.DayPartitioningType,
					},
				}))
			}

			tables := listTables(t, ctx, db, namespace)
			require.Equal(t, []string{stagingTableName, tableName}, lo.Map(tables, func(item *bigquery.TableMetadata, index int) string {
				return item.Name
			}))

			warehouseSchema, unrecognizedWarehouseSchema, err := bq.FetchSchema(ctx)
			require.NoError(t, err)
			require.Empty(t, unrecognizedWarehouseSchema)

			fetchedTables := lo.Keys(warehouseSchema)
			require.Equal(t, []string{tableName}, fetchedTables)
		})
	})

	t.Run("Crash recovery", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		db, err := bigquery.NewClient(ctx,
			credentials.ProjectID,
			option.WithCredentialsJSON([]byte(credentials.Credentials)),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
					"project":     credentials.ProjectID,
					"location":    credentials.Location,
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
					"namespace":   namespace,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		t.Run("should delete staging like table", func(t *testing.T) {
			tableName := "test_table"
			stagingTableName := whutils.StagingTableName(destType, whutils.UsersTable, 127)

			ctrl := gomock.NewController(t)
			mockUploader := mockuploader.NewMockUploader(ctrl)

			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			for _, table := range []string{tableName, stagingTableName} {
				require.NoError(t, db.Dataset(namespace).Table(table).Create(ctx, &bigquery.TableMetadata{
					Schema: []*bigquery.FieldSchema{
						{Name: "id", Type: bigquery.StringFieldType},
					},
					TimePartitioning: &bigquery.TimePartitioning{
						Type: bigquery.DayPartitioningType,
					},
				}))
			}

			tables := listTables(t, ctx, db, namespace)
			require.Equal(t, []string{stagingTableName, tableName}, lo.Map(tables, func(item *bigquery.TableMetadata, index int) string {
				return item.Name
			}))

			bq.Cleanup(ctx)

			tables = listTables(t, ctx, db, namespace)
			require.Equal(t, []string{tableName}, lo.Map(tables, func(item *bigquery.TableMetadata, index int) string {
				return item.Name
			}))
		})
	})

	t.Run("IsEmpty", func(t *testing.T) {
		ctx := context.Background()
		namespace := whth.RandSchema(destType)

		db, err := bigquery.NewClient(ctx,
			credentials.ProjectID,
			option.WithCredentialsJSON([]byte(credentials.Credentials)),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		t.Cleanup(func() {
			dropSchema(t, db, namespace)
		})

		warehouse := model.Warehouse{
			Source: backendconfig.SourceT{
				ID: "test_source_id",
			},
			Destination: backendconfig.DestinationT{
				ID: "test_destination_id",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
				},
				Config: map[string]any{
					"project":     credentials.ProjectID,
					"location":    credentials.Location,
					"bucketName":  credentials.BucketName,
					"credentials": credentials.Credentials,
					"namespace":   namespace,
				},
			},
			WorkspaceID: "test_workspace_id",
			Namespace:   namespace,
		}

		ctrl := gomock.NewController(t)
		t.Cleanup(ctrl.Finish)

		mockUploader := mockuploader.NewMockUploader(ctrl)
		mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()

		insertRecords := func(t testing.TB, tableName string) {
			t.Helper()

			query := db.Query(`
				INSERT INTO ` + tableName + ` (
				  id, received_at, test_bool, test_datetime,
				  test_float, test_int, test_string
				)
				VALUES
				  (
					'1', '2020-01-01 00:00:00', true,
					'2020-01-01 00:00:00', 1.1, 1, 'test'
				  );`,
			)
			job, err := query.Run(ctx)
			require.NoError(t, err)

			status, err := job.Wait(ctx)
			require.NoError(t, err)
			require.Nil(t, status.Err())
		}

		t.Run("tables doesn't exists", func(t *testing.T) {
			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			isEmpty, err := bq.IsEmpty(ctx, warehouse)
			require.NoError(t, err)
			require.True(t, isEmpty)
		})
		t.Run("tables empty", func(t *testing.T) {
			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			err = bq.CreateSchema(ctx)
			require.NoError(t, err)

			tables := []string{"pages", "screens"}
			for _, table := range tables {
				err = bq.CreateTable(ctx, table, model.TableSchema{
					"test_bool":     "boolean",
					"test_datetime": "datetime",
					"test_float":    "float",
					"test_int":      "int",
					"test_string":   "string",
					"id":            "string",
					"received_at":   "datetime",
				})
				require.NoError(t, err)
			}

			isEmpty, err := bq.IsEmpty(ctx, warehouse)
			require.NoError(t, err)
			require.True(t, isEmpty)
		})
		t.Run("tables not empty", func(t *testing.T) {
			bq := whbigquery.New(config.New(), logger.NOP)
			err := bq.Setup(ctx, warehouse, mockUploader)
			require.NoError(t, err)

			insertRecords(t, fmt.Sprintf("`%s`.`%s`", namespace, "pages"))
			insertRecords(t, fmt.Sprintf("`%s`.`%s`", namespace, "screens"))

			isEmpty, err := bq.IsEmpty(ctx, warehouse)
			require.NoError(t, err)
			require.False(t, isEmpty)
		})
	})
}

func listTables(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) []*bigquery.TableMetadata {
	t.Helper()
	t.Log("Listing tables in namespace", namespace)

	it := db.Dataset(namespace).Tables(ctx)

	var tables []*bigquery.TableMetadata
	for table, err := it.Next(); !errors.Is(err, iterator.Done); table, err = it.Next() {
		require.NoError(t, err)

		metadata, err := db.Dataset(namespace).Table(table.TableID).Metadata(ctx)
		require.NoError(t, err)

		metadata.Name = table.TableID
		tables = append(tables, metadata)
	}

	return lo.Filter(tables, func(item *bigquery.TableMetadata, index int) bool {
		return item.Type == "TABLE"
	})
}

func listPartitions(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string) (partitions []lo.Tuple2[string, string]) {
	t.Helper()
	t.Log("Listing partitions in namespace", namespace)

	query := fmt.Sprintf(`SELECT table_name, partition_id FROM %s.INFORMATION_SCHEMA.PARTITIONS;`,
		namespace,
	)

	it, err := db.Query(query).Read(ctx)
	require.NoError(t, err)

	for {
		var row []bigquery.Value

		if err = it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			require.NoError(t, err)
		}
		require.Len(t, row, 2)

		tableName, tableNameOK := row[0].(string)
		require.True(t, tableNameOK)
		partitionID, partitionIDOK := row[1].(string)
		require.True(t, partitionIDOK)

		partitions = append(partitions, lo.Tuple2[string, string]{
			A: tableName,
			B: partitionID,
		})
	}
	return
}

func verifyEventsUsingView(t testing.TB, ctx context.Context, db *bigquery.Client, namespace string, expectedEvents whth.EventsCountMap) {
	t.Helper()
	t.Log("Verifying events in view in namespace", namespace)

	for table, count := range expectedEvents {
		view := fmt.Sprintf("%s_view", table)
		query := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s;`, namespace, view)

		t.Logf("checking view %s", view)

		it, err := db.Query(query).Read(ctx)
		require.NoError(t, err)

		var row []bigquery.Value
		err = it.Next(&row)
		require.NoError(t, err)
		require.Len(t, row, 1)
		require.EqualValues(t, count, row[0])
	}
}

func dropSchema(t *testing.T, db *bigquery.Client, namespace string) {
	t.Helper()
	t.Log("Dropping schema", namespace)

	require.Eventually(t, func() bool {
		if err := db.Dataset(namespace).DeleteWithContents(context.Background()); err != nil {
			t.Logf("error deleting dataset: %v", err)
			return false
		}
		return true
	},
		time.Minute,
		time.Second,
	)
}

func newMockUploader(
	t testing.TB,
	loadFiles []whutils.LoadFile,
	tableName string,
	schemaInUpload model.TableSchema,
	schemaInWarehouse model.TableSchema,
) whutils.Uploader {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockUploader := mockuploader.NewMockUploader(ctrl)
	mockUploader.EXPECT().UseRudderStorage().Return(false).AnyTimes()
	mockUploader.EXPECT().GetLoadFilesMetadata(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, options whutils.GetLoadFilesOptions) ([]whutils.LoadFile, error) {
			return slices.Clone(loadFiles), nil
		},
	).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInUpload(tableName).Return(schemaInUpload).AnyTimes()
	mockUploader.EXPECT().GetTableSchemaInWarehouse(tableName).Return(schemaInWarehouse).AnyTimes()

	return mockUploader
}

func loadFilesEventsMap() whth.EventsCountMap {
	return whth.EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        1,
		"_groups":       3,
	}
}

func tableUploadsEventsMap() whth.EventsCountMap {
	return whth.EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        1,
		"_groups":       3,
	}
}

func stagingFilesEventsMap() whth.EventsCountMap {
	return whth.EventsCountMap{
		"wh_staging_files": 34, // Since extra 2 merge events because of ID resolution
	}
}

func appendEventsMap() whth.EventsCountMap {
	return whth.EventsCountMap{
		"identifies":    4,
		"users":         1,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        1,
		"_groups":       3,
	}
}
