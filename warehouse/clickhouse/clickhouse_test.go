package clickhouse_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestIntegrationClickHouse(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	compose := testcompose.New(t, compose.FilePath("./testdata/docker-compose.yaml"))
	compose.Start(context.Background())
	t.Cleanup(func() {
		compose.Stop(context.Background())
	})

	rudderServer := testhelper.SetupServer(t, workspaceConfig.CreateTempFile(t,
		"testdata/config_template.json",
		map[string]string{
			"workspaceId": "BpLnfgDsc2WD8F2qNfHK5a84jjJ",

			"clickHouseWriteKey": "C5AWX39IVUWSP2NcHciWvqZTa2N",
			"clickHouseHost":     "localhost",
			"clickHouseDatabase": "rudderdb",
			"clickHouseUser":     "rudder",
			"clickHousePassword": "rudder-password",
			"clickHousePort":     strconv.Itoa(compose.Port("wh-clickhouse", 9000)),

			"clickhouseClusterWriteKey": "95RxRTZHWUsaD6HEdz0ThbXfQ6p",
			"clickhouseClusterHost":     "localhost",
			"clickhouseClusterDatabase": "rudderdb",
			"clickhouseClusterCluster":  "rudder_cluster",
			"clickhouseClusterUser":     "rudder",
			"clickhouseClusterPassword": "rudder-password",
			"clickhouseClusterPort":     strconv.Itoa(compose.Port("wh-clickhouse01", 9000)),

			"minioBucketName":      "devintegrationtest",
			"minioAccesskeyID":     "MYACCESSKEY",
			"minioSecretAccessKey": "MYSECRETKEY",
			"minioEndpoint":        fmt.Sprintf("localhost:%d", compose.Port("minio", 9000)),
		},
	))

	clickhouse.Init()

	var dbs []*sql.DB
	for _, host := range []string{"wh-clickhouse", "wh-clickhouse01", "wh-clickhouse02", "wh-clickhouse03", "wh-clickhouse04"} {
		db, err := clickhouse.Connect(clickhouse.CredentialsT{
			Host:          "localhost",
			User:          "rudder",
			Password:      "rudder-password",
			DBName:        "rudderdb",
			Secure:        "false",
			SkipVerify:    "true",
			TLSConfigName: "",
			Port:          strconv.Itoa(compose.Port(host, 9000)),
		}, true)
		require.NoError(t, err)

		err = db.Ping()
		require.NoError(t, err)

		dbs = append(dbs, db)
	}

	var (
		provider = warehouseutils.CLICKHOUSE
		jobsDB   = testhelper.SetUpJobsDB(t)
		tables   = []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}
	)

	testCases := []struct {
		name            string
		sourceID        string
		destinationID   string
		writeKey        string
		warehouseEvents testhelper.EventsCountMap
		clusterSetup    func(t testing.TB)
		db              *sql.DB
	}{
		{
			name:          "Single Setup",
			sourceID:      "1wRvLmEnMOOxNM79pwaZhyCqXRE",
			destinationID: "21Ev6TI6emCFDKph2Zn6XfTP7PI",
			writeKey:      "C5AWX39IVUWSP2NcHciWvqZTa2N",
			db:            dbs[0],
		},
		{
			name:          "Cluster Mode Setup",
			sourceID:      "1wRvLmEnMOOxNM79ghdZhyCqXRE",
			destinationID: "21Ev6TI6emCFDKhp2Zn6XfTP7PI",
			writeKey:      "95RxRTZHWUsaD6HEdz0ThbXfQ6p",
			db:            dbs[1],
			warehouseEvents: testhelper.EventsCountMap{
				"identifies":    8,
				"users":         2,
				"tracks":        8,
				"product_track": 8,
				"pages":         8,
				"screens":       8,
				"aliases":       8,
				"groups":        8,
			},
			clusterSetup: func(t testing.TB) {
				t.Helper()
				initializeClickhouseClusterMode(t, dbs[1:], tables)
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ts := testhelper.WareHouseTest{
				Schema: "rudderdb",
				EventClient: &testhelper.EventClient{
					URL:      rudderServer.URL,
					WriteKey: tc.writeKey,
				},
				SourceID:      tc.sourceID,
				DestinationID: tc.destinationID,
				Tables:        tables,
				Provider:      provider,
				JobsDB:        jobsDB,
				UserID:        testhelper.GetUserId(provider),
				StatsToVerify: []string{
					"warehouse_clickhouse_commitTimeouts",
					"warehouse_clickhouse_execTimeouts",
					"warehouse_clickhouse_failedRetries",
					"warehouse_clickhouse_syncLoadFileTime",
					"warehouse_clickhouse_downloadLoadFilesTime",
					"warehouse_clickhouse_numRowsLoadFile",
				},
				Client: &client.Client{
					SQL:  tc.db,
					Type: client.SQLClient,
				},
			}
			ts.VerifyEvents(t)

			if tc.clusterSetup != nil {
				tc.clusterSetup(t)
			}

			ts.UserID = testhelper.GetUserId(provider)
			ts.WarehouseEventsMap = tc.warehouseEvents
			ts.VerifyModifiedEvents(t)
		})
	}
}

func TestConfigurationValidationClickhouse(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}

	t.Parallel()

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	clickhouse.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "21Ev6TI6emCFDKph2Zn6XfTP7PI",
		Config: map[string]interface{}{
			"host":             configurations["clickHouseHost"],
			"database":         configurations["clickHouseDatabase"],
			"cluster":          "",
			"user":             configurations["clickHouseUser"],
			"password":         configurations["clickHousePassword"],
			"port":             configurations["clickHousePort"],
			"secure":           false,
			"namespace":        "",
			"bucketProvider":   "MINIO",
			"bucketName":       configurations["minioBucketName"],
			"accessKeyID":      configurations["minioAccesskeyID"],
			"secretAccessKey":  configurations["minioSecretAccessKey"],
			"useSSL":           false,
			"endPoint":         configurations["minioEndpoint"],
			"syncFrequency":    "30",
			"useRudderStorage": false,
		},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "1eBvkIRSwc2ESGMK9dj6OXq2G12",
			Name:        "CLICKHOUSE",
			DisplayName: "ClickHouse",
		},
		Name:       "clickhouse-demo",
		Enabled:    true,
		RevisionID: "29eeuTnqbBKn0XVTj5z9XQIbaru",
	}
	testhelper.VerifyConfigurationTest(t, destination)
}

func initializeClickhouseClusterMode(t testing.TB, clusterDBs []*sql.DB, tables []string) {
	t.Helper()

	type ColumnInfoT struct {
		ColumnName string
		ColumnType string
	}

	tableColumnInfoMap := map[string][]ColumnInfoT{
		"identifies": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"product_track": {
			{
				ColumnName: "revenue",
				ColumnType: "Nullable(Float64)",
			},
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"tracks": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"users": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "SimpleAggregateFunction(anyLast, Nullable(String))",
			},
		},
		"pages": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"screens": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"aliases": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
		"groups": {
			{
				ColumnName: "context_passed_ip",
				ColumnType: "Nullable(String)",
			},
			{
				ColumnName: "context_library_name",
				ColumnType: "Nullable(String)",
			},
		},
	}

	clusterDB := clusterDBs[0]

	// Rename tables to tables_shard
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		log.Printf("Renaming tables to sharded tables for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		require.NoError(t, testhelper.WithConstantBackoff(func() error {
			_, err := clusterDB.Exec(sqlStatement)
			return err
		}))
	}

	// Create distribution views for tables
	for _, table := range tables {
		sqlStatement := fmt.Sprintf(`
			CREATE TABLE rudderdb.%[1]s ON CLUSTER 'rudder_cluster' AS rudderdb.%[1]s_shard ENGINE = Distributed(
			  'rudder_cluster',
			  rudderdb,
			  %[1]s_shard,
			  cityHash64(
				concat(
				  toString(
					toDate(received_at)
				  ),
				  id
				)
			  )
			);`,
			table,
		)
		log.Printf("Creating distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		require.NoError(t, testhelper.WithConstantBackoff(func() error {
			_, err := clusterDB.Exec(sqlStatement)
			return err
		}))
	}

	// Alter columns to all the cluster tables
	for _, clusterDB := range clusterDBs {
		for tableName, columnInfos := range tableColumnInfoMap {
			sqlStatement := fmt.Sprintf(`
				ALTER TABLE rudderdb.%[1]s_shard`,
				tableName,
			)
			for _, columnInfo := range columnInfos {
				sqlStatement += fmt.Sprintf(`
					ADD COLUMN IF NOT EXISTS %[1]s %[2]s,`,
					columnInfo.ColumnName,
					columnInfo.ColumnType,
				)
			}
			sqlStatement = strings.TrimSuffix(sqlStatement, ",")
			log.Printf("Altering columns for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

			require.NoError(t, testhelper.WithConstantBackoff(func() error {
				_, err := clusterDB.Exec(sqlStatement)
				return err
			}))
		}
	}
}
