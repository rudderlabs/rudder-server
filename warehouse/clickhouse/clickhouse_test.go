//go:build warehouse_integration

package clickhouse_test

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

var statsToVerify = []string{
	"warehouse_clickhouse_commitTimeouts",
	"warehouse_clickhouse_execTimeouts",
	"warehouse_clickhouse_failedRetries",
	"warehouse_clickhouse_syncLoadFileTime",
	"warehouse_clickhouse_downloadLoadFilesTime",
	"warehouse_clickhouse_numRowsLoadFile",
}

func TestClickHouseIntegration(t *testing.T) {
	t.Parallel()

	clickhouse.Init()

	var (
		db         *sql.DB
		clusterDBs []*sql.DB
	)

	db, err := clickhouse.Connect(clickhouse.CredentialsT{
		Host:          "wh-clickhouse",
		User:          "rudder",
		Password:      "rudder-password",
		DBName:        "rudderdb",
		Secure:        "false",
		SkipVerify:    "true",
		TLSConfigName: "",
		Port:          "9000",
	}, true)
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	for _, i := range []int{1, 2, 3, 4} {
		db, err := clickhouse.Connect(clickhouse.CredentialsT{
			Host:          fmt.Sprintf("wh-clickhouse0%d", i),
			User:          "rudder",
			Password:      "rudder-password",
			DBName:        "rudderdb",
			Secure:        "false",
			SkipVerify:    "true",
			TLSConfigName: "",
			Port:          "9000",
		}, true)
		require.NoError(t, err)

		err = db.Ping()
		require.NoError(t, err)

		clusterDBs = append(clusterDBs, db)
	}

	jobsDB := testhelper.SetUpJobsDB(t)
	tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}

	testCases := []struct {
		name            string
		sourceID        string
		destinationID   string
		writeKey        string
		userType        string
		db              *sql.DB
		warehouseEvents testhelper.EventsCountMap
		clusterSetup    func(t *testing.T)
	}{
		{
			name:            "Single Setup",
			sourceID:        "1wRvLmEnMOOxNM79pwaZhyCqXRE",
			destinationID:   "21Ev6TI6emCFDKph2Zn6XfTP7PI",
			writeKey:        "C5AWX39IVUWSP2NcHciWvqZTa2N",
			userType:        warehouseutils.CLICKHOUSE,
			db:              db,
			warehouseEvents: testhelper.DefaultWarehouseEventsMap(),
		},
		{
			name:            "Cluster Mode Setup",
			sourceID:        "1wRvLmEnMOOxNM79ghdZhyCqXRE",
			destinationID:   "21Ev6TI6emCFDKhp2Zn6XfTP7PI",
			writeKey:        "95RxRTZHWUsaD6HEdz0ThbXfQ6p",
			userType:        fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"),
			db:              clusterDBs[0],
			warehouseEvents: clusterWarehouseEventsMap(),
			clusterSetup: func(t *testing.T) {
				t.Helper()
				initializeClickhouseClusterMode(t, clusterDBs, tables)
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			warehouseTest := &testhelper.WareHouseTest{
				Client: &client.Client{
					SQL:  tc.db,
					Type: client.SQLClient,
				},
				WriteKey:      tc.writeKey,
				Schema:        "rudderdb",
				Tables:        tables,
				Provider:      warehouseutils.CLICKHOUSE,
				SourceID:      tc.sourceID,
				DestinationID: tc.destinationID,
			}

			// Scenario 1
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(tc.userType)

			sendEventsMap := testhelper.SendEventsMap()
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.DefaultStagingFilesEventsMap())
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.DefaultLoadFilesEventsMap())
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.DefaultTableUploadsEventsMap())
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.DefaultWarehouseEventsMap())

			// Scenario 2
			warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
			warehouseTest.UserId = testhelper.GetUserId(tc.userType)

			if tc.clusterSetup != nil {
				tc.clusterSetup(t)
			}

			sendEventsMap = testhelper.SendEventsMap()
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
			testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

			testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.DefaultStagingFilesEventsMap())
			testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.DefaultLoadFilesEventsMap())
			testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.DefaultTableUploadsEventsMap())
			testhelper.VerifyEventsInWareHouse(t, warehouseTest, tc.warehouseEvents)

			testhelper.VerifyWorkspaceIDInStats(t, statsToVerify...)
		})
	}
}

func TestClickhouseConfigurationValidation(t *testing.T) {
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

func initializeClickhouseClusterMode(t *testing.T, clusterDBs []*sql.DB, tables []string) {
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

func clusterWarehouseEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    8,
		"users":         2,
		"tracks":        8,
		"product_track": 8,
		"pages":         8,
		"screens":       8,
		"aliases":       8,
		"groups":        8,
	}
}
