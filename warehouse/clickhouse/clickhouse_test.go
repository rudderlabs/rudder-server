//go:build warehouse_integration && !sources_integration

package clickhouse_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type TestHandle struct {
	DB              *sql.DB
	ClusterDBs      []*sql.DB
	WriteKey        string
	ClusterWriteKey string
	Schema          string
	Tables          []string
}

var handle *TestHandle

var statsToVerify = []string{
	"warehouse_clickhouse_commitTimeouts",
	"warehouse_clickhouse_execTimeouts",
	"warehouse_clickhouse_failedRetries",
	"warehouse_clickhouse_syncLoadFileTime",
	"warehouse_clickhouse_downloadLoadFilesTime",
	"warehouse_clickhouse_numRowsLoadFile",
}

func (*TestHandle) VerifyConnection() error {
	err := testhelper.WithConstantBackoff(func() (err error) {
		credentials := clickhouse.CredentialsT{
			Host:          "wh-clickhouse",
			User:          "rudder",
			Password:      "rudder-password",
			DBName:        "rudderdb",
			Secure:        "false",
			SkipVerify:    "true",
			TLSConfigName: "",
			Port:          "9000",
		}
		if handle.DB, err = clickhouse.Connect(credentials, true); err != nil {
			err = fmt.Errorf("could not connect to warehouse clickhouse with error: %w", err)
			return
		}
		if err = handle.DB.Ping(); err != nil {
			err = fmt.Errorf("could not connect to warehouse clickhouse while pinging with error: %w", err)
			return
		}
		return
	})
	if err != nil {
		return fmt.Errorf("error while running test connection for clickhouse normal mode with err: %s", err.Error())
	}

	clusterCredentials := []struct {
		Credentials *clickhouse.CredentialsT
	}{
		{
			Credentials: &clickhouse.CredentialsT{
				Host:          "wh-clickhouse01",
				User:          "rudder",
				Password:      "rudder-password",
				DBName:        "rudderdb",
				Secure:        "false",
				SkipVerify:    "true",
				TLSConfigName: "",
				Port:          "9000",
			},
		},
		{
			Credentials: &clickhouse.CredentialsT{
				Host:          "wh-clickhouse02",
				User:          "rudder",
				Password:      "rudder-password",
				DBName:        "rudderdb",
				Secure:        "false",
				SkipVerify:    "true",
				TLSConfigName: "",
				Port:          "9000",
			},
		},
		{
			Credentials: &clickhouse.CredentialsT{
				Host:          "wh-clickhouse03",
				User:          "rudder",
				Password:      "rudder-password",
				DBName:        "rudderdb",
				Secure:        "false",
				SkipVerify:    "true",
				TLSConfigName: "",
				Port:          "9000",
			},
		},
		{
			Credentials: &clickhouse.CredentialsT{
				Host:          "wh-clickhouse04",
				User:          "rudder",
				Password:      "rudder-password",
				DBName:        "rudderdb",
				Secure:        "false",
				SkipVerify:    "true",
				TLSConfigName: "",
				Port:          "9000",
			},
		},
	}

	for i, chResource := range clusterCredentials {
		var clickhouseDB *sql.DB

		err = testhelper.WithConstantBackoff(func() (err error) {
			if clickhouseDB, err = clickhouse.Connect(*chResource.Credentials, true); err != nil {
				err = fmt.Errorf("could not connect to warehouse clickhouse cluster: %d with error: %w", i, err)
				return
			}
			if err = clickhouseDB.Ping(); err != nil {
				err = fmt.Errorf("could not connect to warehouse clickhouse cluster: %d while pinging with error: %w", i, err)
				return
			}
			return
		})
		if err != nil {
			return fmt.Errorf("error while running test connection for clickhouse cluster mode with err: %s", err.Error())
		}
		handle.ClusterDBs = append(handle.ClusterDBs, clickhouseDB)
	}
	return nil
}

func TestClickHouseIntegration(t *testing.T) {
	t.Run("Single Setup", func(t *testing.T) {
		t.Parallel()

		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				SQL:  handle.DB,
				Type: client.SQLClient,
			},
			WriteKey:      handle.WriteKey,
			Schema:        handle.Schema,
			Tables:        handle.Tables,
			Provider:      warehouseutils.CLICKHOUSE,
			SourceID:      "1wRvLmEnMOOxNM79pwaZhyCqXRE",
			DestinationID: "21Ev6TI6emCFDKph2Zn6XfTP7PI",
		}

		// Scenario 1
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.CLICKHOUSE)

		sendEventsMap := testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
		testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.CLICKHOUSE)

		sendEventsMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
		testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())

		testhelper.VerifyWorkspaceIDInStats(t, statsToVerify...)
	})

	t.Run("Cluster Mode Setup", func(t *testing.T) {
		t.Parallel()

		require.NotNil(t, handle.ClusterDBs)
		require.NotNil(t, handle.ClusterDBs[0])

		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				SQL:  handle.ClusterDBs[0],
				Type: client.SQLClient,
			},
			WriteKey:      handle.ClusterWriteKey,
			Schema:        handle.Schema,
			Tables:        handle.Tables,
			Provider:      warehouseutils.CLICKHOUSE,
			SourceID:      "1wRvLmEnMOOxNM79ghdZhyCqXRE",
			DestinationID: "21Ev6TI6emCFDKhp2Zn6XfTP7PI",
		}

		// Scenario 1
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"))

		sendEventsMap := testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
		testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"))

		initializeClickhouseClusterMode(t)

		sendEventsMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
		testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

		testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
		testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
		testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
		testhelper.VerifyEventsInWareHouse(t, warehouseTest, clusterWarehouseEventsMap())

		testhelper.VerifyWorkspaceIDInStats(t, statsToVerify...)
	})
}

func TestClickhouseConfigurationValidation(t *testing.T) {
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
	testhelper.VerifyingConfigurationTest(t, destination)
}

func initializeClickhouseClusterMode(t *testing.T) {
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
	require.NotNil(t, handle.ClusterDBs)
	require.NotNil(t, handle.ClusterDBs[0])

	clusterDB := handle.ClusterDBs[0]

	// Rename tables to tables_shard
	for _, table := range handle.Tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		log.Printf("Renaming tables to sharded tables for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		require.NoError(t, testhelper.WithConstantBackoff(func() error {
			_, err := clusterDB.Exec(sqlStatement)
			return err
		}))
	}

	// Create distribution views for tables
	for _, table := range handle.Tables {
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
	for _, clusterDB := range handle.ClusterDBs {
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

func TestMain(m *testing.M) {
	handle = &TestHandle{
		WriteKey:        "C5AWX39IVUWSP2NcHciWvqZTa2N",
		ClusterWriteKey: "95RxRTZHWUsaD6HEdz0ThbXfQ6p",
		Schema:          "rudderdb",
		Tables:          []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
