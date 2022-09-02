//go:build warehouse_integration

package clickhouse_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	"github.com/stretchr/testify/require"
)

type TestHandle struct {
	Schema          string
	Tables          []string
	WriteKey        string
	ClusterWriteKey string
	DB              *sql.DB
	ClusterDBs      []*sql.DB
}

var handle *TestHandle

// VerifyConnection test connection for clickhouse and clickhouse cluster
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

func checkScenarioOne(t *testing.T) {
	t.Helper()

	// Setting up the warehouseSingleSetupTest
	warehouseSingleSetupTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:             handle.WriteKey,
		Schema:               handle.Schema,
		Tables:               handle.Tables,
		EventsCountMap:       testhelper.DefaultEventMap(),
		TablesQueryFrequency: testhelper.DefaultQueryFrequency,
		UserId:               testhelper.GetUserId(warehouseutils.CLICKHOUSE),
		Provider:             warehouseutils.CLICKHOUSE,
	}

	// Scenario 1
	// Sending the first set of events.
	// Since we are sending unique message Ids. These should result in
	// These should result in events count will be equal to the number of events being sent
	testhelper.SendEvents(t, warehouseSingleSetupTest)
	testhelper.SendEvents(t, warehouseSingleSetupTest)
	testhelper.SendEvents(t, warehouseSingleSetupTest)
	testhelper.SendIntegratedEvents(t, warehouseSingleSetupTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	warehouseSingleSetupTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":    4,
		"users":         1,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
		"gateway":       24,
		"batchRT":       32,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseSingleSetupTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseSingleSetupTest)
	testhelper.VerifyingTablesEventCount(t, warehouseSingleSetupTest)

	require.NotNil(t, handle.ClusterDBs)
	require.NotNil(t, handle.ClusterDBs[0])

	// Setting up the warehouseClusterModeTest
	warehouseClusterModeTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.ClusterDBs[0],
			Type: client.SQLClient,
		},
		WriteKey:             handle.ClusterWriteKey,
		Schema:               handle.Schema,
		Tables:               handle.Tables,
		EventsCountMap:       testhelper.DefaultEventMap(),
		TablesQueryFrequency: testhelper.DefaultQueryFrequency,
		UserId:               testhelper.GetUserId(fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER")),
		Provider:             warehouseutils.CLICKHOUSE,
	}

	// Scenario 1
	// Sending the first set of events.
	// Since we are sending unique message Ids.
	// These should result in events count will be equal to the number of events being sent
	testhelper.SendEvents(t, warehouseClusterModeTest)
	testhelper.SendEvents(t, warehouseClusterModeTest)
	testhelper.SendEvents(t, warehouseClusterModeTest)
	testhelper.SendIntegratedEvents(t, warehouseClusterModeTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	warehouseClusterModeTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":    4,
		"users":         1,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
		"gateway":       24,
		"batchRT":       32,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseClusterModeTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseClusterModeTest)
	testhelper.VerifyingTablesEventCount(t, warehouseClusterModeTest)
}

func checkScenarioTwo(t *testing.T) {
	t.Helper()

	// Setting up the warehouseSingleSetupTest
	warehouseSingleSetupTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:             handle.WriteKey,
		Schema:               handle.Schema,
		Tables:               handle.Tables,
		EventsCountMap:       testhelper.DefaultEventMap(),
		TablesQueryFrequency: testhelper.DefaultQueryFrequency,
		UserId:               testhelper.GetUserId(warehouseutils.CLICKHOUSE),
		Provider:             warehouseutils.CLICKHOUSE,
	}

	// Scenario 2
	// Sending the second set of modified events.
	// Since we are sending unique message Ids.
	// These should result in events count will be equal to the number of events being sent
	warehouseSingleSetupTest.EventsCountMap = testhelper.DefaultEventMap()
	warehouseSingleSetupTest.UserId = testhelper.GetUserId(warehouseutils.CLICKHOUSE)
	testhelper.SendModifiedEvents(t, warehouseSingleSetupTest)
	testhelper.SendModifiedEvents(t, warehouseSingleSetupTest)
	testhelper.SendModifiedEvents(t, warehouseSingleSetupTest)
	testhelper.SendIntegratedEvents(t, warehouseSingleSetupTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	warehouseSingleSetupTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":    4,
		"users":         1,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
		"gateway":       24,
		"batchRT":       32,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseSingleSetupTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseSingleSetupTest)
	testhelper.VerifyingTablesEventCount(t, warehouseSingleSetupTest)

	require.NotNil(t, handle.ClusterDBs)
	require.NotNil(t, handle.ClusterDBs[0])

	// Setting up the warehouseClusterModeTest
	warehouseClusterModeTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.ClusterDBs[0],
			Type: client.SQLClient,
		},
		WriteKey:             handle.ClusterWriteKey,
		Schema:               handle.Schema,
		Tables:               handle.Tables,
		EventsCountMap:       testhelper.DefaultEventMap(),
		TablesQueryFrequency: testhelper.DefaultQueryFrequency,
		UserId:               testhelper.GetUserId(fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER")),
		Provider:             warehouseutils.CLICKHOUSE,
	}

	// Scenario 2
	// Setting up events count map
	// Setting up the UserID
	// Sending the second set of modified events.
	// Since we are sending unique message Ids.
	// These should result in events count will be equal to the number of events being sent
	warehouseClusterModeTest.EventsCountMap = testhelper.DefaultEventMap()
	warehouseClusterModeTest.UserId = testhelper.GetUserId(fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"))
	testhelper.SendModifiedEvents(t, warehouseClusterModeTest)
	testhelper.SendModifiedEvents(t, warehouseClusterModeTest)
	testhelper.SendModifiedEvents(t, warehouseClusterModeTest)
	testhelper.SendIntegratedEvents(t, warehouseClusterModeTest)

	// Setting up the events map
	// Checking for Gateway and Batch router events
	// Checking for the events count for each table
	// With the cluster mode setup, events are getting duplicated.
	warehouseClusterModeTest.EventsCountMap = testhelper.EventsCountMap{
		"identifies":    8,
		"users":         2,
		"tracks":        8,
		"product_track": 8,
		"pages":         8,
		"screens":       8,
		"aliases":       8,
		"groups":        8,
		"gateway":       24,
		"batchRT":       32,
	}
	testhelper.VerifyingGatewayEvents(t, warehouseClusterModeTest)
	testhelper.VerifyingBatchRouterEvents(t, warehouseClusterModeTest)
	testhelper.VerifyingTablesEventCount(t, warehouseClusterModeTest)
}

func TestClickHouseIntegration(t *testing.T) {
	t.Run("Load tables via download strategy", func(t *testing.T) {
		// Setting up the test configuration
		// Without S3 engine for loading
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.clickhouse.enabledS3EngineForLoading",
				Value: false,
			},
		}))
		checkScenarioOne(t)

		// Setting up the test configuration
		// With S3 engine for loading
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.clickhouse.enabledS3EngineForLoading",
				Value: true,
			},
		}))
		checkScenarioOne(t)

		// Initializing cluster mode setup
		initializeClickhouseClusterMode(t)

		// Setting up the test configuration
		// Without S3 engine for loading
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.clickhouse.enabledS3EngineForLoading",
				Value: false,
			},
		}))
		checkScenarioTwo(t)

		// Setting up the test configuration
		// With S3 engine for loading
		require.NoError(t, testhelper.SetConfig([]warehouseutils.KeyValue{
			{
				Key:   "Warehouse.clickhouse.enabledS3EngineForLoading",
				Value: true,
			},
		}))
		checkScenarioTwo(t)
	})
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
