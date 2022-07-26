package clickhouse_test

import (
	"database/sql"
	"fmt"
	"github.com/gofrs/uuid"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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

var (
	handle *TestHandle
)

// TestConnection test connection for clickhouse and clickhouse cluster
func (*TestHandle) TestConnection() error {
	err := testhelper.ConnectWithBackoff(func() (err error) {
		credentials := clickhouse.CredentialsT{
			Host:          "clickhouse",
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
				Host:          "clickhouse01",
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
				Host:          "clickhouse02",
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
				Host:          "clickhouse03",
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
				Host:          "clickhouse04",
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

		err = testhelper.ConnectWithBackoff(func() (err error) {
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

func initializeClickhouseClusterMode(t *testing.T, whDestTest *testhelper.WareHouseTest) {
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
	clusterDB := handle.ClusterDBs[0]

	// Rename tables to tables_shard
	for _, table := range handle.Tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		log.Printf("Renaming tables to sharded tables for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		_, err := clusterDB.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}

	// Create distribution views for tables
	for _, table := range handle.Tables {
		sqlStatement := fmt.Sprintf("CREATE TABLE rudderdb.%[1]s ON CLUSTER 'rudder_cluster' AS rudderdb.%[1]s_shard ENGINE = Distributed('rudder_cluster', rudderdb, %[1]s_shard, cityHash64(concat(toString(received_at), id)));", table)
		log.Printf("Creating distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		_, err := clusterDB.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}

	// Alter columns to all the cluster tables
	for _, clusterDB := range handle.ClusterDBs {
		for tableName, columnInfos := range tableColumnInfoMap {
			for _, columnInfo := range columnInfos {
				sqlStatement := fmt.Sprintf("ALTER TABLE rudderdb.%[1]s_shard ADD COLUMN IF NOT EXISTS %[2]s %[3]s;", tableName, columnInfo.ColumnName, columnInfo.ColumnType)
				log.Printf("Altering columns for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

				_, err := clusterDB.Exec(sqlStatement)
				require.Equal(t, err, nil)
			}
		}
	}
}

func TestClickHouseClusterIntegration(t *testing.T) {
	t.Parallel()

	t.Run("Single Setup", func(t *testing.T) {
		// Setting up the warehouseTest
		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				SQL:  handle.DB,
				Type: client.SQLClient,
			},
			WriteKey:                 handle.WriteKey,
			Schema:                   handle.Schema,
			Tables:                   handle.Tables,
			EventsCountMap:           testhelper.DefaultEventMap(),
			VerifyingTablesFrequency: testhelper.DefaultQueryFrequency,
			UserId:                   fmt.Sprintf("userId_%s_%s", strings.ToLower(warehouseutils.CLICKHOUSE), strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")),
		}

		// Scenario 1
		// Sending the first set of events.
		// Since we are sending unique message Ids. These should result in
		// These should result in events count will be equal to the number of events being sent
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)

		// Setting up the events map
		// Checking for Gateway and Batch router events
		// Checking for the events count for each table
		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
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
		testhelper.VerifyingGatewayEvents(t, warehouseTest)
		testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
		testhelper.VerifyingTablesEventCount(t, warehouseTest)

		// Scenario 2
		// Sending the second set of modified events.
		// Since we are sending unique message Ids.
		// These should result in events count will be equal to the number of events being sent
		warehouseTest.EventsCountMap = testhelper.DefaultEventMap()
		warehouseTest.UserId = fmt.Sprintf("userId_%s_%s", strings.ToLower(warehouseutils.CLICKHOUSE), strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""))
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)

		// Setting up the events map
		// Checking for Gateway and Batch router events
		// Checking for the events count for each table
		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
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
		testhelper.VerifyingGatewayEvents(t, warehouseTest)
		testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
		testhelper.VerifyingTablesEventCount(t, warehouseTest)
	})

	t.Run("Cluster Mode Setup", func(t *testing.T) {
		require.NotNil(t, handle.ClusterDBs)
		require.NotNil(t, handle.ClusterDBs[0])

		// Setting up the warehouseTest
		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				SQL:  handle.ClusterDBs[0],
				Type: client.SQLClient,
			},
			WriteKey:                 handle.ClusterWriteKey,
			Schema:                   handle.Schema,
			Tables:                   handle.Tables,
			EventsCountMap:           testhelper.DefaultEventMap(),
			VerifyingTablesFrequency: testhelper.DefaultQueryFrequency,
			UserId:                   fmt.Sprintf("userId_%s_%s", fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"), strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")),
		}

		// Scenario 1
		// Sending the first set of events.
		// Since we are sending unique message Ids.
		// These should result in events count will be equal to the number of events being sent
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)

		// Setting up the events map
		// Checking for Gateway and Batch router events
		// Checking for the events count for each table
		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
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
		testhelper.VerifyingGatewayEvents(t, warehouseTest)
		testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
		testhelper.VerifyingTablesEventCount(t, warehouseTest)

		// Scenario 2
		// Setting up events count map
		// Setting up the UserID
		// Initializing cluster mode setup
		// Sending the second set of modified events.
		// Since we are sending unique message Ids.
		// These should result in events count will be equal to the number of events being sent
		warehouseTest.EventsCountMap = testhelper.DefaultEventMap()
		warehouseTest.UserId = fmt.Sprintf("userId_%s_%s", fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"), strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""))
		initializeClickhouseClusterMode(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)

		// Setting up the events map
		// Checking for Gateway and Batch router events
		// Checking for the events count for each table
		// With the cluster mode setup, events are getting duplicated.
		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
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
		testhelper.VerifyingGatewayEvents(t, warehouseTest)
		testhelper.VerifyingBatchRouterEvents(t, warehouseTest)
		testhelper.VerifyingTablesEventCount(t, warehouseTest)
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
