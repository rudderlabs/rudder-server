package clickhouse_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type TestHandle struct {
	DB               *sql.DB
	EventsMap        testhelper.EventsCountMap
	WriteKey         string
	ClusterDBs       []*sql.DB
	ClusterEventsMap testhelper.EventsCountMap
	ClusterWriteKey  string
}

var handle *TestHandle

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

	tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}
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
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		log.Printf("Renaming tables to sharded tables for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		_, err := clusterDB.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}

	// Create distribution views for tables
	for _, table := range tables {
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

	t.Run("Normal Mode", func(t *testing.T) {
		whDestTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				SQL:  handle.DB,
				Type: client.SQLClient,
			},
			WriteKey:                 handle.WriteKey,
			Schema:                   "rudderdb",
			EventsCountMap:           handle.EventsMap,
			VerifyingTablesFrequency: testhelper.DefaultQueryFrequency,
		}

		whDestTest.SetUserId(warehouseutils.CLICKHOUSE)
		testhelper.SendEvents(t, whDestTest)
		testhelper.VerifyingDestination(t, whDestTest)

		whDestTest.SetUserId(warehouseutils.CLICKHOUSE)
		testhelper.SendModifiedEvents(t, whDestTest)
		testhelper.VerifyingDestination(t, whDestTest)
	})
	t.Run("Cluster Mode", func(t *testing.T) {
		warehouseTest := &testhelper.WareHouseTest{
			Client: &client.Client{
				SQL:  handle.ClusterDBs[0],
				Type: client.SQLClient,
			},
			WriteKey:                 handle.ClusterWriteKey,
			Schema:                   "rudderdb",
			EventsCountMap:           handle.ClusterEventsMap,
			VerifyingTablesFrequency: testhelper.DefaultQueryFrequency,
		}

		warehouseTest.SetUserId(fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"))
		testhelper.SendEvents(t, warehouseTest)
		testhelper.VerifyingDestination(t, warehouseTest)

		warehouseTest.SetUserId(fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"))
		initializeClickhouseClusterMode(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)

		// Update events count Map
		// This is required as because of the cluster mode setup and distributed view, events are getting duplicated.
		warehouseTest.EventsCountMap = testhelper.EventsCountMap{
			"identifies":    2,
			"users":         2,
			"tracks":        2,
			"product_track": 2,
			"pages":         2,
			"screens":       2,
			"aliases":       2,
			"groups":        2,
			"gateway":       6,
			"batchRT":       8,
		}
		testhelper.VerifyingDestination(t, warehouseTest)
	})
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		WriteKey:         "C5AWX39IVUWSP2NcHciWvqZTa2N",
		EventsMap:        testhelper.DefaultEventMap(),
		ClusterWriteKey:  "95RxRTZHWUsaD6HEdz0ThbXfQ6p",
		ClusterEventsMap: testhelper.DefaultEventMap(),
	}
	os.Exit(testhelper.Run(m, handle))
}
