package integration__tests

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/integration__tests/testhelper"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
)

// SetupClickHouseCluster setup warehouse clickhouse cluster mode destination
func SetupClickHouseCluster() (chClusterTest *testhelper.ClickHouseClusterTest) {
	chClusterTest = &testhelper.ClickHouseClusterTest{
		WriteKey: testhelper.RandString(27),
		EventsMap: testhelper.EventsCountMap{
			"identifies":    1,
			"users":         1,
			"tracks":        1,
			"product_track": 1,
			"pages":         1,
			"screens":       1,
			"aliases":       1,
			"groups":        1,
			"gateway":       6,
			"batchRT":       8,
		},
		Resources: []*testhelper.ClickHouseClusterResource{
			{
				Name:     "clickhouse01",
				HostName: "clickhouse01",
				Credentials: &clickhouse.CredentialsT{
					Host:          "localhost",
					User:          "rudder",
					Password:      "rudder-password",
					DBName:        "rudderdb",
					Secure:        "false",
					SkipVerify:    "true",
					TLSConfigName: "",
					Port:          "54324",
				},
			},
			{
				Name:     "clickhouse02",
				HostName: "clickhouse02",
				Credentials: &clickhouse.CredentialsT{
					Host:          "localhost",
					User:          "rudder",
					Password:      "rudder-password",
					DBName:        "rudderdb",
					Secure:        "false",
					SkipVerify:    "true",
					TLSConfigName: "",
					Port:          "54325",
				},
			},
			{
				Name:     "clickhouse03",
				HostName: "clickhouse03",
				Credentials: &clickhouse.CredentialsT{
					Host:          "localhost",
					User:          "rudder",
					Password:      "rudder-password",
					DBName:        "rudderdb",
					Secure:        "false",
					SkipVerify:    "true",
					TLSConfigName: "",
					Port:          "54326",
				},
			},
			{
				Name:     "clickhouse04",
				HostName: "clickhouse04",
				Credentials: &clickhouse.CredentialsT{
					Host:          "localhost",
					User:          "rudder",
					Password:      "rudder-password",
					DBName:        "rudderdb",
					Secure:        "false",
					SkipVerify:    "true",
					TLSConfigName: "",
					Port:          "54327",
				},
			},
		},
		TableTestQueryFreq: 100 * time.Millisecond,
	}

	var err error
	for i, chResource := range chClusterTest.Resources {
		if chResource.DB, err = clickhouse.Connect(*chResource.Credentials, true); err != nil {
			panic(fmt.Errorf("could not connect to warehouse clickhouse cluster: %d with error: %s", i, err.Error()))
		}
		if err = chResource.DB.Ping(); err != nil {
			panic(fmt.Errorf("could not connect to warehouse clickhouse cluster: %d while pinging with error: %s", i, err.Error()))
		}
	}
	return
}

// initializeClickhouseClusterMode Initialize cluster mode setup
func initializeClickhouseClusterMode(t *testing.T) {
	type ColumnInfoT struct {
		ColumnName string
		ColumnType string
	}

	chClusterTest := CHClusterTest
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

	// Rename tables to tables_shard
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		_, err := chClusterTest.GetResource().DB.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}

	// Create distribution views for tables
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("CREATE TABLE rudderdb.%[1]s ON CLUSTER 'rudder_cluster' AS rudderdb.%[1]s_shard ENGINE = Distributed('rudder_cluster', rudderdb, %[1]s_shard, cityHash64(concat(toString(received_at), id)));", table)
		_, err := chClusterTest.GetResource().DB.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}

	// Alter columns to all the cluster tables
	for _, chResource := range chClusterTest.Resources {
		for tableName, columnInfos := range tableColumnInfoMap {
			for _, columnInfo := range columnInfos {
				sqlStatement := fmt.Sprintf("ALTER TABLE rudderdb.%[1]s_shard ADD COLUMN IF NOT EXISTS %[2]s %[3]s;", tableName, columnInfo.ColumnName, columnInfo.ColumnType)
				_, err := chResource.DB.Exec(sqlStatement)
				require.Equal(t, err, nil)
			}
		}
	}
}

func TestClickHouseCluster(t *testing.T) {
	t.Parallel()

	chClusterTest := CHClusterTest

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  chClusterTest.GetResource().DB,
			Type: client.SQLClient,
		},
		EventsCountMap:     chClusterTest.EventsMap,
		WriteKey:           chClusterTest.WriteKey,
		UserId:             "userId_clickhouse_cluster",
		Schema:             "rudderdb",
		TableTestQueryFreq: chClusterTest.TableTestQueryFreq,
	}
	sendEvents(whDestTest)
	destinationTest(t, whDestTest)

	initializeClickhouseClusterMode(t)

	whDestTest.UserId = "userId_clickhouse_cluster_1"
	sendUpdatedEvents(whDestTest)

	// Update events count Map
	// This is required as because of the cluster mode setup and distributed view, events are getting duplicated.
	whDestTest.EventsCountMap = testhelper.EventsCountMap{
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
	destinationTest(t, whDestTest)
}
