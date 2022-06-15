package clickhouse_test

import (
	"database/sql"
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"testing"
)

type CHNOOP struct{}

type ClickHouseTest struct {
	CHNOOP
	Credentials *clickhouse.CredentialsT
	DB          *sql.DB
	EventsMap   testhelper.EventsCountMap
	WriteKey    string
}

type ClickHouseClusterResource struct {
	Name        string
	HostName    string
	IPAddress   string
	Credentials *clickhouse.CredentialsT
	Port        string
	DB          *sql.DB
}

type ClickHouseClusterResources []*ClickHouseClusterResource

type ClickHouseClusterTest struct {
	CHNOOP
	Resources ClickHouseClusterResources
	EventsMap testhelper.EventsCountMap
	WriteKey  string
}

var (
	CHTest        *ClickHouseTest
	CHClusterTest *ClickHouseClusterTest
)

func (resources *ClickHouseClusterTest) GetResource() *ClickHouseClusterResource {
	if len(resources.Resources) == 0 {
		log.Panic("No such clickhouse cluster resource available.")
	}
	return resources.Resources[0]
}

func (*CHNOOP) SetUpDestination() {
	SetUpClickHouseDestination()
	SetUpClickHouseClusterDestination()
}

func SetUpClickHouseDestination() {
	CHTest.WriteKey = "C5AWX39IVUWSP2NcHciWvqZTa2N"
	CHTest.Credentials = &clickhouse.CredentialsT{
		Host:          "localhost",
		User:          "rudder",
		Password:      "rudder-password",
		DBName:        "rudderdb",
		Secure:        "false",
		SkipVerify:    "true",
		TLSConfigName: "",
		Port:          "54321",
	}
	CHTest.EventsMap = testhelper.DefaultEventMap()

	testhelper.ConnectWithBackoff(func() (err error) {
		if CHTest.DB, err = clickhouse.Connect(*CHTest.Credentials, true); err != nil {
			err = fmt.Errorf("could not connect to warehouse clickhouse with error: %w", err)
			return
		}
		if err = CHTest.DB.Ping(); err != nil {
			err = fmt.Errorf("could not connect to warehouse clickhouse while pinging with error: %w", err)
			return
		}
		return
	})
}

func SetUpClickHouseClusterDestination() {
	CHClusterTest.WriteKey = "95RxRTZHWUsaD6HEdz0ThbXfQ6p"
	CHClusterTest.Resources = []*ClickHouseClusterResource{
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
	}
	CHClusterTest.EventsMap = testhelper.DefaultEventMap()

	for i, chResource := range CHClusterTest.Resources {
		testhelper.ConnectWithBackoff(func() (err error) {
			if chResource.DB, err = clickhouse.Connect(*chResource.Credentials, true); err != nil {
				err = fmt.Errorf("could not connect to warehouse clickhouse cluster: %d with error: %w", i, err)
				return
			}
			if err = chResource.DB.Ping(); err != nil {
				err = fmt.Errorf("could not connect to warehouse clickhouse cluster: %d while pinging with error: %w", i, err)
				return
			}
			return
		})
	}
}

func TestClickHouseIntegration(t *testing.T) {
	t.Parallel()

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  CHTest.DB,
			Type: client.SQLClient,
		},
		WriteKey:                 CHTest.WriteKey,
		Schema:                   "rudderdb",
		EventsCountMap:           CHTest.EventsMap,
		VerifyingTablesFrequency: testhelper.DefaultQueryFrequency,
	}

	whDestTest.Reset(warehouseutils.CLICKHOUSE, true)
	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	whDestTest.Reset(warehouseutils.CLICKHOUSE, true)
	testhelper.SendModifiedEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)
}

func initializeClickhouseClusterMode(t *testing.T, whDestTest *testhelper.WareHouseDestinationTest) {
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

	// Rename tables to tables_shard
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("RENAME TABLE %[1]s to %[1]s_shard ON CLUSTER rudder_cluster;", table)
		log.Printf("Renaming tables to sharded tables for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		_, err := CHClusterTest.GetResource().DB.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}

	// Create distribution views for tables
	for _, table := range tables {
		sqlStatement := fmt.Sprintf("CREATE TABLE rudderdb.%[1]s ON CLUSTER 'rudder_cluster' AS rudderdb.%[1]s_shard ENGINE = Distributed('rudder_cluster', rudderdb, %[1]s_shard, cityHash64(concat(toString(received_at), id)));", table)
		log.Printf("Creating distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

		_, err := CHClusterTest.GetResource().DB.Exec(sqlStatement)
		require.Equal(t, err, nil)
	}

	// Alter columns to all the cluster tables
	for _, chResource := range CHClusterTest.Resources {
		for tableName, columnInfos := range tableColumnInfoMap {
			for _, columnInfo := range columnInfos {
				sqlStatement := fmt.Sprintf("ALTER TABLE rudderdb.%[1]s_shard ADD COLUMN IF NOT EXISTS %[2]s %[3]s;", tableName, columnInfo.ColumnName, columnInfo.ColumnType)
				log.Printf("Altering columns for distribution view for clickhouse cluster with sqlStatement: %s", sqlStatement)

				_, err := chResource.DB.Exec(sqlStatement)
				require.Equal(t, err, nil)
			}
		}
	}
}

func TestClickHouseClusterIntegration(t *testing.T) {
	t.Parallel()

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  CHClusterTest.GetResource().DB,
			Type: client.SQLClient,
		},
		WriteKey:                 CHClusterTest.WriteKey,
		Schema:                   "rudderdb",
		EventsCountMap:           CHClusterTest.EventsMap,
		VerifyingTablesFrequency: testhelper.DefaultQueryFrequency,
	}

	whDestTest.Reset(fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"), false)
	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	whDestTest.Reset(fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"), false)
	initializeClickhouseClusterMode(t, whDestTest)
	testhelper.SendModifiedEvents(t, whDestTest)

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
	testhelper.VerifyingDestination(t, whDestTest)
}

func TestMain(m *testing.M) {
	CHTest = &ClickHouseTest{}
	CHClusterTest = &ClickHouseClusterTest{}
	NOOP := &CHNOOP{}
	os.Exit(testhelper.Run(m, NOOP))
}
