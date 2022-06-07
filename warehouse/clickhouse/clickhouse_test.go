package clickhouse_test

import (
	"database/sql"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

type NOOP struct{}

type ClickHouseTest struct {
	NOOP
	WriteKey           string
	Credentials        *clickhouse.CredentialsT
	DB                 *sql.DB
	EventsMap          testhelper.EventsCountMap
	TableTestQueryFreq time.Duration
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
	NOOP
	Resources          ClickHouseClusterResources
	EventsMap          testhelper.EventsCountMap
	WriteKey           string
	TableTestQueryFreq time.Duration
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

func (*NOOP) EnhanceWorkspaceConfig(configMap map[string]string) {
	configMap["clickHouseWriteKey"] = CHTest.WriteKey
	configMap["clickHousePort"] = CHTest.Credentials.Port
	configMap["clickHouseClusterWriteKey"] = CHClusterTest.WriteKey
	configMap["clickHouseClusterPort"] = CHClusterTest.GetResource().Credentials.Port
}

func (*NOOP) SetUpDestination() {
	SetUpClickHouseDestination()
	SetUpClickHouseClusterDestination()
}

func SetUpClickHouseDestination() {
	CHTest.WriteKey = testhelper.RandString(27)
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
	CHTest.EventsMap = testhelper.EventsCountMap{
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
	}
	CHTest.TableTestQueryFreq = 100 * time.Millisecond

	var err error
	if CHTest.DB, err = clickhouse.Connect(*CHTest.Credentials, true); err != nil {
		panic(fmt.Errorf("could not connect to warehouse clickhouse with error: %s", err.Error()))
	}
	if err = CHTest.DB.Ping(); err != nil {
		panic(fmt.Errorf("could not connect to warehouse clickhouse while pinging with error: %s", err.Error()))
	}
	return
}

func SetUpClickHouseClusterDestination() {
	CHClusterTest.WriteKey = testhelper.RandString(27)
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
	CHClusterTest.EventsMap = testhelper.EventsCountMap{
		"identifies": 1,
		"users":      1,
		"tracks":     1,
		"pages":      1,
		"screens":    1,
		"aliases":    1,
		"groups":     1,
		"gateway":    6,
		"batchRT":    8,
	}
	CHClusterTest.TableTestQueryFreq = 100 * time.Millisecond

	var err error
	for i, chResource := range CHClusterTest.Resources {
		if chResource.DB, err = clickhouse.Connect(*chResource.Credentials, true); err != nil {
			panic(fmt.Errorf("could not connect to warehouse clickhouse cluster: %d with error: %s", i, err.Error()))
		}
		if err = chResource.DB.Ping(); err != nil {
			panic(fmt.Errorf("could not connect to warehouse clickhouse cluster: %d while pinging with error: %s", i, err.Error()))
		}
	}
	return
}

func TestClickHouseIntegration(t *testing.T) {
	t.Parallel()

	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  CHTest.DB,
			Type: client.SQLClient,
		},
		EventsCountMap:           CHTest.EventsMap,
		WriteKey:                 CHTest.WriteKey,
		UserId:                   fmt.Sprintf("userId_clickhouse_%s", randomness),
		Event:                    fmt.Sprintf("Product Track %s", randomness),
		Schema:                   "rudderdb",
		VerifyingTablesFrequency: CHTest.TableTestQueryFreq,
	}
	whDestTest.Tables = []string{"identifies", "users", "tracks", strcase.ToSnake(whDestTest.Event), "pages", "screens", "aliases", "groups"}
	whDestTest.PrimaryKeys = []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"}
	whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1

	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_clickhouse_%s", randomness)
	whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1
	whDestTest.Tables = []string{"identifies", "users", "tracks", strcase.ToSnake(whDestTest.Event), "pages", "screens", "aliases", "groups"}
	whDestTest.PrimaryKeys = []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"}
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

	randomness := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	whDestTest := &testhelper.WareHouseDestinationTest{
		Client: &client.Client{
			SQL:  CHClusterTest.GetResource().DB,
			Type: client.SQLClient,
		},
		EventsCountMap:           CHClusterTest.EventsMap,
		WriteKey:                 CHClusterTest.WriteKey,
		UserId:                   fmt.Sprintf("userId_clickhouse_cluster_%s", randomness),
		Event:                    "Product Track",
		Schema:                   "rudderdb",
		VerifyingTablesFrequency: CHClusterTest.TableTestQueryFreq,
	}
	whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1
	whDestTest.Tables = []string{"identifies", "users", "tracks", strcase.ToSnake(whDestTest.Event), "pages", "screens", "aliases", "groups"}
	whDestTest.PrimaryKeys = []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"}

	testhelper.SendEvents(t, whDestTest)
	testhelper.VerifyingDestination(t, whDestTest)

	randomness = strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	whDestTest.UserId = fmt.Sprintf("userId_clickhouse_cluster%s", randomness)
	whDestTest.Event = "Product Track"
	whDestTest.EventsCountMap[strcase.ToSnake(whDestTest.Event)] = 1
	whDestTest.Tables = []string{"identifies", "users", "tracks", strcase.ToSnake(whDestTest.Event), "pages", "screens", "aliases", "groups"}
	whDestTest.PrimaryKeys = []string{"user_id", "id", "user_id", "user_id", "user_id", "user_id", "user_id", "user_id"}

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
	NOOP := &NOOP{}
	os.Exit(testhelper.Setup(m, NOOP))
}
