//go:build warehouse_integration

package clickhouse_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

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

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.WarehouseEventsMap()
		testhelper.VerifyingEventsInWareHouse(t, warehouseTest)

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(warehouseutils.CLICKHOUSE)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.WarehouseEventsMap()
		testhelper.VerifyingEventsInWareHouse(t, warehouseTest)
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

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.WarehouseEventsMap()
		testhelper.VerifyingEventsInWareHouse(t, warehouseTest)

		// Scenario 2
		warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
		warehouseTest.UserId = testhelper.GetUserId(fmt.Sprintf("%s_%s", warehouseutils.CLICKHOUSE, "CLUSTER"))

		initializeClickhouseClusterMode(t)

		warehouseTest.EventsCountMap = testhelper.SendEventsMap()
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendModifiedEvents(t, warehouseTest)
		testhelper.SendIntegratedEvents(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.StagingFilesEventsMap()
		testhelper.VerifyingEventsInStagingFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.LoadFilesEventsMap()
		testhelper.VerifyingEventsInLoadFiles(t, warehouseTest)

		warehouseTest.EventsCountMap = testhelper.TableUploadsEventsMap()
		testhelper.VerifyingEventsInTableUploads(t, warehouseTest)

		warehouseTest.EventsCountMap = clusterWarehoseEventsMap()
		testhelper.VerifyingEventsInWareHouse(t, warehouseTest)
	})
}

func clusterWarehoseEventsMap() testhelper.EventsCountMap {
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
