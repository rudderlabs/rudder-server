//go:build warehouse_integration && !sources_integration

package mssql_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type TestHandle struct {
	DB       *sql.DB
	WriteKey string
	Schema   string
	Tables   []string
}

var handle *TestHandle

func (*TestHandle) VerifyConnection() error {
	return testhelper.WithConstantBackoff(func() (err error) {
		credentials := mssql.CredentialsT{
			DBName:   "master",
			Password: "reallyStrongPwd123",
			User:     "SA",
			Host:     "wh-mssql",
			SSLMode:  "disable",
			Port:     "1433",
		}
		if handle.DB, err = mssql.Connect(credentials); err != nil {
			err = fmt.Errorf("could not connect to warehouse mssql with error: %w", err)
			return
		}
		if err = handle.DB.Ping(); err != nil {
			err = fmt.Errorf("could not connect to warehouse mssql while pinging with error: %w", err)
			return
		}
		return
	})
}

func TestMSSQLIntegration(t *testing.T) {
	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  handle.DB,
			Type: client.SQLClient,
		},
		WriteKey:      handle.WriteKey,
		Schema:        handle.Schema,
		Tables:        handle.Tables,
		Provider:      warehouseutils.MSSQL,
		SourceID:      "1wRvLmEnMOONMbdspwaZhyCqXRE",
		DestinationID: "21Ezdq58khNMj07VJB0VJmxLvgu",
	}

	// Scenario 1
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.MSSQL)

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
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.MSSQL)

	sendEventsMap = testhelper.SendEventsMap()
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInStagingFiles(t, warehouseTest, testhelper.StagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, warehouseTest, testhelper.LoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, warehouseTest, testhelper.TableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())

	testhelper.VerifyWorkspaceIDInStats(t)
}

func TestMSSQLConfigurationValidation(t *testing.T) {
	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "21Ezdq58khNMj07VJB0VJmxLvgu",
		Config: map[string]interface{}{
			"host":             configurations["mssqlHost"],
			"database":         configurations["mssqlDatabase"],
			"user":             configurations["mssqlUser"],
			"password":         configurations["mssqlPassword"],
			"port":             configurations["mssqlPort"],
			"sslMode":          "disable",
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
			ID:          "1qvbUYC2xVQ7lvI9UUYkkM4IBt9",
			Name:        "MSSQL",
			DisplayName: "Microsoft SQL Server",
		},
		Name:       "mssql-demo",
		Enabled:    true,
		RevisionID: "29eeuUb21cuDBeFKPTUA9GaQ9Aq",
	}
	testhelper.VerifyingConfigurationTest(t, destination)
}

func TestMain(m *testing.M) {
	handle = &TestHandle{
		WriteKey: "YSQ3n267l1VQKGNbSuJE9fQbzON",
		Schema:   "mssql_wh_integration",
		Tables:   []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
	}
	os.Exit(testhelper.Run(m, handle))
}
