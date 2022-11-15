//go:build warehouse_integration && !sources_integration

package mssql_test

import (
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMSSQLIntegration(t *testing.T) {
	t.Parallel()

	mssql.Init()

	db, err := mssql.Connect(mssql.CredentialsT{
		DBName:   "master",
		Password: "reallyStrongPwd123",
		User:     "SA",
		Host:     "wh-mssql",
		SSLMode:  "disable",
		Port:     "1433",
	})
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	jobsDB := testhelper.SetUpJobsDB(t)

	warehouseTest := &testhelper.WareHouseTest{
		Client: &client.Client{
			SQL:  db,
			Type: client.SQLClient,
		},
		WriteKey:      "YSQ3n267l1VQKGNbSuJE9fQbzON",
		Schema:        "mssql_wh_integration",
		Tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
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

	testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.StagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.LoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.TableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())

	// Scenario 2
	warehouseTest.TimestampBeforeSendingEvents = timeutil.Now()
	warehouseTest.UserId = testhelper.GetUserId(warehouseutils.MSSQL)

	sendEventsMap = testhelper.SendEventsMap()
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendModifiedEvents(t, warehouseTest, sendEventsMap)
	testhelper.SendIntegratedEvents(t, warehouseTest, sendEventsMap)

	testhelper.VerifyEventsInStagingFiles(t, jobsDB, warehouseTest, testhelper.StagingFilesEventsMap())
	testhelper.VerifyEventsInLoadFiles(t, jobsDB, warehouseTest, testhelper.LoadFilesEventsMap())
	testhelper.VerifyEventsInTableUploads(t, jobsDB, warehouseTest, testhelper.TableUploadsEventsMap())
	testhelper.VerifyEventsInWareHouse(t, warehouseTest, testhelper.WarehouseEventsMap())

	testhelper.VerifyWorkspaceIDInStats(t)
}

func TestMSSQLConfigurationValidation(t *testing.T) {
	t.Parallel()

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	mssql.Init()

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
	testhelper.VerifyConfigurationTest(t, destination)
}
