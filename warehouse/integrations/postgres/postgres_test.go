package postgres_test

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"

	"github.com/rudderlabs/rudder-server/warehouse/tunnelling"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIntegrationPostgresThroughTunnelling(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	encoding.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	tunnelInfoConfig := map[string]interface{}{
		"sshUser":       configurations["sshUser"],
		"sshPort":       configurations["sshPort"],
		"sshHost":       configurations["sshHost"],
		"sshPrivateKey": strings.ReplaceAll(configurations["sshPrivateKey"], "\\n", "\n"),
	}

	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		configurations["privatePostgresUser"], configurations["privatePostgresPassword"], configurations["privatePostgresHost"], configurations["privatePostgresPort"], configurations["privatePostgresDatabase"],
	)
	db, err := tunnelling.SQLConnectThroughTunnel(dsn, tunnelInfoConfig)
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	jobsDB := testhelper.SetUpJobsDB(t)

	testCases := []struct {
		name      string
		useLegacy bool
	}{
		{
			name:      "Legacy",
			useLegacy: true,
		},
		{
			name:      "New",
			useLegacy: false,
		},
	}

	for _, tc := range testCases {
		tc := tc

		testhelper.SetConfig(t, []warehouseutils.KeyValue{
			{
				Key:   "Warehouse.postgres.useLegacy",
				Value: strconv.FormatBool(tc.useLegacy),
			},
		})

		subTestcases := []struct {
			name                  string
			writeKey              string
			schema                string
			sourceID              string
			destinationID         string
			eventsMap             testhelper.EventsCountMap
			stagingFilesEventsMap testhelper.EventsCountMap
			loadFilesEventsMap    testhelper.EventsCountMap
			tableUploadsEventsMap testhelper.EventsCountMap
			warehouseEventsMap    testhelper.EventsCountMap
			asyncJob              bool
			tables                []string
		}{
			{
				name:          "upload job through ssh tunnelling",
				writeKey:      "kwzDkh9h2fhfUVuS9jZ8uVbhV3w",
				schema:        "postgres_wh_ssh_tunnelled_integration",
				tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				sourceID:      "1wRvLmEnMOOxSQD9pwaZhyCqXRF",
				destinationID: "216ZvbavR21Um6eGKQCagZHqLGZ",
			},
		}

		for _, stc := range subTestcases {
			stc := stc

			t.Run(tc.name+" "+stc.name, func(t *testing.T) {
				ts := testhelper.WareHouseTest{
					Schema:                stc.schema,
					WriteKey:              stc.writeKey,
					SourceID:              stc.sourceID,
					DestinationID:         stc.destinationID,
					Tables:                stc.tables,
					EventsMap:             stc.eventsMap,
					StagingFilesEventsMap: stc.stagingFilesEventsMap,
					LoadFilesEventsMap:    stc.loadFilesEventsMap,
					TableUploadsEventsMap: stc.tableUploadsEventsMap,
					WarehouseEventsMap:    stc.warehouseEventsMap,
					UserID:                testhelper.GetUserId(warehouseutils.POSTGRES),
					Provider:              warehouseutils.POSTGRES,
					JobsDB:                jobsDB,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StatsToVerify:         []string{"pg_rollback_timeout"},
					Client: &client.Client{
						SQL:  db,
						Type: client.SQLClient,
					},
				}
				ts.VerifyEvents(t)

				ts.UserID = testhelper.GetUserId(warehouseutils.POSTGRES)
				ts.JobRunID = misc.FastUUID().String()
				ts.TaskRunID = misc.FastUUID().String()
				ts.VerifyModifiedEvents(t)
			})
		}
	}
}

func TestIntegrationPostgres(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		"rudder", "rudder-password", "wh-postgres", "5432", "rudderdb",
	)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	var (
		provider = warehouseutils.POSTGRES
		jobsDB   = testhelper.SetUpJobsDB(t)
	)

	subTestcase := []struct {
		name                  string
		writeKey              string
		schema                string
		sourceID              string
		destinationID         string
		eventsMap             testhelper.EventsCountMap
		stagingFilesEventsMap testhelper.EventsCountMap
		loadFilesEventsMap    testhelper.EventsCountMap
		tableUploadsEventsMap testhelper.EventsCountMap
		warehouseEventsMap    testhelper.EventsCountMap
		asyncJob              bool
		tables                []string
	}{
		{
			name:          "Upload Job",
			writeKey:      "kwzDkh9h2fhfUVuS9jZ8uVbhV3v",
			schema:        "postgres_wh_integration",
			tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
			sourceID:      "1wRvLmEnMOOxSQD9pwaZhyCqXRE",
			destinationID: "216ZvbavR21Um6eGKQCagZHqLGZ",
		},
		{
			name:                  "Async Job",
			writeKey:              "2DkCpXZcEvJK2fcpUD3LmjPI7J6",
			schema:                "postgres_wh_sources_integration",
			tables:                []string{"tracks", "google_sheet"},
			sourceID:              "2DkCpUr0xfiGBPJxIwqyqfyHdq4",
			destinationID:         "308ZvbavR21Um6eGKQCagZHqLGZ",
			eventsMap:             testhelper.SourcesSendEventsMap(),
			stagingFilesEventsMap: testhelper.SourcesStagingFilesEventsMap(),
			loadFilesEventsMap:    testhelper.SourcesLoadFilesEventsMap(),
			tableUploadsEventsMap: testhelper.SourcesTableUploadsEventsMap(),
			warehouseEventsMap:    testhelper.SourcesWarehouseEventsMap(),
			asyncJob:              true,
		},
	}

	testCases := []struct {
		name      string
		useLegacy bool
	}{
		{
			name:      "Legacy",
			useLegacy: true,
		},
		{
			name:      "New",
			useLegacy: false,
		},
	}

	for _, tc := range testCases {
		tc := tc

		testhelper.SetConfig(t, []warehouseutils.KeyValue{
			{
				Key:   "Warehouse.postgres.useLegacy",
				Value: strconv.FormatBool(tc.useLegacy),
			},
		})

		for _, stc := range subTestcase {
			stc := stc

			t.Run(tc.name+" "+stc.name, func(t *testing.T) {
				ts := testhelper.WareHouseTest{
					Schema:                stc.schema,
					WriteKey:              stc.writeKey,
					SourceID:              stc.sourceID,
					DestinationID:         stc.destinationID,
					Tables:                stc.tables,
					EventsMap:             stc.eventsMap,
					StagingFilesEventsMap: stc.stagingFilesEventsMap,
					LoadFilesEventsMap:    stc.loadFilesEventsMap,
					TableUploadsEventsMap: stc.tableUploadsEventsMap,
					WarehouseEventsMap:    stc.warehouseEventsMap,
					AsyncJob:              stc.asyncJob,
					UserID:                testhelper.GetUserId(provider),
					Provider:              provider,
					JobsDB:                jobsDB,
					JobRunID:              misc.FastUUID().String(),
					TaskRunID:             misc.FastUUID().String(),
					StatsToVerify:         []string{"pg_rollback_timeout"},
					Client: &client.Client{
						SQL:  db,
						Type: client.SQLClient,
					},
				}
				ts.VerifyEvents(t)

				if !stc.asyncJob {
					ts.UserID = testhelper.GetUserId(provider)
				}
				ts.JobRunID = misc.FastUUID().String()
				ts.TaskRunID = misc.FastUUID().String()
				ts.VerifyModifiedEvents(t)
			})
		}
	}
}

func TestConfigurationValidationPostgres(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	encoding.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "216ZvbavR21Um6eGKQCagZHqLGZ",
		Config: map[string]interface{}{
			"host":             configurations["postgresHost"],
			"database":         configurations["postgresDatabase"],
			"user":             configurations["postgresUser"],
			"password":         configurations["postgresPassword"],
			"port":             configurations["postgresPort"],
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
			ID:          "1bJ4YC7INdkvBTzotNh0zta5jDm",
			Name:        "POSTGRES",
			DisplayName: "Postgres",
		},
		Name:       "postgres-demo",
		Enabled:    true,
		RevisionID: "29eeuu9kywWsRAybaXcxcnTVEl8",
	}

	testCases := []struct {
		name      string
		useLegacy bool
	}{
		{
			name:      "Legacy",
			useLegacy: true,
		},
		{
			name:      "New",
			useLegacy: false,
		},
	}

	for _, tc := range testCases {
		tc := tc

		testhelper.SetConfig(t, []warehouseutils.KeyValue{
			{
				Key:   "Warehouse.postgres.useLegacy",
				Value: strconv.FormatBool(tc.useLegacy),
			},
		})

		t.Run(tc.name, func(t *testing.T) {
			testhelper.VerifyConfigurationTest(t, destination)
		})
	}
}
