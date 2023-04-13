package deltalake_native_test

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	dbsql "github.com/databricks/databricks-sql-go"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/utils/misc"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
)

func TestIntegrationDeltalake(t *testing.T) {
	t.Parallel()

	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if _, exists := os.LookupEnv(testhelper.DeltalakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.DeltalakeIntegrationTestCredentials)
	}

	deltalake.Init()

	credentials, err := testhelper.DatabricksCredentials()
	require.NoError(t, err)

	port, err := strconv.Atoi(credentials.Port)
	require.NoError(t, err)

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(credentials.Host),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(credentials.Path),
		dbsql.WithAccessToken(credentials.Token),
		dbsql.WithSessionParams(map[string]string{
			"ansi_mode": "false",
		}),
	)
	require.NoError(t, err)

	db := sql.OpenDB(connector)

	var (
		jobsDB       = testhelper.SetUpJobsDB(t)
		provider     = warehouseutils.DELTALAKE
		schema       = testhelper.Schema(provider, testhelper.DeltalakeIntegrationTestSchema)
		nativeSchema = fmt.Sprintf("%s_%s", schema, "native")
	)

	testCases := []struct {
		name          string
		writeKey      string
		sourceID      string
		destinationID string
		schema        string
		useNative     bool
	}{
		{
			name:          "Native",
			writeKey:      "dasFgoilA0U1WxNeW1gdgUVDfas",
			sourceID:      "36H5EpYzojqQSepRSaGBrrPx3e4",
			destinationID: "36IDjdnoEus6DDNrth3SWO1FOpu",
			schema:        nativeSchema,
			useNative:     true,
		},
		{
			name:          "Legacy",
			writeKey:      "sToFgoilA0U1WxNeW1gdgUVDsEW",
			sourceID:      "25H5EpYzojqQSepRSaGBrrPx3e4",
			destinationID: "25IDjdnoEus6DDNrth3SWO1FOpu",
			schema:        schema,
			useNative:     false,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			t.Cleanup(func() {
				_, err := db.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %[1]s;`, tc.schema))
				require.NoError(t, err)
			})

			subTestCases := []struct {
				name               string
				schema             string
				writeKey           string
				sourceID           string
				destinationID      string
				messageID          string
				warehouseEventsMap testhelper.EventsCountMap
				prerequisite       func(t testing.TB)
			}{
				{
					name:               "Merge Mode",
					writeKey:           tc.writeKey,
					schema:             tc.schema,
					sourceID:           tc.sourceID,
					destinationID:      tc.destinationID,
					warehouseEventsMap: mergeEventsMap(),
					prerequisite: func(t testing.TB) {
						t.Helper()
						testhelper.SetConfig(t, []warehouseutils.KeyValue{
							{
								Key:   "Warehouse.deltalake.loadTableStrategy",
								Value: "MERGE",
							},
							{
								Key:   "Warehouse.deltalake.useParquetLoadFiles",
								Value: "false",
							},
							{
								Key:   "Warehouse.deltalake.useNative",
								Value: strconv.FormatBool(tc.useNative),
							},
						})
					},
				},
				{
					name:               "Append Mode",
					writeKey:           tc.writeKey,
					schema:             tc.schema,
					sourceID:           tc.sourceID,
					destinationID:      tc.destinationID,
					warehouseEventsMap: appendEventsMap(),
					prerequisite: func(t testing.TB) {
						t.Helper()
						testhelper.SetConfig(t, []warehouseutils.KeyValue{
							{
								Key:   "Warehouse.deltalake.loadTableStrategy",
								Value: "APPEND",
							},
							{
								Key:   "Warehouse.deltalake.useParquetLoadFiles",
								Value: "false",
							},
							{
								Key:   "Warehouse.deltalake.useNative",
								Value: strconv.FormatBool(tc.useNative),
							},
						})
					},
				},
				{
					name:               "Parquet load files",
					writeKey:           tc.writeKey,
					schema:             tc.schema,
					sourceID:           tc.sourceID,
					destinationID:      tc.destinationID,
					warehouseEventsMap: mergeEventsMap(),
					prerequisite: func(t testing.TB) {
						t.Helper()
						testhelper.SetConfig(t, []warehouseutils.KeyValue{
							{
								Key:   "Warehouse.deltalake.loadTableStrategy",
								Value: "MERGE",
							},
							{
								Key:   "Warehouse.deltalake.useParquetLoadFiles",
								Value: "true",
							},
							{
								Key:   "Warehouse.deltalake.useNative",
								Value: strconv.FormatBool(tc.useNative),
							},
						})
					},
				},
			}

			for _, stc := range subTestCases {
				stc := stc

				t.Run(tc.name+" "+stc.name, func(t *testing.T) {
					ts := testhelper.WareHouseTest{
						Schema:        stc.schema,
						WriteKey:      stc.writeKey,
						SourceID:      stc.sourceID,
						DestinationID: stc.destinationID,
						Prerequisite:  stc.prerequisite,
						JobsDB:        jobsDB,
						Provider:      provider,
						UserID:        testhelper.GetUserId(provider),
						MessageID:     misc.FastUUID().String(),
						Tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
						WarehouseEventsMap: testhelper.EventsCountMap{
							"identifies":    1,
							"users":         1,
							"tracks":        1,
							"product_track": 1,
							"pages":         1,
							"screens":       1,
							"aliases":       1,
							"groups":        1,
						},
						Client: &warehouseclient.Client{
							SQL:  db,
							Type: warehouseclient.SQLClient,
						},
						StatsToVerify: []string{
							"warehouse_deltalake_grpcExecTime",
							"warehouse_deltalake_healthTimeouts",
						},
					}
					ts.VerifyEvents(t)

					ts.WarehouseEventsMap = stc.warehouseEventsMap
					ts.VerifyModifiedEvents(t)
				})
			}
		})
	}
}

func TestConfigurationValidationDeltalake(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("Skipping tests. Add 'SLOW=1' env var to run test.")
	}
	if _, exists := os.LookupEnv(testhelper.DeltalakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.DeltalakeIntegrationTestCredentials)
	}

	t.Parallel()

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	encoding.Init()
	deltalake.Init()

	configurations := testhelper.PopulateTemplateConfigurations()

	testCases := []struct {
		name        string
		useLegacy   bool
		destination backendconfig.DestinationT
	}{
		{
			name: "Native",
			destination: backendconfig.DestinationT{
				ID: "36H5EpYzojqQSepRSaGBrrPx3e4",
				Config: map[string]interface{}{
					"host":            configurations["deltalakeNativeHost"],
					"port":            configurations["deltalakeNativePort"],
					"path":            configurations["deltalakeNativePath"],
					"token":           configurations["deltalakeNativeToken"],
					"namespace":       configurations["deltalakeNativeNamespace"],
					"bucketProvider":  "AZURE_BLOB",
					"containerName":   configurations["deltalakeNativeContainerName"],
					"prefix":          "",
					"useSTSTokens":    false,
					"enableSSE":       false,
					"accountName":     configurations["deltalakeNativeAccountName"],
					"accountKey":      configurations["deltalakeNativeAccountKey"],
					"syncFrequency":   "30",
					"eventDelivery":   false,
					"eventDeliveryTS": 1648195480174,
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:          "23HLpnDJnIg7DsBvDWGU6DQzFEo",
					Name:        "DELTALAKE",
					DisplayName: "Databricks (Delta Lake)",
				},
				Name:       "deltalake-native-demo",
				Enabled:    true,
				RevisionID: "39eClxJQQlaWzMWyqnQctFDP5T2",
			},
			useLegacy: false,
		},
		{
			name: "Legacy",
			destination: backendconfig.DestinationT{
				ID: "25IDjdnoEus6DDNrth3SWO1FOpu",
				Config: map[string]interface{}{
					"host":            configurations["deltalakeHost"],
					"port":            configurations["deltalakePort"],
					"path":            configurations["deltalakePath"],
					"token":           configurations["deltalakeToken"],
					"namespace":       configurations["deltalakeNamespace"],
					"bucketProvider":  "AZURE_BLOB",
					"containerName":   configurations["deltalakeContainerName"],
					"prefix":          "",
					"useSTSTokens":    false,
					"enableSSE":       false,
					"accountName":     configurations["deltalakeAccountName"],
					"accountKey":      configurations["deltalakeAccountKey"],
					"syncFrequency":   "30",
					"eventDelivery":   false,
					"eventDeliveryTS": 1648195480174,
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:          "23HLpnDJnIg7DsBvDWGU6DQzFEo",
					Name:        "DELTALAKE",
					DisplayName: "Databricks (Delta Lake)",
				},
				Name:       "deltalake-demo",
				Enabled:    true,
				RevisionID: "29eClxJQQlaWzMWyqnQctFDP5T2",
			},
			useLegacy: true,
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			subTestCases := []struct {
				name                string
				useParquetLoadFiles bool
			}{
				{
					name:                "Parquet load files",
					useParquetLoadFiles: true,
				},
				{
					name:                "CSV load files",
					useParquetLoadFiles: false,
				},
			}
			for _, stc := range subTestCases {
				stc := stc

				t.Run(tc.name+" "+stc.name, func(t *testing.T) {
					testhelper.SetConfig(t, []warehouseutils.KeyValue{
						{
							Key:   "Warehouse.deltalake.useParquetLoadFiles",
							Value: strconv.FormatBool(stc.useParquetLoadFiles),
						},
						{
							Key:   "Warehouse.deltalake.useNative",
							Value: strconv.FormatBool(tc.useLegacy),
						},
					})

					testhelper.VerifyConfigurationTest(t, tc.destination)
				})
			}
		})
	}
}

func mergeEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    1,
		"users":         1,
		"tracks":        1,
		"product_track": 1,
		"pages":         1,
		"screens":       1,
		"aliases":       1,
		"groups":        1,
	}
}

func appendEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    2,
		"users":         2,
		"tracks":        2,
		"product_track": 2,
		"pages":         2,
		"screens":       2,
		"aliases":       2,
		"groups":        2,
	}
}
