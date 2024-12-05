package api_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/api"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type integrationTestConfig struct {
	credentials *testhelper.TestCredentials
	db          *sql.DB
	namespace   string
	tableName   string
	clientURL   string
}

func TestAPIIntegration(t *testing.T) {
	for _, key := range []string{
		testhelper.TestKeyPairUnencrypted,
	} {
		if _, exists := os.LookupEnv(key); !exists {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatalf("%s environment variable not set", key)
			}
			t.Skipf("Skipping %s as %s is not set", t.Name(), key)
		}
	}

	t.Run("Create channel + Get channel + Insert data + Status", func(t *testing.T) {
		testCases := []struct {
			name              string
			enableCompression bool
			payloadSize       int
		}{
			{
				name:              "Compression enabled",
				enableCompression: true,
				payloadSize:       378,
			},
			{
				name:              "Compression disabled",
				enableCompression: false,
				payloadSize:       839,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				testConfig := setupIntegrationTestConfig(t, ctx)

				c := config.New()
				c.Set("SnowpipeStreaming.enableCompression", tc.enableCompression)

				statsStore, err := memstats.New()
				require.NoError(t, err)

				snowpipeAPI := api.New(c, statsStore, testConfig.clientURL, http.DefaultClient)

				t.Log("Creating channel")
				createChannelRes, err := snowpipeAPI.CreateChannel(ctx, &model.CreateChannelRequest{
					RudderIdentifier: "1",
					Partition:        "1",
					AccountConfig: model.AccountConfig{
						Account:              testConfig.credentials.Account,
						User:                 testConfig.credentials.User,
						Role:                 testConfig.credentials.Role,
						PrivateKey:           strings.ReplaceAll(testConfig.credentials.PrivateKey, "\n", "\\\\\n"),
						PrivateKeyPassphrase: testConfig.credentials.PrivateKeyPassphrase,
					},
					TableConfig: model.TableConfig{
						Database: testConfig.credentials.Database,
						Schema:   testConfig.namespace,
						Table:    testConfig.tableName,
					},
				})
				require.NoError(t, err)
				require.NotEmpty(t, createChannelRes.ChannelID)
				require.True(t, createChannelRes.Valid)
				require.False(t, createChannelRes.Deleted)
				require.EqualValues(t, whutils.ModelTableSchema{"ACTIVE": "boolean", "AGE": "int", "DOB": "datetime", "EMAIL": "string", "ID": "string", "NAME": "string"},
					createChannelRes.SnowpipeSchema,
				)

				t.Log("Getting channel")
				getChannelRes, err := snowpipeAPI.GetChannel(ctx, createChannelRes.ChannelID)
				require.NoError(t, err)
				require.Equal(t, createChannelRes, getChannelRes)

				rows := []model.Row{
					{"ID": "ID1", "NAME": "Alice Johnson", "EMAIL": "alice.johnson@example.com", "AGE": 28, "ACTIVE": true, "DOB": "1995-06-15T12:30:00Z"},
					{"ID": "ID2", "NAME": "Bob Smith", "EMAIL": "bob.smith@example.com", "AGE": 35, "ACTIVE": true, "DOB": "1988-01-20T09:30:00Z"},
					{"ID": "ID3", "NAME": "Charlie Brown", "EMAIL": "charlie.brown@example.com", "AGE": 22, "ACTIVE": false, "DOB": "2001-11-05T14:45:00Z"},
					{"ID": "ID4", "NAME": "Diana Prince", "EMAIL": "diana.prince@example.com", "AGE": 30, "ACTIVE": true, "DOB": "1993-08-18T08:15:00Z"},
					{"ID": "ID5", "NAME": "Eve Adams", "AGE": 45, "ACTIVE": true, "DOB": "1978-03-22T16:50:00Z"}, // -- No email
					{"ID": "ID6", "NAME": "Frank Castle", "EMAIL": "frank.castle@example.com", "AGE": 38, "ACTIVE": false, "DOB": "1985-09-14T10:10:00Z"},
					{"ID": "ID7", "NAME": "Grace Hopper", "EMAIL": "grace.hopper@example.com", "AGE": 85, "ACTIVE": true, "DOB": "1936-12-09T11:30:00Z"},
				}

				t.Log("Inserting records")
				insertRes, err := snowpipeAPI.Insert(ctx, createChannelRes.ChannelID, &model.InsertRequest{
					Rows:   rows,
					Offset: "8",
				})
				require.NoError(t, err)
				require.Equal(t, &model.InsertResponse{Success: true, Errors: nil}, insertRes)
				require.EqualValues(t, tc.payloadSize, statsStore.Get("snowpipe_streaming_request_body_size", stats.Tags{
					"api": "insert",
				}).LastValue())

				t.Log("Checking status")
				require.Eventually(t, func() bool {
					statusRes, err := snowpipeAPI.GetStatus(ctx, createChannelRes.ChannelID)
					if err != nil {
						t.Log("Error getting status:", err)
						return false
					}
					return statusRes.Offset == "8"
				},
					30*time.Second,
					300*time.Millisecond,
				)

				t.Log("Checking records in warehouse")
				records := whth.RetrieveRecordsFromWarehouse(t, testConfig.db, fmt.Sprintf(`SELECT ID, NAME, EMAIL, AGE, ACTIVE, DOB FROM %q.%q ORDER BY ID;`, testConfig.namespace, testConfig.tableName))
				require.ElementsMatch(t, convertRowsToRecord(rows, []string{"ID", "NAME", "EMAIL", "AGE", "ACTIVE", "DOB"}), records)
			})
		}
	})

	t.Run("Create + Delete channel", func(t *testing.T) {
		ctx := context.Background()
		testConfig := setupIntegrationTestConfig(t, ctx)
		snowpipeAPI := api.New(config.New(), stats.NOP, testConfig.clientURL, http.DefaultClient)

		t.Log("Creating channel")
		createChannelReq := &model.CreateChannelRequest{
			RudderIdentifier: "1",
			Partition:        "1",
			AccountConfig: model.AccountConfig{
				Account:              testConfig.credentials.Account,
				User:                 testConfig.credentials.User,
				Role:                 testConfig.credentials.Role,
				PrivateKey:           strings.ReplaceAll(testConfig.credentials.PrivateKey, "\n", "\\\\\n"),
				PrivateKeyPassphrase: testConfig.credentials.PrivateKeyPassphrase,
			},
			TableConfig: model.TableConfig{
				Database: testConfig.credentials.Database,
				Schema:   testConfig.namespace,
				Table:    testConfig.tableName,
			},
		}
		createChannelRes1, err := snowpipeAPI.CreateChannel(ctx, createChannelReq)
		require.NoError(t, err)
		require.True(t, createChannelRes1.Valid)

		t.Log("Creating channel again, should return the same channel id")
		createChannelRes2, err := snowpipeAPI.CreateChannel(ctx, createChannelReq)
		require.NoError(t, err)
		require.True(t, createChannelRes2.Valid)
		require.Equal(t, createChannelRes1, createChannelRes2)

		t.Log("Deleting channel")
		err = snowpipeAPI.DeleteChannel(ctx, createChannelRes1.ChannelID, true)
		require.NoError(t, err)

		t.Log("Creating channel again, should return a new channel id")
		createChannelRes3, err := snowpipeAPI.CreateChannel(ctx, createChannelReq)
		require.NoError(t, err)
		require.NotEqual(t, createChannelRes1.ChannelID, createChannelRes3.ChannelID)
	})
}

func setupIntegrationTestConfig(t *testing.T, ctx context.Context) *integrationTestConfig {
	t.Helper()

	c := testcompose.New(t, compose.FilePaths([]string{"../../testdata/docker-compose.rudder-snowpipe-clients.yml"}))
	c.Start(context.Background())

	credentials, err := testhelper.GetSnowpipeTestCredentials(testhelper.TestKeyPairUnencrypted)
	require.NoError(t, err)

	namespace := testhelper.RandSchema()
	table := "TEST_TABLE"
	tableSchema := whutils.ModelTableSchema{
		"ID": "string", "NAME": "string", "EMAIL": "string", "AGE": "int", "ACTIVE": "boolean", "DOB": "datetime",
	}

	warehouse := whutils.ModelWarehouse{
		Namespace: namespace,
		Destination: backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			WithConfigOption("account", credentials.Account).
			WithConfigOption("warehouse", credentials.Warehouse).
			WithConfigOption("database", credentials.Database).
			WithConfigOption("role", credentials.Role).
			WithConfigOption("user", credentials.User).
			WithConfigOption("useKeyPairAuth", true).
			WithConfigOption("privateKey", credentials.PrivateKey).
			WithConfigOption("privateKeyPassphrase", credentials.PrivateKeyPassphrase).
			Build(),
	}

	t.Log("Creating namespace and table")
	sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
	require.NoError(t, sm.Setup(ctx, warehouse, whutils.NewNoOpUploader()))
	t.Cleanup(func() { sm.Cleanup(ctx) })
	require.NoError(t, sm.CreateSchema(ctx))
	t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
	require.NoError(t, sm.CreateTable(ctx, table, tableSchema))

	snowpipeClientsURL := fmt.Sprintf("http://localhost:%d", c.Port("rudder-snowpipe-clients", 9078))

	return &integrationTestConfig{
		credentials: credentials,
		db:          sm.DB.DB,
		namespace:   namespace,
		tableName:   table,
		clientURL:   snowpipeClientsURL,
	}
}

func convertRowsToRecord(rows []model.Row, columns []string) [][]string {
	return lo.Map(rows, func(row model.Row, _ int) []string {
		return lo.Map(columns, func(col string, index int) string {
			if v, ok := row[col]; ok {
				return fmt.Sprintf("%v", v)
			}
			return ""
		})
	})
}
