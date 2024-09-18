package snowpipestreaming

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	thwh "github.com/rudderlabs/rudder-server/testhelper/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type mockRequestDoer struct {
	err      error
	response *http.Response
}

func (c *mockRequestDoer) Do(*http.Request) (*http.Response, error) {
	return c.response, c.err
}

func (nopReadCloser) Close() error {
	return nil
}

type nopReadCloser struct {
	io.Reader
}

type testCredentials struct {
	Account              string `json:"account"`
	User                 string `json:"user"`
	Role                 string `json:"role"`
	Database             string `json:"database"`
	Warehouse            string `json:"warehouse"`
	PrivateKey           string `json:"privateKey"`
	PrivateKeyPassphrase string `json:"privateKeyPassphrase"`
}

const (
	testKeyPairUnencrypted = "SNOWPIPE_STREAMING_KEYPAIR_UNENCRYPTED_INTEGRATION_TEST_CREDENTIALS"
)

func getSnowpipeTestCredentials(key string) (*testCredentials, error) {
	cred, exists := os.LookupEnv(key)
	if !exists {
		return nil, errors.New("snowpipe test credentials not found")
	}

	var credentials testCredentials
	err := json.Unmarshal([]byte(cred), &credentials)
	if err != nil {
		return nil, fmt.Errorf("unable to marshall %s to snowpipe test credentials: %v", key, err)
	}
	return &credentials, nil
}

func randSchema(provider string) string {
	hex := strings.ToLower(rand.String(12))
	namespace := fmt.Sprintf("test_%s_%d", hex, time.Now().Unix())
	return whutils.ToProviderCase(provider, whutils.ToSafeNamespace(provider,
		namespace,
	))
}

func TestSnowpipeStreaming(t *testing.T) {
	t.Run("Integration", func(t *testing.T) {
		for _, key := range []string{
			testKeyPairUnencrypted,
		} {
			if _, exists := os.LookupEnv(key); !exists {
				if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
					t.Fatalf("%s environment variable not set", key)
				}
				t.Skipf("Skipping %s as %s is not set", t.Name(), key)
			}
		}

		t.Run("Create channel + Insert + Status", func(t *testing.T) {
			c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.rudder-snowpipe-clients.yml"}))
			c.Start(context.Background())

			credentials, err := getSnowpipeTestCredentials(testKeyPairUnencrypted)
			require.NoError(t, err)

			ctx := context.Background()

			namespace := randSchema(whutils.SNOWFLAKE)
			table := "TEST_TABLE"
			tableSchema := whutils.ModelTableSchema{
				"ID": "string", "NAME": "string", "EMAIL": "string", "AGE": "int", "ACTIVE": "boolean", "DOB": "datetime",
			}

			destination := backendconfigtest.
				NewDestinationBuilder("SNOWPIPE_STREAMING").
				WithConfigOption("account", credentials.Account).
				WithConfigOption("warehouse", credentials.Warehouse).
				WithConfigOption("database", credentials.Database).
				WithConfigOption("role", credentials.Role).
				WithConfigOption("user", credentials.User).
				WithConfigOption("useKeyPairAuth", true).
				WithConfigOption("privateKey", credentials.PrivateKey).
				WithConfigOption("privateKeyPassphrase", credentials.PrivateKeyPassphrase).
				Build()
			warehouse := whutils.ModelWarehouse{
				Namespace:   namespace,
				Destination: destination,
			}

			// Creating namespace and table
			sm := snowflake.New(config.New(), logger.NewLogger().Child("test"), stats.NOP)
			require.NoError(t, err)
			require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
			t.Cleanup(func() { sm.Cleanup(ctx) })
			require.NoError(t, sm.CreateSchema(ctx))
			t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
			require.NoError(t, sm.CreateTable(ctx, table, tableSchema))

			snowpipeClientsURL := fmt.Sprintf("http://localhost:%d", c.Port("rudder-snowpipe-clients", 9078))

			conf := config.New()
			conf.Set("Snowpipe.Client.URL", snowpipeClientsURL)

			snowpipeStreamManager := New(conf, logger.NewLogger().Child("test"), stats.NOP, &destination)

			// creating channel
			createChannelRes, err := snowpipeStreamManager.createChannel(ctx, &createChannelRequest{
				RudderIdentifier: "1",
				Partition:        "1",
				AccountConfig: accountConfig{
					Account:              credentials.Account,
					User:                 credentials.User,
					Role:                 credentials.Role,
					PrivateKey:           strings.ReplaceAll(credentials.PrivateKey, "\n", "\\\\\n"),
					PrivateKeyPassphrase: credentials.PrivateKeyPassphrase,
				},
				TableConfig: tableConfig{
					Database: credentials.Database,
					Schema:   namespace,
					Table:    table,
				},
			})
			require.NoError(t, err)
			require.True(t, createChannelRes.Valid)

			// inserting rows
			insertRes, err := snowpipeStreamManager.insert(ctx, createChannelRes.ChannelID, &insertRequest{
				Rows: []Row{
					{"ID": "ID1", "NAME": "Alice Johnson", "EMAIL": "alice.johnson@example.com", "AGE": 28, "ACTIVE": true, "DOB": "1995-06-15T12:30:00Z"},
					{"ID": "ID2", "NAME": "Bob Smith", "EMAIL": "bob.smith@example.com", "AGE": 35, "ACTIVE": true, "DOB": "1988-01-20T09:30:00Z"},
					{"ID": "ID3", "NAME": "Charlie Brown", "EMAIL": "charlie.brown@example.com", "AGE": 22, "ACTIVE": false, "DOB": "2001-11-05T14:45:00Z"},
					{"ID": "ID4", "NAME": "Diana Prince", "EMAIL": "diana.prince@example.com", "AGE": 30, "ACTIVE": true, "DOB": "1993-08-18T08:15:00Z"},
					{"ID": "ID5", "NAME": "Eve Adams", "AGE": 45, "ACTIVE": true, "DOB": "1978-03-22T16:50:00Z"}, // -- No email
					{"ID": "ID6", "NAME": "Frank Castle", "EMAIL": "frank.castle@example.com", "AGE": 38, "ACTIVE": false, "DOB": "1985-09-14T10:10:00Z"},
					{"ID": "ID7", "NAME": "Grace Hopper", "EMAIL": "grace.hopper@example.com", "AGE": 85, "ACTIVE": true, "DOB": "1936-12-09T11:30:00Z"},
				},
				Offset: "100",
			})
			require.NoError(t, err)
			require.True(t, insertRes.Success)
			require.Empty(t, insertRes.Errors)

			// getting status
			require.Eventually(t, func() bool {
				statusRes, err := snowpipeStreamManager.status(ctx, createChannelRes.ChannelID)
				if err != nil {
					t.Log("Error getting status:", err)
					return false
				}
				return statusRes.Offset == "100"
			},
				30*time.Second,
				300*time.Millisecond,
			)

			// checking records in warehouse
			records := thwh.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT ID, NAME, EMAIL, AGE, ACTIVE, DOB FROM %q.%q ORDER BY ID;`, namespace, table))
			require.ElementsMatch(t, [][]string{
				{"ID1", "Alice Johnson", "alice.johnson@example.com", "28", "true", "1995-06-15T12:30:00Z"},
				{"ID2", "Bob Smith", "bob.smith@example.com", "35", "true", "1988-01-20T09:30:00Z"},
				{"ID3", "Charlie Brown", "charlie.brown@example.com", "22", "false", "2001-11-05T14:45:00Z"},
				{"ID4", "Diana Prince", "diana.prince@example.com", "30", "true", "1993-08-18T08:15:00Z"},
				{"ID5", "Eve Adams", "", "45", "true", "1978-03-22T16:50:00Z"},
				{"ID6", "Frank Castle", "frank.castle@example.com", "38", "false", "1985-09-14T10:10:00Z"},
				{"ID7", "Grace Hopper", "grace.hopper@example.com", "85", "true", "1936-12-09T11:30:00Z"},
			}, records)
		})

		t.Run("Create + Delete channel", func(t *testing.T) {
			c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.rudder-snowpipe-clients.yml"}))
			c.Start(context.Background())

			credentials, err := getSnowpipeTestCredentials(testKeyPairUnencrypted)
			require.NoError(t, err)

			ctx := context.Background()

			namespace := randSchema(whutils.SNOWFLAKE)
			table := "TEST_TABLE"
			tableSchema := whutils.ModelTableSchema{
				"ID": "string", "NAME": "string", "EMAIL": "string", "AGE": "int", "ACTIVE": "boolean", "DOB": "datetime",
			}

			destination := backendconfigtest.
				NewDestinationBuilder("SNOWPIPE_STREAMING").
				WithConfigOption("account", credentials.Account).
				WithConfigOption("warehouse", credentials.Warehouse).
				WithConfigOption("database", credentials.Database).
				WithConfigOption("role", credentials.Role).
				WithConfigOption("user", credentials.User).
				WithConfigOption("useKeyPairAuth", true).
				WithConfigOption("privateKey", credentials.PrivateKey).
				WithConfigOption("privateKeyPassphrase", credentials.PrivateKeyPassphrase).
				Build()
			warehouse := whutils.ModelWarehouse{
				Namespace:   namespace,
				Destination: destination,
			}

			// Creating namespace and table
			sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
			require.NoError(t, err)
			require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
			t.Cleanup(func() { sm.Cleanup(ctx) })
			require.NoError(t, sm.CreateSchema(ctx))
			t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
			require.NoError(t, sm.CreateTable(ctx, table, tableSchema))

			snowpipeClientsURL := fmt.Sprintf("http://localhost:%d", c.Port("rudder-snowpipe-clients", 9078))

			conf := config.New()
			conf.Set("Snowpipe.Client.URL", snowpipeClientsURL)

			snowpipeStreamManager := New(conf, logger.NOP, stats.NOP, &destination)

			// creating channel
			createChannelReq := &createChannelRequest{
				RudderIdentifier: "1",
				Partition:        "1",
				AccountConfig: accountConfig{
					Account:              credentials.Account,
					User:                 credentials.User,
					Role:                 credentials.Role,
					PrivateKey:           strings.ReplaceAll(credentials.PrivateKey, "\n", "\\\\\n"),
					PrivateKeyPassphrase: credentials.PrivateKeyPassphrase,
				},
				TableConfig: tableConfig{
					Database: credentials.Database,
					Schema:   namespace,
					Table:    table,
				},
			}

			// creating channel
			createChannelRes1, err := snowpipeStreamManager.createChannel(ctx, createChannelReq)
			require.NoError(t, err)
			require.True(t, createChannelRes1.Valid)

			// creating channel again with same request should return same channel id
			createChannelRes2, err := snowpipeStreamManager.createChannel(ctx, createChannelReq)
			require.NoError(t, err)
			require.True(t, createChannelRes2.Valid)
			require.Equal(t, createChannelRes1.ChannelID, createChannelRes2.ChannelID)

			// deleting channel
			err = snowpipeStreamManager.deleteChannel(ctx, createChannelReq)
			require.NoError(t, err)

			// creating channel again, since the previous channel is deleted, it should return a new channel id
			createChannelRes3, err := snowpipeStreamManager.createChannel(ctx, createChannelReq)
			require.NoError(t, err)
			require.True(t, createChannelRes3.Valid)
			require.NotEqual(t, createChannelRes1.ChannelID, createChannelRes3.ChannelID)
		})
	})
}

func dropSchema(t *testing.T, db *sql.DB, namespace string) {
	t.Helper()
	t.Log("dropping schema", namespace)

	require.Eventually(t,
		func() bool {
			_, err := db.ExecContext(context.Background(), fmt.Sprintf(`DROP SCHEMA %q CASCADE;`, namespace))
			if err != nil {
				t.Logf("error deleting schema %q: %v", namespace, err)
				return false
			}
			return true
		},
		time.Minute,
		time.Second,
	)
}
