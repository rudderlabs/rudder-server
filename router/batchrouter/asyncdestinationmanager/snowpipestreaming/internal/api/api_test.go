package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/testhelper"
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

type nopReadCloser struct {
	io.Reader
}

func (nopReadCloser) Close() error {
	return nil
}

func TestMustReadAll(t *testing.T) {
	t.Run("ReadAll", func(t *testing.T) {
		r := strings.NewReader("hello")
		data := mustReadAll(r)
		require.Equal(t, []byte("hello"), data)
	})
	t.Run("ReadAll error", func(t *testing.T) {
		r := iotest.ErrReader(errors.New("error"))
		data := mustReadAll(r)
		require.Empty(t, data)
	})
}

func TestAPI(t *testing.T) {
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
		c := testcompose.New(t, compose.FilePaths([]string{"../../testdata/docker-compose.rudder-snowpipe-clients.yml"}))
		c.Start(context.Background())

		credentials, err := testhelper.GetSnowPipeTestCredentials(testhelper.TestKeyPairUnencrypted)
		require.NoError(t, err)

		ctx := context.Background()

		namespace := testhelper.RandSchema(whutils.SNOWFLAKE)
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

		t.Log("Creating namespace and table")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, table, tableSchema))

		snowPipeClientsURL := fmt.Sprintf("http://localhost:%d", c.Port("rudder-snowpipe-clients", 9078))
		api := New(snowPipeClientsURL, http.DefaultClient)

		t.Log("Creating channel")
		createChannelRes, err := api.CreateChannel(ctx, &model.CreateChannelRequest{
			RudderIdentifier: "1",
			Partition:        "1",
			AccountConfig: model.AccountConfig{
				Account:              credentials.Account,
				User:                 credentials.User,
				Role:                 credentials.Role,
				PrivateKey:           strings.ReplaceAll(credentials.PrivateKey, "\n", "\\\\\n"),
				PrivateKeyPassphrase: credentials.PrivateKeyPassphrase,
			},
			TableConfig: model.TableConfig{
				Database: credentials.Database,
				Schema:   namespace,
				Table:    table,
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, createChannelRes.ChannelID)
		require.True(t, createChannelRes.Valid)
		require.False(t, createChannelRes.Deleted)
		require.EqualValues(t, map[string]map[string]interface{}{
			"ACTIVE": {
				"byteLength":  nil,
				"length":      nil,
				"logicalType": "BOOLEAN",
				"nullable":    true,
				"precision":   nil,
				"scale":       nil,
				"type":        "BOOLEAN",
			},
			"AGE": {
				"byteLength":  nil,
				"length":      nil,
				"logicalType": "FIXED",
				"nullable":    true,
				"precision":   float64(38),
				"scale":       float64(0),
				"type":        "NUMBER(38,0)",
			},
			"DOB": {
				"byteLength":  nil,
				"length":      nil,
				"logicalType": "TIMESTAMP_TZ",
				"nullable":    true,
				"precision":   float64(0),
				"scale":       float64(9),
				"type":        "TIMESTAMP_TZ(9)",
			},
			"EMAIL": {
				"byteLength":  1.6777216e+07,
				"length":      1.6777216e+07,
				"logicalType": "TEXT",
				"nullable":    true,
				"precision":   nil,
				"scale":       nil,
				"type":        "VARCHAR(16777216)",
			},
			"ID": {
				"byteLength":  1.6777216e+07,
				"length":      1.6777216e+07,
				"logicalType": "TEXT",
				"nullable":    true,
				"precision":   nil,
				"scale":       nil,
				"type":        "VARCHAR(16777216)",
			},
			"NAME": {
				"byteLength":  1.6777216e+07,
				"length":      1.6777216e+07,
				"logicalType": "TEXT",
				"nullable":    true,
				"precision":   nil,
				"scale":       nil,
				"type":        "VARCHAR(16777216)",
			},
		},
			createChannelRes.TableSchema,
		)

		t.Log("Getting channel")
		getChannelRes, err := api.GetChannel(ctx, createChannelRes.ChannelID)
		require.NoError(t, err)
		require.Equal(t, createChannelRes, getChannelRes)

		t.Log("Inserting records")
		insertRes, err := api.Insert(ctx, createChannelRes.ChannelID, &model.InsertRequest{
			Rows: []model.Row{
				{"ID": "ID1", "NAME": "Alice Johnson", "EMAIL": "alice.johnson@example.com", "AGE": 28, "ACTIVE": true, "DOB": "1995-06-15T12:30:00Z"},
				{"ID": "ID2", "NAME": "Bob Smith", "EMAIL": "bob.smith@example.com", "AGE": 35, "ACTIVE": true, "DOB": "1988-01-20T09:30:00Z"},
				{"ID": "ID3", "NAME": "Charlie Brown", "EMAIL": "charlie.brown@example.com", "AGE": 22, "ACTIVE": false, "DOB": "2001-11-05T14:45:00Z"},
				{"ID": "ID4", "NAME": "Diana Prince", "EMAIL": "diana.prince@example.com", "AGE": 30, "ACTIVE": true, "DOB": "1993-08-18T08:15:00Z"},
				{"ID": "ID5", "NAME": "Eve Adams", "AGE": 45, "ACTIVE": true, "DOB": "1978-03-22T16:50:00Z"}, // -- No email
				{"ID": "ID6", "NAME": "Frank Castle", "EMAIL": "frank.castle@example.com", "AGE": 38, "ACTIVE": false, "DOB": "1985-09-14T10:10:00Z"},
				{"ID": "ID7", "NAME": "Grace Hopper", "EMAIL": "grace.hopper@example.com", "AGE": 85, "ACTIVE": true, "DOB": "1936-12-09T11:30:00Z"},
			},
			Offset: "8",
		})
		require.NoError(t, err)
		require.Equal(t, &model.InsertResponse{Success: true, Errors: nil}, insertRes)

		t.Log("Checking status")
		require.Eventually(t, func() bool {
			statusRes, err := api.Status(ctx, createChannelRes.ChannelID)
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
		c := testcompose.New(t, compose.FilePaths([]string{"../../testdata/docker-compose.rudder-snowpipe-clients.yml"}))
		c.Start(context.Background())

		credentials, err := testhelper.GetSnowPipeTestCredentials(testhelper.TestKeyPairUnencrypted)
		require.NoError(t, err)

		ctx := context.Background()

		namespace := testhelper.RandSchema(whutils.SNOWFLAKE)
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

		t.Log("Creating namespace and table")
		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { testhelper.DropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, table, tableSchema))

		snowPipeClientsURL := fmt.Sprintf("http://localhost:%d", c.Port("rudder-snowpipe-clients", 9078))
		api := New(snowPipeClientsURL, http.DefaultClient)

		t.Log("Creating channel")
		createChannelReq := &model.CreateChannelRequest{
			RudderIdentifier: "1",
			Partition:        "1",
			AccountConfig: model.AccountConfig{
				Account:              credentials.Account,
				User:                 credentials.User,
				Role:                 credentials.Role,
				PrivateKey:           strings.ReplaceAll(credentials.PrivateKey, "\n", "\\\\\n"),
				PrivateKeyPassphrase: credentials.PrivateKeyPassphrase,
			},
			TableConfig: model.TableConfig{
				Database: credentials.Database,
				Schema:   namespace,
				Table:    table,
			},
		}
		createChannelRes1, err := api.CreateChannel(ctx, createChannelReq)
		require.NoError(t, err)
		require.True(t, createChannelRes1.Valid)

		t.Log("Creating channel again, should return the same channel id")
		createChannelRes2, err := api.CreateChannel(ctx, createChannelReq)
		require.NoError(t, err)
		require.True(t, createChannelRes2.Valid)
		require.Equal(t, createChannelRes1, createChannelRes2)

		t.Log("Deleting channel")
		err = api.DeleteChannel(ctx, createChannelRes1.ChannelID, true)
		require.NoError(t, err)

		t.Log("Creating channel again, should return a new channel id")
		createChannelRes3, err := api.CreateChannel(ctx, createChannelReq)
		require.NoError(t, err)
		require.NotEqual(t, createChannelRes1.ChannelID, createChannelRes3.ChannelID)
	})
}
