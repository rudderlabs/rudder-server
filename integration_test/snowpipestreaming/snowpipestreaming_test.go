package snowpipestreaming

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	promClient "github.com/prometheus/client_model/go"
	"github.com/rudderlabs/rudder-go-kit/stats/testhelper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	testKeyPairUnencrypted = "SNOWPIPE_STREAMING_KEYPAIR_UNENCRYPTED_INTEGRATION_TEST_CREDENTIALS"
)

type testCredentials struct {
	Account              string `json:"account"`
	User                 string `json:"user"`
	Role                 string `json:"role"`
	Database             string `json:"database"`
	Warehouse            string `json:"warehouse"`
	PrivateKey           string `json:"privateKey"`
	PrivateKeyPassphrase string `json:"privateKeyPassphrase"`
}

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

func randSchema(provider string) string { // nolint:unparam
	hex := strings.ToLower(rand.String(12))
	namespace := fmt.Sprintf("test_%s_%d", hex, time.Now().Unix())
	return whutils.ToProviderCase(provider, whutils.ToSafeNamespace(provider,
		namespace,
	))
}

func TestSnowPipeStreaming(t *testing.T) {
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

	c := testcompose.New(t, compose.FilePaths([]string{"testdata/docker-compose.rudder-snowpipe-clients.yml", "testdata/docker-compose.rudder-transformer.yml"}))
	c.Start(context.Background())

	transformerURL := fmt.Sprintf("http://localhost:%d", c.Port("transformer", 9090))
	snowPipeClientsURL := fmt.Sprintf("http://localhost:%d", c.Port("rudder-snowpipe-clients", 9078))

	keyPairUnEncryptedCredentials, err := getSnowpipeTestCredentials(testKeyPairUnencrypted)
	require.NoError(t, err)

	t.Run("namespace and table already exists", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		namespace := randSchema(whutils.SNOWFLAKE)

		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			WithID("destination1").
			WithConfigOption("account", keyPairUnEncryptedCredentials.Account).
			WithConfigOption("warehouse", keyPairUnEncryptedCredentials.Warehouse).
			WithConfigOption("database", keyPairUnEncryptedCredentials.Database).
			WithConfigOption("role", keyPairUnEncryptedCredentials.Role).
			WithConfigOption("user", keyPairUnEncryptedCredentials.User).
			WithConfigOption("useKeyPairAuth", true).
			WithConfigOption("privateKey", keyPairUnEncryptedCredentials.PrivateKey).
			WithConfigOption("privateKeyPassphrase", keyPairUnEncryptedCredentials.PrivateKeyPassphrase).
			WithConfigOption("namespace", namespace).
			WithRevisionID("destination1").
			Build()
		source := backendconfigtest.NewSourceBuilder().
			WithID("source1").
			WithWriteKey("writekey1").
			WithConnection(destination).
			Build()
		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(source).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowPipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "IDENTIFIES", whutils.ModelTableSchema{
			"CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_SOURCE_TYPE": "string", "ORIGINAL_TIMESTAMP": "datetime", "UUID_TS": "datetime", "CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_PASSED_IP": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "CONTEXT_SOURCE_ID": "string", "CONTEXT_REQUEST_IP": "string", "ID": "string", "RECEIVED_AT": "datetime", "USER_ID": "string",
		}))
		require.NoError(t, sm.CreateTable(ctx, "USERS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_REQUEST_IP": "string", "RECEIVED_AT": "datetime", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "ID": "string", "UUID_TS": "datetime", "ORIGINAL_TIMESTAMP": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"messageId": "%[1]s", "userId":"%[1]s","type":"%[2]s","context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z"}]}`,
				strconv.Itoa(index+1),
				"identify",
			)
		}

		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 5
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 10
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"IDENTIFIES": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"USERS":      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		usersRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "USERS"))
		ts := timeutil.Now().Format("2006-01-02")
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
		},
			usersRecords,
		)
		identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "IDENTIFIES"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
		},
			identifiesRecords,
		)

		cancel()
		_ = wg.Wait()
	})

	t.Run("namespace does not exists", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		namespace := randSchema(whutils.SNOWFLAKE)

		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			WithID("destination1").
			WithConfigOption("account", keyPairUnEncryptedCredentials.Account).
			WithConfigOption("warehouse", keyPairUnEncryptedCredentials.Warehouse).
			WithConfigOption("database", keyPairUnEncryptedCredentials.Database).
			WithConfigOption("role", keyPairUnEncryptedCredentials.Role).
			WithConfigOption("user", keyPairUnEncryptedCredentials.User).
			WithConfigOption("useKeyPairAuth", true).
			WithConfigOption("privateKey", keyPairUnEncryptedCredentials.PrivateKey).
			WithConfigOption("privateKeyPassphrase", keyPairUnEncryptedCredentials.PrivateKeyPassphrase).
			WithConfigOption("namespace", namespace).
			WithRevisionID("destination1").
			Build()
		source := backendconfigtest.NewSourceBuilder().
			WithID("source1").
			WithWriteKey("writekey1").
			WithConnection(destination).
			Build()
		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(source).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowPipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"messageId": "%[1]s", "userId":"%[1]s","type":"%[2]s","context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z"}]}`,
				strconv.Itoa(index+1),
				"identify",
			)
		}

		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 5
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 10
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"IDENTIFIES": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"USERS":      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		usersRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "USERS"))
		ts := timeutil.Now().Format("2006-01-02")
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
		},
			usersRecords,
		)
		identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "IDENTIFIES"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
		},
			identifiesRecords,
		)

		cancel()
		_ = wg.Wait()
	})

	t.Run("table does not exists", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		namespace := randSchema(whutils.SNOWFLAKE)

		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			WithID("destination1").
			WithConfigOption("account", keyPairUnEncryptedCredentials.Account).
			WithConfigOption("warehouse", keyPairUnEncryptedCredentials.Warehouse).
			WithConfigOption("database", keyPairUnEncryptedCredentials.Database).
			WithConfigOption("role", keyPairUnEncryptedCredentials.Role).
			WithConfigOption("user", keyPairUnEncryptedCredentials.User).
			WithConfigOption("useKeyPairAuth", true).
			WithConfigOption("privateKey", keyPairUnEncryptedCredentials.PrivateKey).
			WithConfigOption("privateKeyPassphrase", keyPairUnEncryptedCredentials.PrivateKeyPassphrase).
			WithConfigOption("namespace", namespace).
			WithRevisionID("destination1").
			Build()
		source := backendconfigtest.NewSourceBuilder().
			WithID("source1").
			WithWriteKey("writekey1").
			WithConnection(destination).
			Build()
		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(source).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowPipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"messageId": "%[1]s", "userId":"%[1]s","type":"%[2]s","context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z"}]}`,
				strconv.Itoa(index+1),
				"identify",
			)
		}

		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 5
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 10
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"IDENTIFIES": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"USERS":      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		usersRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "USERS"))
		ts := timeutil.Now().Format("2006-01-02")
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
		},
			usersRecords,
		)
		identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "IDENTIFIES"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
		},
			identifiesRecords,
		)

		cancel()
		_ = wg.Wait()
	})

	t.Run("events with different schema", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		namespace := randSchema(whutils.SNOWFLAKE)

		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			WithID("destination1").
			WithConfigOption("account", keyPairUnEncryptedCredentials.Account).
			WithConfigOption("warehouse", keyPairUnEncryptedCredentials.Warehouse).
			WithConfigOption("database", keyPairUnEncryptedCredentials.Database).
			WithConfigOption("role", keyPairUnEncryptedCredentials.Role).
			WithConfigOption("user", keyPairUnEncryptedCredentials.User).
			WithConfigOption("useKeyPairAuth", true).
			WithConfigOption("privateKey", keyPairUnEncryptedCredentials.PrivateKey).
			WithConfigOption("privateKeyPassphrase", keyPairUnEncryptedCredentials.PrivateKeyPassphrase).
			WithConfigOption("namespace", namespace).
			WithRevisionID("destination1").
			Build()
		source := backendconfigtest.NewSourceBuilder().
			WithID("source1").
			WithWriteKey("writekey1").
			WithConnection(destination).
			Build()
		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(source).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowPipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "IDENTIFIES", whutils.ModelTableSchema{
			"CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_SOURCE_TYPE": "string", "ORIGINAL_TIMESTAMP": "datetime", "UUID_TS": "datetime", "CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_PASSED_IP": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "CONTEXT_SOURCE_ID": "string", "CONTEXT_REQUEST_IP": "string", "ID": "string", "RECEIVED_AT": "datetime", "USER_ID": "string",
		}))
		require.NoError(t, sm.CreateTable(ctx, "USERS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_REQUEST_IP": "string", "RECEIVED_AT": "datetime", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "ID": "string", "UUID_TS": "datetime", "ORIGINAL_TIMESTAMP": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"messageId": "%[1]s", "userId":"%[1]s","type":"%[2]s","context":{"ip":"14.5.67.21","library":{"name":"http"}, "additional_column_%[1]s": "%[1]s"},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z"}]}`,
				strconv.Itoa(index+1),
				"identify",
			)
		}

		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 5
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 10
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"IDENTIFIES": {"CONTEXT_ADDITIONAL_COLUMN_1": "TEXT", "CONTEXT_ADDITIONAL_COLUMN_2": "TEXT", "CONTEXT_ADDITIONAL_COLUMN_3": "TEXT", "CONTEXT_ADDITIONAL_COLUMN_4": "TEXT", "CONTEXT_ADDITIONAL_COLUMN_5": "TEXT", "CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"USERS":      {"CONTEXT_ADDITIONAL_COLUMN_1": "TEXT", "CONTEXT_ADDITIONAL_COLUMN_2": "TEXT", "CONTEXT_ADDITIONAL_COLUMN_3": "TEXT", "CONTEXT_ADDITIONAL_COLUMN_4": "TEXT", "CONTEXT_ADDITIONAL_COLUMN_5": "TEXT", "CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		usersRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, TO_CHAR(UUID_TS, 'YYYY-MM-DD'), CONTEXT_ADDITIONAL_COLUMN_1, CONTEXT_ADDITIONAL_COLUMN_2, CONTEXT_ADDITIONAL_COLUMN_3, CONTEXT_ADDITIONAL_COLUMN_4, CONTEXT_ADDITIONAL_COLUMN_5 FROM %q.%q;`, namespace, "USERS"))
		ts := timeutil.Now().Format("2006-01-02")
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts, "1", "", "", "", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts, "", "2", "", "", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts, "", "", "3", "", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts, "", "", "", "4", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts, "", "", "", "", "5"},
		},
			usersRecords,
		)
		identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD'), CONTEXT_ADDITIONAL_COLUMN_1, CONTEXT_ADDITIONAL_COLUMN_2, CONTEXT_ADDITIONAL_COLUMN_3, CONTEXT_ADDITIONAL_COLUMN_4, CONTEXT_ADDITIONAL_COLUMN_5 FROM %q.%q;`, namespace, "IDENTIFIES"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts, "1", "", "", "", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts, "", "2", "", "", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts, "", "", "3", "", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts, "", "", "", "4", ""},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts, "", "", "", "", "5"},
		},
			identifiesRecords,
		)

		cancel()
		_ = wg.Wait()
	})

	t.Run("discards", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		namespace := randSchema(whutils.SNOWFLAKE)

		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			WithID("destination1").
			WithConfigOption("account", keyPairUnEncryptedCredentials.Account).
			WithConfigOption("warehouse", keyPairUnEncryptedCredentials.Warehouse).
			WithConfigOption("database", keyPairUnEncryptedCredentials.Database).
			WithConfigOption("role", keyPairUnEncryptedCredentials.Role).
			WithConfigOption("user", keyPairUnEncryptedCredentials.User).
			WithConfigOption("useKeyPairAuth", true).
			WithConfigOption("privateKey", keyPairUnEncryptedCredentials.PrivateKey).
			WithConfigOption("privateKeyPassphrase", keyPairUnEncryptedCredentials.PrivateKeyPassphrase).
			WithConfigOption("namespace", namespace).
			WithRevisionID("destination1").
			Build()
		source := backendconfigtest.NewSourceBuilder().
			WithID("source1").
			WithWriteKey("writekey1").
			WithConnection(destination).
			Build()
		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(source).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowPipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "IDENTIFIES", whutils.ModelTableSchema{
			"CONTEXT_IP": "int", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_SOURCE_TYPE": "string", "ORIGINAL_TIMESTAMP": "datetime", "UUID_TS": "datetime", "CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_PASSED_IP": "int", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "CONTEXT_SOURCE_ID": "string", "CONTEXT_REQUEST_IP": "int", "ID": "string", "RECEIVED_AT": "datetime", "USER_ID": "string",
		}))
		require.NoError(t, sm.CreateTable(ctx, "USERS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_IP": "int", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_PASSED_IP": "int", "CONTEXT_REQUEST_IP": "int", "RECEIVED_AT": "datetime", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "ID": "string", "UUID_TS": "datetime", "ORIGINAL_TIMESTAMP": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"messageId": "%[1]s", "userId":"%[1]s","type":"%[2]s","context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z"}]}`,
				strconv.Itoa(index+1),
				"identify",
			)
		}

		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 5
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 10
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"IDENTIFIES":      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_REQUEST_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS": {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"USERS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_REQUEST_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		usersRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "USERS"))
		ts := timeutil.Now().Format("2006-01-02")
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
		},
			usersRecords,
		)
		identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "IDENTIFIES"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
		},
			identifiesRecords,
		)
		discardsRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_VALUE, REASON, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), ROW_ID, TABLE_NAME, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "RUDDER_DISCARDS"))
		require.ElementsMatch(t, [][]string{
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "1", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "1", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "1", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "2", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "2", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "2", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "3", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "3", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "3", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "4", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "4", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "4", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "5", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "5", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "5", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "1", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "1", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "1", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "2", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "2", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "2", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "3", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "3", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "3", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "4", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "4", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "4", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "5", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "5", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "5", "USERS", ts},
		},
			discardsRecords,
		)

		cancel()
		_ = wg.Wait()
	})

	t.Run("discards migration for reason", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		namespace := randSchema(whutils.SNOWFLAKE)

		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			WithID("destination1").
			WithConfigOption("account", keyPairUnEncryptedCredentials.Account).
			WithConfigOption("warehouse", keyPairUnEncryptedCredentials.Warehouse).
			WithConfigOption("database", keyPairUnEncryptedCredentials.Database).
			WithConfigOption("role", keyPairUnEncryptedCredentials.Role).
			WithConfigOption("user", keyPairUnEncryptedCredentials.User).
			WithConfigOption("useKeyPairAuth", true).
			WithConfigOption("privateKey", keyPairUnEncryptedCredentials.PrivateKey).
			WithConfigOption("privateKeyPassphrase", keyPairUnEncryptedCredentials.PrivateKeyPassphrase).
			WithConfigOption("namespace", namespace).
			WithRevisionID("destination1").
			Build()
		source := backendconfigtest.NewSourceBuilder().
			WithID("source1").
			WithWriteKey("writekey1").
			WithConnection(destination).
			Build()
		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(source).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowPipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "IDENTIFIES", whutils.ModelTableSchema{
			"CONTEXT_IP": "int", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_SOURCE_TYPE": "string", "ORIGINAL_TIMESTAMP": "datetime", "UUID_TS": "datetime", "CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_PASSED_IP": "int", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "CONTEXT_SOURCE_ID": "string", "CONTEXT_REQUEST_IP": "int", "ID": "string", "RECEIVED_AT": "datetime", "USER_ID": "string",
		}))
		require.NoError(t, sm.CreateTable(ctx, "USERS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_IP": "int", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_PASSED_IP": "int", "CONTEXT_REQUEST_IP": "int", "RECEIVED_AT": "datetime", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "ID": "string", "UUID_TS": "datetime", "ORIGINAL_TIMESTAMP": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "RUDDER_DISCARDS", whutils.ModelTableSchema{
			"COLUMN_NAME": "string", "COLUMN_VALUE": "string", "RECEIVED_AT": "datetime", "ROW_ID": "string", "TABLE_NAME": "string", "UUID_TS": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"messageId": "%[1]s", "userId":"%[1]s","type":"%[2]s","context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z"}]}`,
				strconv.Itoa(index+1),
				"identify",
			)
		}

		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 5
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 10
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"IDENTIFIES":      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_REQUEST_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS": {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"USERS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_REQUEST_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		usersRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "USERS"))
		ts := timeutil.Now().Format("2006-01-02")
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
		},
			usersRecords,
		)
		identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "IDENTIFIES"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
		},
			identifiesRecords,
		)
		discardsRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_VALUE, REASON, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), ROW_ID, TABLE_NAME, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "RUDDER_DISCARDS"))
		require.ElementsMatch(t, [][]string{
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "1", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "1", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "1", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "2", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "2", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "2", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "3", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "3", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "3", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "4", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "4", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "4", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "5", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "5", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "5", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "1", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "1", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "1", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "2", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "2", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "2", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "3", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "3", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "3", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "4", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "4", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "4", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "5", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "5", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "5", "USERS", ts},
		},
			discardsRecords,
		)

		cancel()
		_ = wg.Wait()
	})

	t.Run("discards migrated", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		namespace := randSchema(whutils.SNOWFLAKE)

		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			WithID("destination1").
			WithConfigOption("account", keyPairUnEncryptedCredentials.Account).
			WithConfigOption("warehouse", keyPairUnEncryptedCredentials.Warehouse).
			WithConfigOption("database", keyPairUnEncryptedCredentials.Database).
			WithConfigOption("role", keyPairUnEncryptedCredentials.Role).
			WithConfigOption("user", keyPairUnEncryptedCredentials.User).
			WithConfigOption("useKeyPairAuth", true).
			WithConfigOption("privateKey", keyPairUnEncryptedCredentials.PrivateKey).
			WithConfigOption("privateKeyPassphrase", keyPairUnEncryptedCredentials.PrivateKeyPassphrase).
			WithConfigOption("namespace", namespace).
			WithRevisionID("destination1").
			Build()
		source := backendconfigtest.NewSourceBuilder().
			WithID("source1").
			WithWriteKey("writekey1").
			WithConnection(destination).
			Build()
		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(source).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowPipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "IDENTIFIES", whutils.ModelTableSchema{
			"CONTEXT_IP": "int", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_SOURCE_TYPE": "string", "ORIGINAL_TIMESTAMP": "datetime", "UUID_TS": "datetime", "CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_PASSED_IP": "int", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "CONTEXT_SOURCE_ID": "string", "CONTEXT_REQUEST_IP": "int", "ID": "string", "RECEIVED_AT": "datetime", "USER_ID": "string",
		}))
		require.NoError(t, sm.CreateTable(ctx, "USERS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_IP": "int", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_PASSED_IP": "int", "CONTEXT_REQUEST_IP": "int", "RECEIVED_AT": "datetime", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "ID": "string", "UUID_TS": "datetime", "ORIGINAL_TIMESTAMP": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime",
		}))
		require.NoError(t, sm.CreateTable(ctx, "RUDDER_DISCARDS", whutils.ModelTableSchema{
			"COLUMN_NAME": "string", "COLUMN_VALUE": "string", "RECEIVED_AT": "datetime", "ROW_ID": "string", "TABLE_NAME": "string", "UUID_TS": "datetime", "REASON": "string",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"messageId": "%[1]s", "userId":"%[1]s","type":"%[2]s","context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z"}]}`,
				strconv.Itoa(index+1),
				"identify",
			)
		}

		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 5
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 10
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"IDENTIFIES":      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_REQUEST_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"RUDDER_DISCARDS": {"COLUMN_NAME": "TEXT", "COLUMN_VALUE": "TEXT", "REASON": "TEXT", "RECEIVED_AT": "TIMESTAMP_TZ", "ROW_ID": "TEXT", "TABLE_NAME": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"USERS":           {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "NUMBER", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "NUMBER", "CONTEXT_REQUEST_IP": "NUMBER", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		usersRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "USERS"))
		ts := timeutil.Now().Format("2006-01-02")
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
		},
			usersRecords,
		)
		identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "IDENTIFIES"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "", "http", "", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
		},
			identifiesRecords,
		)
		discardsRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT COLUMN_NAME, COLUMN_VALUE, REASON, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), ROW_ID, TABLE_NAME, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "RUDDER_DISCARDS"))
		require.ElementsMatch(t, [][]string{
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "1", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "1", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "1", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "2", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "2", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "2", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "3", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "3", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "3", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "4", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "4", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "4", "IDENTIFIES", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "5", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "5", "IDENTIFIES", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "5", "IDENTIFIES", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "1", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "1", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "1", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "2", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "2", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "2", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "3", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "3", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "3", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "4", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "4", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "4", "USERS", ts},
			{"CONTEXT_REQUEST_IP", "", "incompatible schema conversion from int to string", ts, "5", "USERS", ts},
			{"CONTEXT_PASSED_IP", "", "incompatible schema conversion from int to string", ts, "5", "USERS", ts},
			{"CONTEXT_IP", "", "incompatible schema conversion from int to string", ts, "5", "USERS", ts},
		},
			discardsRecords,
		)

		cancel()
		_ = wg.Wait()
	})

	t.Run("don't re-create channel on loading twice when successful", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		prometheusPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		namespace := randSchema(whutils.SNOWFLAKE)

		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			WithID("destination1").
			WithConfigOption("account", keyPairUnEncryptedCredentials.Account).
			WithConfigOption("warehouse", keyPairUnEncryptedCredentials.Warehouse).
			WithConfigOption("database", keyPairUnEncryptedCredentials.Database).
			WithConfigOption("role", keyPairUnEncryptedCredentials.Role).
			WithConfigOption("user", keyPairUnEncryptedCredentials.User).
			WithConfigOption("useKeyPairAuth", true).
			WithConfigOption("privateKey", keyPairUnEncryptedCredentials.PrivateKey).
			WithConfigOption("privateKeyPassphrase", keyPairUnEncryptedCredentials.PrivateKeyPassphrase).
			WithConfigOption("namespace", namespace).
			WithRevisionID("destination1").
			Build()
		source := backendconfigtest.NewSourceBuilder().
			WithID("source1").
			WithWriteKey("writekey1").
			WithConnection(destination).
			Build()
		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(source).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			config.Set("enableStats", true)
			config.Set("RuntimeStats.enabled", false)
			config.Set("OpenTelemetry.enabled", true)
			config.Set("OpenTelemetry.metrics.prometheus.enabled", true)
			config.Set("OpenTelemetry.metrics.prometheus.port", strconv.Itoa(prometheusPort))
			config.Set("OpenTelemetry.metrics.exportInterval", "10ms")

			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowPipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "IDENTIFIES", whutils.ModelTableSchema{
			"CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_SOURCE_TYPE": "string", "ORIGINAL_TIMESTAMP": "datetime", "UUID_TS": "datetime", "CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_PASSED_IP": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "CONTEXT_SOURCE_ID": "string", "CONTEXT_REQUEST_IP": "string", "ID": "string", "RECEIVED_AT": "datetime", "USER_ID": "string",
		}))
		require.NoError(t, sm.CreateTable(ctx, "USERS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_REQUEST_IP": "string", "RECEIVED_AT": "datetime", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "ID": "string", "UUID_TS": "datetime", "ORIGINAL_TIMESTAMP": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"messageId": "%[1]s", "userId":"%[1]s","type":"%[2]s","context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z"}]}`,
				strconv.Itoa(index+1),
				"identify",
			)
		}

		t.Log("Sending 5 events")
		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 5
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 10
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		t.Log("Sending 5 events again")
		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 10
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 20
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		metrics := getPrometheusMetrics(t, prometheusPort)
		require.Equal(t, 1, len(metrics["snowpipestreaming_create_channel_count"].GetMetric()))
		require.Equal(t, float64(2), metrics["snowpipestreaming_create_channel_count"].GetMetric()[0].Counter.GetValue())

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"IDENTIFIES": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"USERS":      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		usersRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "USERS"))
		ts := timeutil.Now().Format("2006-01-02")
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
		},
			usersRecords,
		)
		identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "IDENTIFIES"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
		},
			identifiesRecords,
		)

		cancel()
		_ = wg.Wait()
	})

	t.Run("many tables", func(t *testing.T) {})

	t.Run("schema modified after channel creation (datatype changed)", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		prometheusPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		namespace := randSchema(whutils.SNOWFLAKE)

		destination := backendconfigtest.
			NewDestinationBuilder("SNOWPIPE_STREAMING").
			WithID("destination1").
			WithConfigOption("account", keyPairUnEncryptedCredentials.Account).
			WithConfigOption("warehouse", keyPairUnEncryptedCredentials.Warehouse).
			WithConfigOption("database", keyPairUnEncryptedCredentials.Database).
			WithConfigOption("role", keyPairUnEncryptedCredentials.Role).
			WithConfigOption("user", keyPairUnEncryptedCredentials.User).
			WithConfigOption("useKeyPairAuth", true).
			WithConfigOption("privateKey", keyPairUnEncryptedCredentials.PrivateKey).
			WithConfigOption("privateKeyPassphrase", keyPairUnEncryptedCredentials.PrivateKeyPassphrase).
			WithConfigOption("namespace", namespace).
			WithRevisionID("destination1").
			Build()
		source := backendconfigtest.NewSourceBuilder().
			WithID("source1").
			WithWriteKey("writekey1").
			WithConnection(destination).
			Build()
		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(source).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			config.Set("enableStats", true)
			config.Set("RuntimeStats.enabled", false)
			config.Set("OpenTelemetry.enabled", true)
			config.Set("OpenTelemetry.metrics.prometheus.enabled", true)
			config.Set("OpenTelemetry.metrics.prometheus.port", strconv.Itoa(prometheusPort))
			config.Set("OpenTelemetry.metrics.exportInterval", "10ms")

			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowPipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "IDENTIFIES", whutils.ModelTableSchema{
			"CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_SOURCE_TYPE": "string", "ORIGINAL_TIMESTAMP": "datetime", "UUID_TS": "datetime", "CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_PASSED_IP": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "CONTEXT_SOURCE_ID": "string", "CONTEXT_REQUEST_IP": "string", "ID": "string", "RECEIVED_AT": "datetime", "USER_ID": "string",
		}))
		require.NoError(t, sm.CreateTable(ctx, "USERS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_REQUEST_IP": "string", "RECEIVED_AT": "datetime", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "ID": "string", "UUID_TS": "datetime", "ORIGINAL_TIMESTAMP": "datetime", "SENT_AT": "datetime", "TIMESTAMP": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"messageId": "%[1]s", "userId":"%[1]s","type":"%[2]s","context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z","sentAt":"2020-02-02T00:23:09.544Z","originalTimestamp":"2020-02-02T00:23:09.544Z","receivedAt":"2020-02-02T00:23:09.544Z"}]}`,
				strconv.Itoa(index+1),
				"identify",
			)
		}

		t.Log("Sending 5 events")
		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 5
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 10
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		t.Log("Schema modified")
		_, err = sm.DB.DB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.USERS DROP COLUMN CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_REQUEST_IP;", namespace))
		require.NoError(t, err)
		_, err = sm.DB.DB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.IDENTIFIES DROP COLUMN CONTEXT_IP, CONTEXT_PASSED_IP, CONTEXT_REQUEST_IP;", namespace))
		require.NoError(t, err)
		_, err = sm.DB.DB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.USERS ADD COLUMN CONTEXT_IP NUMBER, CONTEXT_PASSED_IP NUMBER, CONTEXT_REQUEST_IP NUMBER;", namespace))
		require.NoError(t, err)
		_, err = sm.DB.DB.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.IDENTIFIES ADD COLUMN CONTEXT_IP NUMBER, CONTEXT_PASSED_IP NUMBER, CONTEXT_REQUEST_IP NUMBER;", namespace))
		require.NoError(t, err)

		t.Log("Sending 5 events again")
		err = sendEvents(5, eventFormat, "writekey1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 10
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("batch_rt succeeded: %d", jobsCount)
			return jobsCount == 20
		}, 200*time.Second, 1*time.Second, "all events should be aborted in batch router")

		metrics := getPrometheusMetrics(t, prometheusPort)
		require.Equal(t, 1, len(metrics["snowpipestreaming_create_channel_count"].GetMetric()))
		require.Equal(t, float64(2), metrics["snowpipestreaming_create_channel_count"].GetMetric()[0].Counter.GetValue())

		schema := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s';`, namespace))
		require.Equal(t, map[string]map[string]string{
			"IDENTIFIES": {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "USER_ID": "TEXT", "UUID_TS": "TIMESTAMP_TZ"},
			"USERS":      {"CONTEXT_DESTINATION_ID": "TEXT", "CONTEXT_DESTINATION_TYPE": "TEXT", "CONTEXT_IP": "TEXT", "CONTEXT_LIBRARY_NAME": "TEXT", "CONTEXT_PASSED_IP": "TEXT", "CONTEXT_REQUEST_IP": "TEXT", "CONTEXT_SOURCE_ID": "TEXT", "CONTEXT_SOURCE_TYPE": "TEXT", "ID": "TEXT", "ORIGINAL_TIMESTAMP": "TIMESTAMP_TZ", "RECEIVED_AT": "TIMESTAMP_TZ", "SENT_AT": "TIMESTAMP_TZ", "TIMESTAMP": "TIMESTAMP_TZ", "UUID_TS": "TIMESTAMP_TZ"},
		},
			convertRecordsToSchema(schema),
		)

		usersRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "USERS"))
		ts := timeutil.Now().Format("2006-01-02")
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", ts},
		},
			usersRecords,
		)
		identifiesRecords := whth.RetrieveRecordsFromWarehouse(t, sm.DB.DB, fmt.Sprintf(`SELECT CONTEXT_DESTINATION_ID, CONTEXT_DESTINATION_TYPE, CONTEXT_IP, CONTEXT_LIBRARY_NAME, CONTEXT_PASSED_IP, CONTEXT_SOURCE_ID, CONTEXT_SOURCE_TYPE, ID, ORIGINAL_TIMESTAMP, TO_CHAR(RECEIVED_AT, 'YYYY-MM-DD'), SENT_AT, TIMESTAMP, USER_ID, TO_CHAR(UUID_TS, 'YYYY-MM-DD') FROM %q.%q;`, namespace, "IDENTIFIES"))
		require.ElementsMatch(t, [][]string{
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "1", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "1", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "2", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "2", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "3", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "3", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "4", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "4", ts},
			{destination.ID, "SNOWPIPE_STREAMING", "14.5.67.21", "http", "14.5.67.21", source.ID, source.SourceDefinition.Name, "5", "2020-02-02T00:23:09Z", ts, "2020-02-02T00:23:09Z", "2020-02-02T00:23:09Z", "5", ts},
		},
			identifiesRecords,
		)

		cancel()
		_ = wg.Wait()
	})

	t.Run("schema modified after channel creation (table deleted)", func(t *testing.T) {})

	t.Run("schema modified after channel creation (schema deleted)", func(t *testing.T) {})

	t.Run("schema modified after channel creation (columns deleted)", func(t *testing.T) {})
}

func runRudderServer(ctx context.Context, port int, postgresContainer *postgres.Resource, cbURL, transformerURL, snowpipeClientsURL, tmpDir string) (err error) {
	config.Set("CONFIG_BACKEND_URL", cbURL)
	config.Set("WORKSPACE_TOKEN", "token")
	config.Set("DB.host", postgresContainer.Host)
	config.Set("DB.port", postgresContainer.Port)
	config.Set("DB.user", postgresContainer.User)
	config.Set("DB.name", postgresContainer.Database)
	config.Set("DB.password", postgresContainer.Password)
	config.Set("DEST_TRANSFORM_URL", transformerURL)
	config.Set("SnowpipeStreaming.Client.URL", snowpipeClientsURL)
	config.Set("BatchRouter.pollStatusLoopSleep", "1s")
	config.Set("BatchRouter.asyncUploadTimeout", "1s")
	config.Set("BatchRouter.asyncUploadWorkerTimeout", "1s")
	config.Set("BatchRouter.mainLoopFreq", "1s")
	config.Set("BatchRouter.uploadFreq", "1s")
	config.Set("BatchRouter.isolationMode", "none")

	config.Set("Warehouse.mode", "off")
	config.Set("DestinationDebugger.disableEventDeliveryStatusUploads", true)
	config.Set("SourceDebugger.disableEventUploads", true)
	config.Set("TransformationDebugger.disableTransformationStatusUploads", true)
	config.Set("JobsDB.backup.enabled", false)
	config.Set("JobsDB.migrateDSLoopSleepDuration", "60m")
	config.Set("archival.Enabled", false)
	config.Set("Reporting.syncer.enabled", false)
	config.Set("BatchRouter.mainLoopFreq", "1s")
	config.Set("BatchRouter.uploadFreq", "1s")
	config.Set("Gateway.webPort", strconv.Itoa(port))
	config.Set("RUDDER_TMPDIR", os.TempDir())
	config.Set("recovery.storagePath", path.Join(tmpDir, "/recovery_data.json"))
	config.Set("recovery.enabled", false)
	config.Set("Profiler.Enabled", false)
	config.Set("Gateway.enableSuppressUserFeature", false)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "TOKEN"})
	c := r.Run(ctx,
		[]string{"proc-isolation-test-rudder-server"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return
}

func sendEvents(num int, eventFormat func(index int) string, writeKey, url string) error { // nolint:unparam
	for i := 0; i < num; i++ {
		payload := []byte(eventFormat(i))
		req, err := http.NewRequest(http.MethodPost, url+"/v1/batch", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.SetBasicAuth(writeKey, "password")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to send event to rudder server, status code: %d: %s", resp.StatusCode, string(b))
		}
		func() { kithttputil.CloseResponse(resp) }()
	}
	return nil
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

func convertRecordsToSchema(input [][]string) map[string]map[string]string {
	return lo.MapValues(lo.GroupBy(input, func(row []string) string {
		return row[0]
	}), func(columns [][]string, _ string) map[string]string {
		return lo.SliceToMap(columns, func(col []string) (string, string) {
			return col[1], col[2]
		})
	})
}

func getPrometheusMetrics(t *testing.T, prometheusPort int, requiredMetrics ...string) map[string]*promClient.MetricFamily {
	t.Helper()

	buf := make([]byte, 0)
	url := fmt.Sprintf("http://localhost:%d/metrics", prometheusPort)

	require.Eventuallyf(t, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			t.Logf("Failed to fetch metrics: %v", err)
			return false
		}
		defer httputil.CloseResponse(resp)

		buf, err = io.ReadAll(resp.Body)
		if err != nil {
			t.Logf("Failed to read response body: %v", err)
			return false
		}

		bufString := string(buf)
		for _, metric := range requiredMetrics {
			if !strings.Contains(bufString, metric) {
				return false
			}
		}
		return true
	}, time.Minute, 100*time.Millisecond, "Cannot find metrics in time: %s", buf)

	metrics, err := testhelper.ParsePrometheusMetrics(bytes.NewBuffer(buf))
	require.NoError(t, err)

	return metrics
}
