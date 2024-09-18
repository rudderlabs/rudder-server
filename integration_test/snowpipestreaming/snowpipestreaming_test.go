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
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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

func randSchema(provider string) string { // nolint:unparam
	hex := strings.ToLower(rand.String(12))
	namespace := fmt.Sprintf("test_%s_%d", hex, time.Now().Unix())
	return whutils.ToProviderCase(provider, whutils.ToSafeNamespace(provider,
		namespace,
	))
}

func TestSnowpipeStreaming(t *testing.T) {
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
	snowpipeClientsURL := fmt.Sprintf("http://localhost:%d", c.Port("rudder-snowpipe-clients", 9078))

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
		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source1").
							WithWriteKey("writekey1").
							WithConnection(destination).
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "IDENTIFIES", whutils.ModelTableSchema{
			"CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_SOURCE_TYPE": "string", "ORIGINAL_TIMESTAMP": "datetime", "UUID_TS": "datetime", "CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_PASSED_IP": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "CONTEXT_SOURCE_ID": "string", "CONTEXT_TRAITS_TRAIT_1": "string", "CONTEXT_REQUEST_IP": "string", "ID": "string", "RECEIVED_AT": "datetime", "TRAIT_1": "string", "USER_ID": "string",
		}))
		require.NoError(t, sm.CreateTable(ctx, "USERS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_TRAITS_TRAIT_1": "string", "RECEIVED_AT": "datetime", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "ID": "string", "TRAIT_1": "string", "UUID_TS": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"userId":"%[1]s","type":"%[2]s","context":{"traits":{"trait1":"new-val"},"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z"}]}`,
				rand.String(10),
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

		var (
			identifiesCount int
			usersCount      int
		)

		err = sm.DB.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q;`, namespace, "IDENTIFIES")).Scan(&identifiesCount)
		require.NoError(t, err)
		require.Equal(t, 5, identifiesCount)

		err = sm.DB.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q;`, namespace, "USERS")).Scan(&usersCount)
		require.NoError(t, err)
		require.Equal(t, 5, usersCount)

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
		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source1").
							WithWriteKey("writekey1").
							WithConnection(destination).
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"userId":"%[1]s","type":"%[2]s","context":{"traits":{"trait1":"new-val"},"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z"}]}`,
				rand.String(10),
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

		var (
			identifiesCount int
			usersCount      int
		)

		err = sm.DB.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q;`, namespace, "IDENTIFIES")).Scan(&identifiesCount)
		require.NoError(t, err)
		require.Equal(t, 5, identifiesCount)

		err = sm.DB.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q;`, namespace, "USERS")).Scan(&usersCount)
		require.NoError(t, err)
		require.Equal(t, 5, usersCount)

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
		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source1").
							WithWriteKey("writekey1").
							WithConnection(destination).
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"userId":"%[1]s","type":"%[2]s","context":{"traits":{"trait1":"new-val"},"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z"}]}`,
				rand.String(10),
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

		var (
			identifiesCount int
			usersCount      int
		)

		err = sm.DB.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q;`, namespace, "IDENTIFIES")).Scan(&identifiesCount)
		require.NoError(t, err)
		require.Equal(t, 5, identifiesCount)

		err = sm.DB.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q;`, namespace, "USERS")).Scan(&usersCount)
		require.NoError(t, err)
		require.Equal(t, 5, usersCount)

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
		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source1").
							WithWriteKey("writekey1").
							WithConnection(destination).
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"userId":"%[1]s","type":"%[2]s","context":{"traits":{"trait%[3]d":"new-val"},"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z"}]}`,
				rand.String(10),
				"identify",
				index,
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

		var (
			identifiesCount int
			usersCount      int
		)

		err = sm.DB.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q;`, namespace, "IDENTIFIES")).Scan(&identifiesCount)
		require.NoError(t, err)
		require.Equal(t, 5, identifiesCount)

		err = sm.DB.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q;`, namespace, "USERS")).Scan(&usersCount)
		require.NoError(t, err)
		require.Equal(t, 5, usersCount)

		cancel()
		_ = wg.Wait()
	})

	t.Run("addition of new properties", func(t *testing.T) {
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
		warehouse := whutils.ModelWarehouse{
			Namespace:   namespace,
			Destination: destination,
		}

		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source1").
							WithWriteKey("writekey1").
							WithConnection(destination).
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, bcServer.URL, transformerURL, snowpipeClientsURL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		sm := snowflake.New(config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)
		require.NoError(t, sm.Setup(ctx, warehouse, &whutils.NopUploader{}))
		t.Cleanup(func() { sm.Cleanup(ctx) })
		require.NoError(t, sm.CreateSchema(ctx))
		t.Cleanup(func() { dropSchema(t, sm.DB.DB, namespace) })
		require.NoError(t, sm.CreateTable(ctx, "IDENTIFIES", whutils.ModelTableSchema{
			"CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_SOURCE_TYPE": "string", "ORIGINAL_TIMESTAMP": "datetime", "UUID_TS": "datetime", "CONTEXT_DESTINATION_ID": "string", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_PASSED_IP": "string", "SENT_AT": "datetime", "TIMESTAMP": "datetime", "CONTEXT_SOURCE_ID": "string", "CONTEXT_TRAITS_TRAIT_1": "string", "CONTEXT_REQUEST_IP": "string", "ID": "string", "RECEIVED_AT": "datetime", "TRAIT_1": "string", "USER_ID": "string",
		}))
		require.NoError(t, sm.CreateTable(ctx, "USERS", whutils.ModelTableSchema{
			"CONTEXT_DESTINATION_ID": "string", "CONTEXT_IP": "string", "CONTEXT_LIBRARY_NAME": "string", "CONTEXT_PASSED_IP": "string", "CONTEXT_REQUEST_IP": "string", "CONTEXT_TRAITS_TRAIT_1": "string", "RECEIVED_AT": "datetime", "CONTEXT_DESTINATION_TYPE": "string", "CONTEXT_SOURCE_ID": "string", "CONTEXT_SOURCE_TYPE": "string", "ID": "string", "TRAIT_1": "string", "UUID_TS": "datetime",
		}))

		eventFormat := func(index int) string {
			return fmt.Sprintf(`{"batch":[{"userId":"%[1]s","type":"%[2]s","context":{"traits":{"trait%[3]d":"new-val"},"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z"}]}`,
				rand.String(10),
				"identify",
				index,
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

		var (
			identifiesCount int
			usersCount      int
		)

		err = sm.DB.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q;`, namespace, "IDENTIFIES")).Scan(&identifiesCount)
		require.NoError(t, err)
		require.Equal(t, 5, identifiesCount)

		err = sm.DB.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %q.%q;`, namespace, "USERS")).Scan(&usersCount)
		require.NoError(t, err)
		require.Equal(t, 5, usersCount)

		cancel()
		_ = wg.Wait()
	})
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
	config.Set("Snowpipe.Client.URL", snowpipeClientsURL)
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
	c := r.Run(ctx, []string{"proc-isolation-test-rudder-server"})
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
