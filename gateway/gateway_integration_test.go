package gateway_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/runner"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
)

func TestWebhook(t *testing.T) {
	bcServer := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(
			backendconfigtest.NewConfigBuilder().
				WithSource(
					backendconfigtest.NewSourceBuilder().
						WithID("source-1").
						WithWriteKey("writekey-1").
						WithSourceCategory("webhook").
						WithSourceType("SeGment").
						Build()).
				Build()).
		Build()
	defer bcServer.Close()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	transformerContainer, err := transformertest.Setup(pool, t)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		err := runGateway(ctx, gwPort, postgresContainer, bcServer.URL, transformerContainer.TransformerURL, t.TempDir())
		if err != nil {
			t.Logf("rudder-server exited with error: %v", err)
		}
		return err
	})

	url := fmt.Sprintf("http://localhost:%d", gwPort)
	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	// send an event
	req, err := http.NewRequest(http.MethodPost, url+"/v1/webhook", bytes.NewReader([]byte(`{"userId": "user-1", "type": "identity"}`)))
	require.NoError(t, err)
	req.SetBasicAuth("writekey-1", "password")
	resp, err := (&http.Client{}).Do(req)
	require.NoError(t, err, "it should be able to send a webhook event to gateway")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	func() { kithttputil.CloseResponse(resp) }()
	require.NoError(t, err, "it should be able to send a webhook event to gateway")

	// check that the event is stored in jobsdb
	requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Unprocessed.State, 1)
}

func TestDocsEndpoint(t *testing.T) {
	bcServer := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(
			backendconfigtest.NewConfigBuilder().
				WithSource(
					backendconfigtest.NewSourceBuilder().
						WithID("source-1").
						WithWriteKey("writekey-1").
						WithSourceCategory("webhook").
						WithSourceType("my_source_type").
						Build()).
				Build()).
		Build()
	defer bcServer.Close()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	transformerContainer, err := transformertest.Setup(pool, t)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		err := runGateway(ctx, gwPort, postgresContainer, bcServer.URL, transformerContainer.TransformerURL, t.TempDir())
		if err != nil {
			t.Logf("rudder-server exited with error: %v", err)
		}
		return err
	})

	url := fmt.Sprintf("http://localhost:%d", gwPort)
	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	resp, err := http.Get(url + "/docs")
	require.NoError(t, err, "it should be able to get the docs")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { kithttputil.CloseResponse(resp) }()
	require.Equal(t, resp.Header.Get("Content-Type"), "text/html; charset=utf-8")
	all, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Greater(t, len(all), 0)
}

func runGateway(
	ctx context.Context,
	port int,
	postgresContainer *postgres.Resource,
	cbURL, transformerURL, tmpDir string,
) (err error) {
	// first run node migrations
	mg := &migrator.Migrator{
		Handle:                     postgresContainer.DB,
		MigrationsTable:            "node_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}

	err = mg.Migrate("node")
	if err != nil {
		return fmt.Errorf("unable to run the migrations for the node, err: %w", err)
	}

	// then start the server
	config.Set("CONFIG_BACKEND_URL", cbURL)
	config.Set("WORKSPACE_TOKEN", "token")
	config.Set("DB.host", postgresContainer.Host)
	config.Set("DB.port", postgresContainer.Port)
	config.Set("DB.user", postgresContainer.User)
	config.Set("DB.name", postgresContainer.Database)
	config.Set("DB.password", postgresContainer.Password)
	config.Set("DEST_TRANSFORM_URL", transformerURL)

	config.Set("APP_TYPE", "gateway")

	config.Set("Gateway.webPort", strconv.Itoa(port))
	config.Set("JobsDB.backup.enabled", false)
	config.Set("JobsDB.migrateDSLoopSleepDuration", "60m")
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
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, []string{"rudder-gw"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return
}

// nolint: unparam
func requireJobsCount(
	t *testing.T,
	db *sql.DB,
	queue, state string,
	expectedCount int,
) {
	t.Helper()

	query := fmt.Sprintf(`SELECT count(*) FROM unionjobsdbmetadata('%s',1) WHERE job_state = '%s';`, queue, state)
	if state == jobsdb.Unprocessed.State {
		query = fmt.Sprintf(`SELECT count(*) FROM unionjobsdbmetadata('%s',1) WHERE job_state IS NULL;`, queue)
	}
	require.Eventually(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRow(query).Scan(&jobsCount))
		t.Logf("%s %sJobCount: %d", queue, state, jobsCount)
		return jobsCount == expectedCount
	},
		20*time.Second,
		1*time.Second,
		fmt.Sprintf("%d %s events should be in %s state", expectedCount, queue, state),
	)
}
