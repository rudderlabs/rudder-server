package testhelper

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	"github.com/stretchr/testify/require"

	_ "embed"
)

//go:embed docker-compose.yaml
var dockerComposeFile compose.FileBytes

//go:embed .env
var env []byte

type testServer struct {
	URL string
}

func SetupServer(t *testing.T, workspaceConfigPath string) *testServer {
	t.Helper()
	var cancel context.CancelFunc
	ctx := context.Background()
	if deadline, ok := t.Deadline(); ok {
		ctx, cancel = context.WithDeadline(ctx, deadline)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	t.Cleanup(cancel)

	setupStart := time.Now()
	if testing.Verbose() {
		t.Setenv("LOG_LEVEL", "DEBUG")
	}

	config.Reset()
	logger.Reset()

	compose := testcompose.New(t, dockerComposeFile)

	compose.Start(ctx)
	t.Cleanup(func() {
		compose.Stop(ctx)
	})

	envs, err := godotenv.Parse(bytes.NewReader(env))
	require.NoError(t, err)

	for k, v := range envs {
		t.Setenv(k, v)
	}

	t.Setenv("JOBS_DB_PORT", strconv.Itoa(compose.Port("postgres", 5432)))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(compose.Port("postgres", 5432)))
	t.Setenv("DEST_TRANSFORM_URL", fmt.Sprintf("http://localhost:%d", compose.Port("transformer", 9090)))
	t.Setenv("DEPLOYMENT_TYPE", string(deployment.DedicatedType))
	t.Setenv("MINIO_ENDPOINT", fmt.Sprintf("minio:%d", compose.Port("minio", 9000)))

	httpPortInt, err := testhelper.GetFreePort()
	require.NoError(t, err)

	httpPort := strconv.Itoa(httpPortInt)
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", httpPort)
	httpAdminPort, err := testhelper.GetFreePort()
	require.NoError(t, err)

	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_ENABLE_STATS", "false")

	if testing.Verbose() {
		data, err := os.ReadFile(workspaceConfigPath)
		require.NoError(t, err)
		t.Logf("Workspace config: %s", string(data))
	}

	t.Log("workspace config path:", workspaceConfigPath)
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", workspaceConfigPath)

	t.Setenv("RUDDER_TMPDIR", t.TempDir())

	t.Logf("--- Setup done (%s)", time.Since(setupStart))

	svcDone := make(chan struct{})
	go func() {
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: os.Getenv("ENTERPRISE_TOKEN")})
		_ = r.Run(ctx, []string{"docker-test-rudder-server"})
		close(svcDone)
	}()

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%s/health", httpPort)
	t.Log("serviceHealthEndpoint", serviceHealthEndpoint)
	health.WaitUntilReady(
		ctx, t,
		serviceHealthEndpoint,
		time.Minute,
		time.Second,
		"serviceHealthEndpoint",
	)

	return &testServer{
		URL: fmt.Sprintf("http://localhost:%s", httpPort),
	}
}
