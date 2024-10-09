package testhelper

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
)

func BootstrapSvc(t *testing.T, workspaceConfig backendconfig.ConfigT, httpPort, jobsDBPort int) {
	bcServer := backendconfigtest.
		NewBuilder().
		WithWorkspaceConfig(workspaceConfig).
		Build()
	t.Cleanup(func() {
		bcServer.Close()
	})

	enhanceWithDefaultEnvs(t)

	t.Setenv("JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", strconv.Itoa(jobsDBPort))
	t.Setenv("RSERVER_WAREHOUSE_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("WORKSPACE_TOKEN", "token")
	t.Setenv("CONFIG_BACKEND_URL", bcServer.URL)

	svcDone := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		r := runner.New(runner.ReleaseInfo{EnterpriseToken: "TOKEN"})
		_ = r.Run(ctx, []string{"integration-test"})
		close(svcDone)
	}()

	t.Cleanup(func() { <-svcDone })
	t.Cleanup(cancel)

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t,
		serviceHealthEndpoint, time.Minute, time.Second, "serviceHealthEndpoint",
	)
}

func enhanceWithDefaultEnvs(t testing.TB) {
	t.Setenv("JOBS_DB_HOST", jobsDBHost)
	t.Setenv("JOBS_DB_NAME", jobsDBDatabase)
	t.Setenv("JOBS_DB_DB_NAME", jobsDBDatabase)
	t.Setenv("JOBS_DB_USER", jobsDBUser)
	t.Setenv("JOBS_DB_PASSWORD", jobsDBPassword)
	t.Setenv("JOBS_DB_SSL_MODE", "disable")
	t.Setenv("WAREHOUSE_JOBS_DB_HOST", jobsDBHost)
	t.Setenv("WAREHOUSE_JOBS_DB_NAME", jobsDBDatabase)
	t.Setenv("WAREHOUSE_JOBS_DB_DB_NAME", jobsDBDatabase)
	t.Setenv("WAREHOUSE_JOBS_DB_USER", jobsDBUser)
	t.Setenv("WAREHOUSE_JOBS_DB_PASSWORD", jobsDBPassword)
	t.Setenv("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	t.Setenv("GO_ENV", "production")
	t.Setenv("LOG_LEVEL", "INFO")
	t.Setenv("INSTANCE_ID", "1")
	t.Setenv("ALERT_PROVIDER", "pagerduty")
	t.Setenv("CONFIG_PATH", "../../../config/config.yaml")
	t.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	t.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_JITTER_FOR_SYNCS", "false")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_IDRESOLUTION", "true")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "false")
	t.Setenv("RSERVER_ADMIN_SERVER_ENABLED", "false")
	t.Setenv("RUDDER_ADMIN_PASSWORD", "password")
	t.Setenv("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", "false")
	t.Setenv("RSERVER_LOGGER_CONSOLE_JSON_FORMAT", "true")
	t.Setenv("RSERVER_WAREHOUSE_MODE", "master_and_slave")
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RUDDER_TMPDIR", t.TempDir())
	if testing.Verbose() {
		t.Setenv("LOG_LEVEL", "DEBUG")
	}
}
