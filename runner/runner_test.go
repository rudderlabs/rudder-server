package runner

import (
	"context"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
)

func TestRunner(t *testing.T) {
	config.Set("BackendConfig.configFromFile", true)
	configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/config.json", map[string]any{})
	config.Set("BackendConfig.configJSONPath", configJsonPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("embedded", func(t *testing.T) {
		config.Set("APP_TYPE", app.EMBEDDED)
		config.Set("Warehouse.mode", "embedded")

		t.Run("it shouldn't be able to start if both databases are down", func(t *testing.T) {
			exitCode := New(ReleaseInfo{}).Run(ctx, []string{"app"})
			require.Equal(t, 1, exitCode)
		})

		t.Run("it shouldn't be able to start if jobsdb database is down", func(t *testing.T) {
			startWarehousePostgresql(t)
			exitCode := New(ReleaseInfo{}).Run(ctx, []string{"app"})
			require.Equal(t, 1, exitCode)
		})

		t.Run("it shouldn't be able to start if warehouse database is down", func(t *testing.T) {
			startJobsDBPostgresql(t)
			exitCode := New(ReleaseInfo{}).Run(ctx, []string{"app"})
			require.Equal(t, 1, exitCode)
		})
	})
}

func startJobsDBPostgresql(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	r, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	config.Set("DB.host", r.Host)
	config.Set("DB.port", r.Port)
	config.Set("DB.user", r.User)
	config.Set("DB.name", r.Database)
	config.Set("DB.password", r.Password)
	// setting warehouse config values to their defaults
	config.Set("WAREHOUSE_JOBS_DB_HOST", "localhost")
	config.Set("WAREHOUSE_JOBS_DB_PORT", 5432)
	config.Set("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	config.Set("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	config.Set("WAREHOUSE_JOBS_DB_PASSWORD", r.Password)
}

func startWarehousePostgresql(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	r, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	config.Set("WAREHOUSE_JOBS_DB_HOST", r.Host)
	config.Set("WAREHOUSE_JOBS_DB_PORT", r.Port)
	config.Set("WAREHOUSE_JOBS_DB_USER", r.User)
	config.Set("WAREHOUSE_JOBS_DB_DB_NAME", r.Database)
	config.Set("WAREHOUSE_JOBS_DB_PASSWORD", r.Password)
}
