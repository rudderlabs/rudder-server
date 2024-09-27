package drain_config_test

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	drain_config "github.com/rudderlabs/rudder-server/internal/drain-config"
)

func TestDrainConfigRoutine(t *testing.T) {
	conf, db := testSetup(t)
	conf.Set("drainConfig.pollFrequency", "100ms")
	conf.Set("drainConfig.cleanupFrequency", "50ms")
	ctx := context.Background()
	log := logger.NOP
	sts, err := memstats.New()
	require.NoError(t, err, "should create stats")

	drainConfigManager, err := drain_config.NewDrainConfigManager(conf, log, sts)
	require.NoError(t, err, "should create drain config manager")
	go func() {
		require.NoError(t, drainConfigManager.DrainConfigRoutine(ctx), "drain config routine should exit")
	}()
	go func() {
		require.NoError(t, drainConfigManager.CleanupRoutine(ctx), "cleanup routine should exit")
	}()

	_, err = db.ExecContext(ctx, "INSERT INTO drain_config (key, value) VALUES ('drain.jobRunIDs', '123')")
	require.NoError(t, err, "should insert into config table")
	require.Eventually(t, func() bool {
		return slices.Equal([]string{"123"}, conf.GetStringSlice("drain.jobRunIDs", nil))
	}, 1*time.Second, 100*time.Millisecond, "should read from drain config table")

	conf.Set("drain.age", "1ms")
	require.Eventually(t, func() bool {
		return slices.Equal([]string{}, conf.GetStringSlice("drain.jobRunIDs", nil))
	}, 1*time.Second, 100*time.Millisecond, "should read from drain config table")
	require.Nil(t, conf.GetStringSlice("drain.jobRunIDs", nil), "should've cleaned up")

	drainConfigManager.Stop()
}

func TestDrainConfigHttpHandler(t *testing.T) {
	conf, _ := testSetup(t)
	conf.Set("drainConfig.pollFrequency", "100ms")
	ctx, cancel := context.WithCancel(context.Background())
	log := logger.NOP
	sts, err := memstats.New()
	require.NoError(t, err, "should create stats")

	drainConfigManager, err := drain_config.NewDrainConfigManager(conf, log, sts)
	require.NoError(t, err, "should create drain config manager")
	go func() {
		require.NoError(t, drainConfigManager.DrainConfigRoutine(ctx), "drain config routine should exit")
	}()
	go func() {
		require.NoError(t, drainConfigManager.CleanupRoutine(ctx), "cleanup routine should exit")
	}()

	req, err := http.NewRequest("PUT", "http://localhost:8080/job/randomJobRunID", http.NoBody)
	require.NoError(t, err, "create http request")
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	drainConfigManager.DrainConfigHttpHandler().ServeHTTP(resp, req)
	require.Equal(t, http.StatusCreated, resp.Code, "expected status code to be 201")
	require.Eventually(t, func() bool {
		return slices.Equal([]string{"randomJobRunID"}, conf.GetStringSlice("drain.jobRunIDs", nil))
	}, 1*time.Second, 100*time.Millisecond, "should read from drain config table", conf.GetStringSlice("drain.jobRunIDs", nil))

	cancel()
	drainConfigManager.Stop()
}

func testSetup(t *testing.T) (*config.Config, *sql.DB) {
	conf := config.New()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")
	postgresResource, err := postgres.Setup(pool, t, postgres.WithShmSize(256*bytesize.MB))
	require.NoError(t, err, "failed to setup postgres resource")
	conf.Set("DB.name", postgresResource.Database)
	conf.Set("DB.host", postgresResource.Host)
	conf.Set("DB.port", postgresResource.Port)
	conf.Set("DB.user", postgresResource.User)
	conf.Set("DB.password", postgresResource.Password)

	return conf, postgresResource.DB
}
