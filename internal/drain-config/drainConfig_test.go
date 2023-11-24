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
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	drain_config "github.com/rudderlabs/rudder-server/internal/drain-config"
)

func TestDrainConfigRoutine(t *testing.T) {
	conf, db := testSetup(t)
	conf.Set("drainConfig.pollFrequency", "100ms")
	conf.Set("drainConfig.cleanupFrequency", "50ms")
	ctx, cancel := context.WithCancel(context.Background())
	log := logger.NOP

	drainConfigManager, err := drain_config.NewDrainConfigManager(conf, log)
	require.NoError(t, err, "failed to create drain config manager")
	closeChan := make(chan struct{})
	go func() {
		_ = drainConfigManager.DrainConfigRoutine(ctx)
		closeChan <- struct{}{}
	}()
	go func() {
		_ = drainConfigManager.CleanupRoutine(ctx)
		closeChan <- struct{}{}
	}()

	_, err = db.ExecContext(ctx, "INSERT INTO drain_config (key, value) VALUES ('drain.jobRunIDs', '1,2,3')")
	require.NoError(t, err, "failed to insert into config table")
	require.Eventually(t, func() bool {
		return slices.Equal([]string{"1", "2", "3"}, conf.GetStringSlice("drain.jobRunIDs", nil))
	}, 1*time.Second, 100*time.Millisecond, "failed to read from drain config table")

	conf.Set("drain.age", "1ms")
	require.Eventually(t, func() bool {
		return slices.Equal(nil, conf.GetStringSlice("drain.jobRunIDs", nil))
	}, 1*time.Second, 100*time.Millisecond, "failed to read from drain config table")
	require.Nil(t, conf.GetStringSlice("drain.jobRunIDs", nil))

	conf.Set("RSources.toAbortJobRunIDs", "abc def ghi")
	require.Eventually(t, func() bool {
		return slices.Equal([]string{"abc", "def", "ghi"}, conf.GetStringSlice("drain.jobRunIDs", nil))
	}, 1*time.Second, 100*time.Millisecond, "failed to read from drain config table")

	cancel()
	<-closeChan
	<-closeChan
}

func TestDrainConfigHttpHandler(t *testing.T) {
	conf, _ := testSetup(t)
	conf.Set("drainConfig.pollFrequency", "100ms")
	ctx, cancel := context.WithCancel(context.Background())
	log := logger.NOP

	drainConfigManager, err := drain_config.NewDrainConfigManager(conf, log)
	require.NoError(t, err, "failed to create drain config manager")
	closeChan := make(chan struct{})
	go func() {
		_ = drainConfigManager.DrainConfigRoutine(ctx)
		closeChan <- struct{}{}
	}()
	go func() {
		_ = drainConfigManager.CleanupRoutine(ctx)
		closeChan <- struct{}{}
	}()

	req, err := http.NewRequest("GET", "http://localhost:8080/set?job_run_id=randomJobRunID", http.NoBody)
	require.NoError(t, err, "create http request")
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	drainConfigManager.DrainConfigHttpHandler().ServeHTTP(resp, req)
	require.Equal(t, http.StatusCreated, resp.Code, "required error expected to be 201")
	require.Eventually(t, func() bool {
		return slices.Equal([]string{"randomJobRunID"}, conf.GetStringSlice("drain.jobRunIDs", nil))
	}, 1*time.Second, 100*time.Millisecond, "failed to read from drain config table")
	cancel()
	<-closeChan
	<-closeChan
}

func testSetup(t *testing.T) (*config.Config, *sql.DB) {
	conf := config.New()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")
	postgresResource, err := resource.SetupPostgres(pool, t, postgres.WithShmSize(256*bytesize.MB))
	require.NoError(t, err, "failed to setup postgres resource")
	conf.Set("DB.name", postgresResource.Database)
	conf.Set("DB.host", postgresResource.Host)
	conf.Set("DB.port", postgresResource.Port)
	conf.Set("DB.user", postgresResource.User)
	conf.Set("DB.password", postgresResource.Password)

	return conf, postgresResource.DB
}
