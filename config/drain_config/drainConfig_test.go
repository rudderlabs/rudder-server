package drain_config_test

import (
	"context"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/config/drain_config"
)

func TestDrainConfigRoutine(t *testing.T) {
	conf := config.New()
	ctx, cancel := context.WithCancel(context.Background())
	log := logger.NOP

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")
	postgresResource, err := resource.SetupPostgres(pool, t, postgres.WithShmSize(256*bytesize.MB))
	require.NoError(t, err, "failed to setup postgres resource")
	conf.Set("DB.name", postgresResource.Database)
	conf.Set("DB.host", postgresResource.Host)
	conf.Set("DB.port", postgresResource.Port)
	conf.Set("DB.user", postgresResource.User)
	conf.Set("DB.password", postgresResource.Password)
	conf.Set("drainConfig.pollFrequency", "100ms")
	drainConfigManager, err := drain_config.NewDrainConfigManager(conf, log)
	require.NoError(t, err, "failed to create drain config manager")
	close := make(chan struct{})
	go func() {
		_ = drainConfigManager.DrainConfigRoutine(ctx)
		close <- struct{}{}
	}()

	db := postgresResource.DB
	_, err = db.ExecContext(ctx, "INSERT INTO drain_config (key, value) VALUES ('drain.destinationIDs', '1,2,3')")
	require.NoError(t, err, "failed to insert into config table")

	time.Sleep(1 * time.Second)

	require.Equal(t, []string{"1", "2", "3"}, conf.GetStringSlice("drain.destinationIDs", nil))

	conf.Set("drain.age", "1ms")

	time.Sleep(1 * time.Second)

	require.Nil(t, conf.GetStringSlice("drain.destinationIDs", nil))

	conf.Set("Router.toAbortDestinationIDs", "abc, def, ghi")

	time.Sleep(1 * time.Second)

	require.Equal(t, []string{"abc", "def", "ghi"}, conf.GetStringSlice("drain.destinationIDs", nil))
	cancel()
	<-close
}
