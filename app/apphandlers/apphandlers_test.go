package apphandlers

import (
	"context"
	"net/http"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/db"
)

func TestAppHandlerStartSequence(t *testing.T) {
	options := app.LoadOptions([]string{"app"})
	application := app.New(options)
	versionHandler := func(w http.ResponseWriter, _ *http.Request) {}

	suite := func(t *testing.T, conf *config.Config, appHandler AppHandler) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		t.Run("it shouldn't be able to start without setup being called first", func(t *testing.T) {
			require.Error(t, appHandler.StartRudderCore(ctx, options, backendconfig.DefaultBackendConfig))
		})

		t.Run("it shouldn't be able to setup if database is down", func(t *testing.T) {
			require.Error(t, appHandler.Setup(options))
		})

		t.Run("it should be able to setup if database is up", func(t *testing.T) {
			startJobsDBPostgresql(t, conf)
			require.NoError(t, appHandler.Setup(options))
		})
	}

	t.Run("embedded", func(t *testing.T) {
		conf := config.New()
		h, err := GetAppHandler(conf, application, app.EMBEDDED, versionHandler)
		require.NoError(t, err)
		suite(t, conf, h)
	})

	t.Run("gateway", func(t *testing.T) {
		conf := config.New()
		h, err := GetAppHandler(conf, application, app.GATEWAY, versionHandler)
		require.NoError(t, err)
		suite(t, conf, h)
	})

	t.Run("processor", func(t *testing.T) {
		conf := config.New()
		h, err := GetAppHandler(conf, application, app.PROCESSOR, versionHandler)
		require.NoError(t, err)
		suite(t, conf, h)
	})
}

func startJobsDBPostgresql(t *testing.T, conf *config.Config) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	r, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	conf.Set("DB.port", r.Port)
	conf.Set("DB.user", r.User)
	conf.Set("DB.name", r.Database)
	conf.Set("DB.password", r.Password)
}

func init() {
	db.Init()
}
