package apphandlers

import (
	"context"
	"net/http"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/stretchr/testify/require"
)

func TestAppHandlerStartSequence(t *testing.T) {
	options := app.LoadOptions([]string{"app"})
	application := app.New(options)
	versionHandler := func(w http.ResponseWriter, _ *http.Request) {}

	suite := func(t *testing.T, appHandler AppHandler) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		t.Run("it shouldn't be able to start without setup being called first", func(t *testing.T) {
			require.Error(t, appHandler.StartRudderCore(ctx, options))
		})

		t.Run("it shouldn't be able to setup if database is down", func(t *testing.T) {
			require.Error(t, appHandler.Setup(options))
		})

		t.Run("it should be able to setup if database is up", func(t *testing.T) {
			startJobsDBPostgresql(t)
			require.NoError(t, appHandler.Setup(options))
		})
	}

	t.Run("embedded", func(t *testing.T) {
		h, err := GetAppHandler(application, app.EMBEDDED, versionHandler)
		require.NoError(t, err)
		suite(t, h)
	})

	t.Run("gateway", func(t *testing.T) {
		h, err := GetAppHandler(application, app.GATEWAY, versionHandler)
		require.NoError(t, err)
		suite(t, h)
	})

	t.Run("processor", func(t *testing.T) {
		h, err := GetAppHandler(application, app.PROCESSOR, versionHandler)
		require.NoError(t, err)
		suite(t, h)
	})
}

func startJobsDBPostgresql(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	r, err := destination.SetupPostgres(pool, t)
	require.NoError(t, err)
	config.Set("DB.port", r.Port)
	config.Set("DB.user", r.User)
	config.Set("DB.name", r.Database)
	config.Set("DB.password", r.Password)
}

func init() {
	db.Init()
}
