package warehouse

import (
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type testingT interface { // TODO replace with testing.TB
	Setenv(key, value string)
	Cleanup(func())
	Log(...any)
	Errorf(format string, args ...interface{})
	FailNow()
}

func setupWarehouseJobsDB(pool *dockertest.Pool, t testingT) *resource.PostgresResource {
	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	t.Setenv("WAREHOUSE_JOBS_DB_HOST", pgResource.Host)
	t.Setenv("WAREHOUSE_JOBS_DB_USER", pgResource.User)
	t.Setenv("WAREHOUSE_JOBS_DB_PASSWORD", pgResource.Password)
	t.Setenv("WAREHOUSE_JOBS_DB_DB_NAME", pgResource.Database)
	t.Setenv("WAREHOUSE_JOBS_DB_PORT", pgResource.Port)

	return pgResource
}

func initWarehouse() {
	config.Reset()
	admin.Init()
	logger.Reset()
	Init4()
	validations.Init()
	misc.Init()
}
