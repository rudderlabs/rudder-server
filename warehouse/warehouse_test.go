package warehouse

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type testingT interface {
	Setenv(key, value string)
}

func setupWarehouseJobs(pool *dockertest.Pool, es testingT, cleanup *testhelper.Cleanup) *destination.PostgresResource {
	pgResource, err := destination.SetupPostgres(pool, cleanup)
	Expect(err).To(BeNil())

	es.Setenv("WAREHOUSE_JOBS_DB_HOST", pgResource.Host)
	es.Setenv("WAREHOUSE_JOBS_DB_USER", pgResource.User)
	es.Setenv("WAREHOUSE_JOBS_DB_PASSWORD", pgResource.Password)
	es.Setenv("WAREHOUSE_JOBS_DB_DB_NAME", pgResource.Database)
	es.Setenv("WAREHOUSE_JOBS_DB_PORT", pgResource.Port)

	return pgResource
}

func initWarehouse() {
	config.Load()
	admin.Init()
	logger.Init()
	Init()
	Init2()
	Init3()
	Init4()
	Init5()
	stats.Setup()
}

var _ = Describe("Warehouse", func() {
})
