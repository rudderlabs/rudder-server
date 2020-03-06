package maintenanceMode_test

import (
	"database/sql"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/tests/helpers"
)

var (
	dbHandle            *sql.DB
	dbname              string
	gatewayDBPrefix     string
	routerDBPrefix      string
	batchRouterDBPrefix string
)
var dbPollFreqInS int = 1
var gatewayDBCheckBufferInS int = 2
var routerDBCheckBufferInS int = 2
var batchRouterDBCheckBufferInS int = 2

var _ = BeforeSuite(func() {
	var err error
	psqlInfo := jobsdb.GetConnectionString()
	fmt.Println("psqlInfo", psqlInfo)
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	gatewayDBPrefix = config.GetString("Gateway.CustomVal", "GW")
	routerDBPrefix = config.GetString("Router.CustomVal", "RT")
	batchRouterDBPrefix = config.GetString("BatchRouter.CustomVal", "BRT")
	dbname = config.GetEnv("JOBS_DB_DB_NAME", "jobsdb")
})

var _ = Describe("Validate maintenance mode", func() {
	It("should take events and put them in gateway db", func() {
		initGatewayJobsCount := helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
		helpers.SendEventRequest(helpers.EventOptsT{
			WriteKey: "1Yc6YbOGg6U2E8rlj97ZdOawPyr",
		})
		Eventually(func() int {
			return helpers.GetJobsCount(dbHandle, gatewayDBPrefix)
		}, gatewayDBCheckBufferInS, dbPollFreqInS).Should(Equal(initGatewayJobsCount + 1))
	})
	It("should not put events in router db", func() {
		initGatewayJobsCount := helpers.GetJobsCount(dbHandle, routerDBPrefix)
		helpers.SendEventRequest(helpers.EventOptsT{
			WriteKey: "1Yc6YbOGg6U2E8rlj97ZdOawPyr",
		})
		Eventually(func() int {
			return helpers.GetJobsCount(dbHandle, routerDBPrefix)
		}, routerDBCheckBufferInS, dbPollFreqInS).Should(Equal(initGatewayJobsCount))
	})
	It("should not put events in batch router db", func() {
		initGatewayJobsCount := helpers.GetJobsCount(dbHandle, batchRouterDBPrefix)
		helpers.SendEventRequest(helpers.EventOptsT{
			WriteKey: "1Yc6YbOGg6U2E8rlj97ZdOawPyr",
		})
		Eventually(func() int {
			return helpers.GetJobsCount(dbHandle, batchRouterDBPrefix)
		}, batchRouterDBCheckBufferInS, dbPollFreqInS).Should(Equal(initGatewayJobsCount))
	})
	It("should backup the original jobsdb", func() {
		storagePath := "./maintenance_recovery_data.json"
		recoveryData := helpers.GetRecoveryData(storagePath)
		appStartTime := recoveryData.MaintenanceModeStartTimes[len(recoveryData.MaintenanceModeStartTimes)-1]

		originalJobsdbs := helpers.GetListOfMaintenanceModeOriginalDBs(dbHandle, dbname, appStartTime)
		Expect(len(originalJobsdbs)).To(Equal(1))
	})
})
