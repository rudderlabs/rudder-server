package migrations_test

import (
	"database/sql"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/tests/helpers"
)

var dbHandle *sql.DB
var gatewayDBPrefix string
var routerDBPrefix string
var dbPollFreqInS int = 1
var gatewayDBCheckBufferInS int = 2

var _ = BeforeSuite(func() {
	var err error
	psqlInfo := jobsdb.GetConnectionString()
	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	gatewayDBPrefix = config.GetString("Gateway.CustomVal", "GW")
	routerDBPrefix = config.GetString("Router.CustomVal", "RT")
})

var _ = Describe("E2E", func() {
	Context("Validate DB migrations", func() {
		It("should verify that new db table is created after maxDSSize after mainCheckSleepDurationInS", func() {
			initialTableNames := helpers.GetTableNamesWithPrefix(dbHandle, strings.ToLower(gatewayDBPrefix)+"_jobs_")
			for i := 1; i <= config.GetInt("JobsDB.maxDSSize", 10)+1; i++ {
				helpers.SendEventRequest(helpers.EventOptsT{})
			}
			Eventually(func() int {
				return len(helpers.GetTableNamesWithPrefix(dbHandle, strings.ToLower(gatewayDBPrefix)+"_jobs_"))
			}, config.GetInt("JobsDB.mainCheckSleepDurationInS", 2)+1, dbPollFreqInS).Should(Equal(len(initialTableNames) + 1))
		})

		It("should verify that new db table is created after maxDSSize after mainCheckSleepDurationInS", func() {
			initialTableNames := helpers.GetTableNamesWithPrefix(dbHandle, strings.ToLower(gatewayDBPrefix)+"_jobs_")
			for i := 1; i <= config.GetInt("JobsDB.maxDSSize", 10)+1; i++ {
				helpers.SendEventRequest(helpers.EventOptsT{})
			}
			Eventually(func() int {
				return len(helpers.GetTableNamesWithPrefix(dbHandle, strings.ToLower(gatewayDBPrefix)+"_jobs_"))
			}, config.GetInt("JobsDB.backupCheckSleepDurationIns", 5)+1, dbPollFreqInS).Should(Equal(len(initialTableNames)))
			Eventually(func() []string {
				return helpers.GetTableNamesWithPrefix(dbHandle, strings.ToLower(gatewayDBPrefix)+"_jobs_")
			}, config.GetInt("JobsDB.backupCheckSleepDurationIns", 5)+1, dbPollFreqInS).Should(BeEquivalentTo(initialTableNames))
		})
	})
})
