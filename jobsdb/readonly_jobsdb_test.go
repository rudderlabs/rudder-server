package jobsdb

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

func initReadonlyJobsDB() {
	config.Load()
	logger.Init()
	admin.Init()
	Init()
	Init2()
	Init3()
}

var _ = Describe("readonly_jobsdb", func() {
	initReadonlyJobsDB()

	BeforeEach(func() {
		stats.Setup()
	})

	Context("getJobPrefix", func() {
		It("should return prefix for job tables", func() {
			Expect(getJobPrefix("gw")).To(Equal("gw_jobs_"))
			Expect(getJobPrefix("gw_jobs_")).To(Equal("gw_jobs_"))
			Expect(getJobPrefix("proc_error_jobs_")).To(Equal("proc_error_jobs_"))
			Expect(getJobPrefix("proc_error")).To(Equal("proc_error_jobs_"))
			Expect(getJobPrefix("rt")).To(Equal("rt_jobs_"))
			Expect(getJobPrefix("brt")).To(Equal("batch_rt_jobs_"))
			Expect(getJobPrefix("batch_rt")).To(Equal("batch_rt_jobs_"))
		})
	})

	Context("getStatusPrefix", func() {
		It("should return prefix for job status tables", func() {
			Expect(getStatusPrefix("gw")).To(Equal("gw_job_status_"))
			Expect(getStatusPrefix("gw_jobs_")).To(Equal("gw_job_status_"))
			Expect(getStatusPrefix("proc_error_jobs_")).To(Equal("proc_error_job_status_"))
			Expect(getStatusPrefix("proc_error")).To(Equal("proc_error_job_status_"))
			Expect(getStatusPrefix("rt")).To(Equal("rt_job_status_"))
			Expect(getStatusPrefix("brt")).To(Equal("batch_rt_job_status_"))
			Expect(getStatusPrefix("batch_rt")).To(Equal("batch_rt_job_status_"))
		})
	})
})
