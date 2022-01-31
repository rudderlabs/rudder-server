package multitenant

import (
	"math/rand"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	destType1         = "GA"
	noOfWorkers       = 64
	routerTimeOut     = 10 * time.Second
	jobQueryBatchSize = 10000
	timeGained        = 0.0
	procLoopTime      = 200 * time.Millisecond
)

var (
	workspaceID1 = uuid.Must(uuid.NewV4()).String()
	workspaceID2 = uuid.Must(uuid.NewV4()).String()
	workspaceID3 = uuid.Must(uuid.NewV4()).String()
)

var _ = Describe("tenantStats", func() {

	BeforeEach(func() {
		config.Load()
		logger.Init()
		Init()
	})

	Context("Tenant Stats Testing", func() {
		mockCtrl := gomock.NewController(GinkgoT())
		mockRouterJobsDB := mocksJobsDB.NewMockMultiTenantJobsDB(mockCtrl)
		var tenantStats *MultitenantStatsT
		BeforeEach(func() {
			// crash recovery check
			mockRouterJobsDB.EXPECT().GetPileUpCounts(gomock.Any()).Times(1)
			tenantStats = NewStats(mockRouterJobsDB)
		})

		It("TenantStats init", func() {

			Expect(len(tenantStats.routerInputRates)).To(Equal(2))
			Expect(len(tenantStats.routerNonTerminalCounts)).To(Equal(2))
			Expect(len(tenantStats.routerSuccessRatioLoopCount)).To(Equal(0))
			Expect(len(tenantStats.lastDrainedTimestamps)).To(Equal(0))
			Expect(len(tenantStats.failureRate)).To(Equal(0))
		})

		It("Calculate Success Failure Counts , Failure Rate", func() {
			for i := 0; i < int(misc.AVG_METRIC_AGE); i++ {
				tenantStats.CalculateSuccessFailureCounts(workspaceID1, destType1, true, false)
				tenantStats.CalculateSuccessFailureCounts(workspaceID2, destType1, false, false)
			}

			Expect(tenantStats.failureRate[workspaceID1][destType1].Value()).To(Equal(0.0))
			Expect(tenantStats.failureRate[workspaceID2][destType1].Value()).To(Equal(1.0))
			Expect(tenantStats.routerSuccessRatioLoopCount[workspaceID1][destType1]["success"]).To(Equal(int(misc.AVG_METRIC_AGE)))
			Expect(tenantStats.routerSuccessRatioLoopCount[workspaceID2][destType1]["failure"]).To(Equal(int(misc.AVG_METRIC_AGE)))
			Expect(tenantStats.getFailureRate(workspaceID2, destType1)).To(Equal(1.0))
			Expect(tenantStats.getFailureRate(workspaceID1, destType1)).To(Equal(0.0))
		})

		It("Calculate Success Failure Counts , Drain Map Check", func() {
			tenantStats.CalculateSuccessFailureCounts(workspaceID1, destType1, false, true)

			Expect(tenantStats.failureRate[workspaceID1][destType1].Value()).To(Equal(0.0))
			Expect(tenantStats.routerSuccessRatioLoopCount[workspaceID1][destType1]["drained"]).To(Equal(1))
			Expect(tenantStats.lastDrainedTimestamps[workspaceID1][destType1]).To(BeTemporally("~", time.Now(), time.Second))
			Expect(tenantStats.getLastDrainedTimestamp(workspaceID1, destType1)).To(BeTemporally("~", time.Now(), time.Second))
			Expect(tenantStats.getFailureRate(workspaceID1, destType1)).To(Equal(0.0))
		})

		It("Generate Success Rate Map", func() {
			for i := 0; i < int(misc.AVG_METRIC_AGE); i++ {
				tenantStats.CalculateSuccessFailureCounts(workspaceID1, destType1, true, false)
				tenantStats.CalculateSuccessFailureCounts(workspaceID2, destType1, false, false)
			}

			for i := 0; i < int(misc.AVG_METRIC_AGE)/2; i++ {
				tenantStats.CalculateSuccessFailureCounts(workspaceID1, destType1, false, false)
				tenantStats.CalculateSuccessFailureCounts(workspaceID2, destType1, true, false)
			}
			customerSuccessRate, customerDrainedMap := tenantStats.GenerateSuccessRateMap(destType1)
			Expect(customerSuccessRate[workspaceID1]).To(Equal(0.6666666666666666))
			Expect(customerSuccessRate[workspaceID2]).To(Equal(0.3333333333333333))
			Expect(customerDrainedMap[workspaceID2]).To(Equal(0.0))
			Expect(customerDrainedMap[workspaceID2]).To(Equal(0.0))

		})

		It("Add and Remove from InMemory Counts", func() {
			addJobWID1 := rand.Intn(10)
			addJobWID2 := rand.Intn(10)
			removeJobWID1 := rand.Intn(10)
			removeJobWID2 := rand.Intn(10)
			for i := 0; i < addJobWID1; i++ {
				tenantStats.AddToInMemoryCount(workspaceID1, destType1, 1, "router")
			}
			for i := 0; i < removeJobWID2; i++ {
				tenantStats.RemoveFromInMemoryCount(workspaceID2, destType1, 1, "router")
			}
			for i := 0; i < addJobWID2; i++ {
				tenantStats.AddToInMemoryCount(workspaceID2, destType1, 1, "router")
			}
			for i := 0; i < removeJobWID1; i++ {
				tenantStats.RemoveFromInMemoryCount(workspaceID1, destType1, 1, "router")
			}

			netCountWID1 := addJobWID1 - removeJobWID1
			netCountWID2 := addJobWID2 - removeJobWID2
			Expect(tenantStats.routerNonTerminalCounts["router"][workspaceID1][destType1]).To(Equal(netCountWID1))
			Expect(tenantStats.routerNonTerminalCounts["router"][workspaceID2][destType1]).To(Equal(netCountWID2))
		})

		It("Add and Remove from InMemory Counts", func() {
			addJobWID1 := rand.Intn(10)
			addJobWID2 := rand.Intn(10)
			input := make(map[string]map[string]int)
			input[workspaceID1] = make(map[string]int)
			input[workspaceID2] = make(map[string]int)
			input[workspaceID1][destType1] = addJobWID1
			input[workspaceID2][destType1] = addJobWID2

			tenantStats.ReportProcLoopAddStats(input, time.Second, "router")
			Expect(tenantStats.routerNonTerminalCounts["router"][workspaceID1][destType1]).To(Equal(addJobWID1))
			Expect(tenantStats.routerNonTerminalCounts["router"][workspaceID2][destType1]).To(Equal(addJobWID2))
			Expect(tenantStats.routerInputRates["router"][workspaceID1][destType1].Value()).To(Equal(float64(addJobWID1)))
			Expect(tenantStats.routerInputRates["router"][workspaceID2][destType1].Value()).To(Equal(float64(addJobWID2)))
		})

		It("Should Correctly Calculate the Router PickUp Jobs", func() {
			addJobWID1 := rand.Intn(2000)
			addJobWID2 := rand.Intn(2000)
			addJobWID3 := rand.Intn(2000)
			input := make(map[string]map[string]int)
			input[workspaceID1] = make(map[string]int)
			input[workspaceID2] = make(map[string]int)
			input[workspaceID3] = make(map[string]int)
			input[workspaceID1][destType1] = addJobWID1
			input[workspaceID2][destType1] = addJobWID2
			input[workspaceID3][destType1] = addJobWID3
			latencyMap := map[string]misc.MovingAverage{workspaceID1: misc.NewMovingAverage(), workspaceID2: misc.NewMovingAverage(), workspaceID3: misc.NewMovingAverage()}
			tenantStats.ReportProcLoopAddStats(input, procLoopTime, "router")
			routerPickUpJobs, usedLatencies := tenantStats.GetRouterPickupJobs(destType1, noOfWorkers, routerTimeOut, latencyMap, jobQueryBatchSize, timeGained)
			Expect(routerPickUpJobs[workspaceID1]).To(Equal(addJobWID1))
			Expect(routerPickUpJobs[workspaceID2]).To(Equal(addJobWID2))
			Expect(routerPickUpJobs[workspaceID3]).To(Equal(addJobWID3))
			Expect(usedLatencies[workspaceID1]).To(Equal(0.0))
			Expect(usedLatencies[workspaceID2]).To(Equal(0.0))
			Expect(usedLatencies[workspaceID3]).To(Equal(0.0))
		})
	})
})
