package multitenant

import (
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var workspaceID1 = uuid.Must(uuid.NewV4()).String()
var workspaceID2 = uuid.Must(uuid.NewV4()).String()
var destType1 = "GA"

var _ = Describe("tenantStats", func() {

	Context("cache testing", func() {
		mockCtrl := gomock.NewController(GinkgoT())
		mockRouterJobsDB := mocksJobsDB.NewMockMultiTenantJobsDB(mockCtrl)
		var tenantStats *MultitenantStatsT
		BeforeEach(func() {
			// crash recovery check
			mockRouterJobsDB.EXPECT().GetPileUpCounts(gomock.Any()).Times(1)
			tenantStats = NewStats(mockRouterJobsDB)
		})

		It("TenantStats init", func() {

			Expect(len(tenantStats.RouterInputRates)).To(Equal(2))
			Expect(len(tenantStats.RouterInMemoryJobCounts)).To(Equal(2))
			Expect(len(tenantStats.RouterSuccessRatioLoopCount)).To(Equal(0))
			Expect(len(tenantStats.lastDrainedTimestamps)).To(Equal(0))
			Expect(len(tenantStats.failureRate)).To(Equal(0))
			Expect(len(tenantStats.RouterCircuitBreakerMap)).To(Equal(0))
		})

		It("Calculate Success Failure Counts , Failure Rate", func() {
			for i := 0; i < int(misc.AVG_METRIC_AGE); i++ {
				tenantStats.CalculateSuccessFailureCounts(workspaceID1, destType1, true, false)
				tenantStats.CalculateSuccessFailureCounts(workspaceID2, destType1, false, false)
			}

			Expect(tenantStats.failureRate[workspaceID1][destType1].Value()).To(Equal(0.0))
			Expect(tenantStats.failureRate[workspaceID2][destType1].Value()).To(Equal(1.0))
			Expect(tenantStats.RouterSuccessRatioLoopCount[workspaceID1][destType1]["success"]).To(Equal(int(misc.AVG_METRIC_AGE)))
			Expect(tenantStats.RouterSuccessRatioLoopCount[workspaceID2][destType1]["failure"]).To(Equal(int(misc.AVG_METRIC_AGE)))
			Expect(tenantStats.getFailureRate(workspaceID2, destType1)).To(Equal(1.0))
			Expect(tenantStats.getFailureRate(workspaceID1, destType1)).To(Equal(0.0))
		})

		It("Calculate Success Failure Counts , Drain Map Check", func() {
			tenantStats.CalculateSuccessFailureCounts(workspaceID1, destType1, false, true)

			Expect(tenantStats.failureRate[workspaceID1][destType1].Value()).To(Equal(0.0))
			Expect(tenantStats.RouterSuccessRatioLoopCount[workspaceID1][destType1]["drained"]).To(Equal(1))
			Expect(tenantStats.lastDrainedTimestamps[workspaceID1][destType1]).To(BeTemporally("~", time.Now(), time.Second))
			Expect(tenantStats.getFailureRate(workspaceID1, destType1)).To(Equal(0.0))
		})

		// It("Cache timeout", func() {
		// 	c := Cache{
		// 		KeyTTL:      10 * time.Millisecond,
		// 		CleanupFreq: 10 * time.Millisecond,
		// 	}
		// 	c.Update(testKey, testValue)
		// 	Expect(len(c.cacheMap)).To(Equal(1))
		// 	Expect(len(c.cacheMap[testKey].data)).To(Equal(1))
		// 	Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue))
		// 	Eventually(func() int { return len(c.cacheMap) }).Should(Equal(0))
		// })

		// It("Cache readAndPopData", func() {
		// 	var c Cache
		// 	c.Update(testKey, testValue)
		// 	v := c.ReadAndPopData(testKey)
		// 	Expect(v).To(Equal([][]byte{testValue}))
		// 	Expect(len(c.cacheMap)).To(Equal(0))
		// })

		// It("Cache data store limit", func() {
		// 	c := Cache{
		// 		Size: 2,
		// 	}
		// 	testValue2 := []byte("test_value2")
		// 	testValue3 := []byte("test_value3")
		// 	c.Update(testKey, testValue)
		// 	c.Update(testKey, testValue2)
		// 	c.Update(testKey, testValue3)
		// 	Expect(len(c.cacheMap)).To(Equal(1))
		// 	Expect(len(c.cacheMap[testKey].data)).To(Equal(2))
		// 	Expect(c.cacheMap[testKey].data[0]).To(Equal(testValue2))
		// 	Expect(c.cacheMap[testKey].data[1]).To(Equal(testValue3))
		// })

	})
})
