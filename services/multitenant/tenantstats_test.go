package multitenant

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
)

const (
	destType1                = "GA"
	noOfWorkers              = 64
	routerTimeOut            = 10 * time.Second
	jobQueryBatchSize        = 10000
	float64EqualityThreshold = 0.0001
)

var (
	workspaceID1 = uuid.New().String()
	workspaceID2 = uuid.New().String()
	workspaceID3 = uuid.New().String()
)

var _ = Describe("tenantStats", func() {
	BeforeEach(func() {
		metric.Instance.Reset()
		config.Reset()
		logger.Reset()
		Init()
	})

	Context(
		"Tenant Stats Testing",
		func() {
			var tenantStats *Stats
			BeforeEach(func() {
				mockCtrl := gomock.NewController(GinkgoT())
				mockRouterJobsDB := mocksJobsDB.NewMockMultiTenantJobsDB(mockCtrl)
				// crash recovery check
				mockRouterJobsDB.EXPECT().GetPileUpCounts(gomock.Any()).Times(1)
				tenantStats = NewStats(map[string]jobsdb.MultiTenantJobsDB{"rt": mockRouterJobsDB})
				Expect(tenantStats.Start()).To(BeNil())
			})

			It("TenantStats init", func() {
				Expect(len(tenantStats.routerInputRates)).To(Equal(1))
				Expect(len(tenantStats.lastDrainedTimestamps)).To(Equal(0))
				Expect(len(tenantStats.failureRate)).To(Equal(0))
			})

			It("Calculate Success Failure Counts , Failure Rate", func() {
				for i := 0; i < int(metric.AVG_METRIC_AGE); i++ {
					tenantStats.CalculateSuccessFailureCounts(workspaceID1, destType1, true, false)
					tenantStats.CalculateSuccessFailureCounts(workspaceID2, destType1, false, false)
				}

				Expect(tenantStats.failureRate[workspaceID1][destType1].Value()).To(Equal(0.0))
				Expect(tenantStats.failureRate[workspaceID2][destType1].Value()).To(Equal(1.0))
				Expect(tenantStats.getFailureRate(workspaceID2, destType1)).To(Equal(1.0))
				Expect(tenantStats.getFailureRate(workspaceID1, destType1)).To(Equal(0.0))
			})

			It("Should Update Latency Map", func() {
				for i := 0; i < int(metric.AVG_METRIC_AGE); i++ {
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID1, 1)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID2, 2)
				}

				Expect(tenantStats.routerTenantLatencyStat[destType1][workspaceID1].Value()).To(Equal(1.0))
				Expect(tenantStats.routerTenantLatencyStat[destType1][workspaceID2].Value()).To(Equal(2.0))
			})

			It("Calculate Success Failure Counts , Drain Map Check", func() {
				tenantStats.CalculateSuccessFailureCounts(workspaceID1, destType1, false, true)

				Expect(tenantStats.failureRate[workspaceID1][destType1].Value()).To(Equal(0.0))
				Expect(tenantStats.lastDrainedTimestamps[workspaceID1][destType1]).To(BeTemporally("~", time.Now(), time.Second))
				Expect(tenantStats.getLastDrainedTimestamp(workspaceID1, destType1)).To(BeTemporally("~", time.Now(), time.Second))
				Expect(tenantStats.getFailureRate(workspaceID1, destType1)).To(Equal(0.0))
			})

			It("Add and Remove from InMemory Counts", func() {
				addJobWID1 := 10
				addJobWID2 := 5
				input := make(map[string]map[string]int)
				input[workspaceID1] = make(map[string]int)
				input[workspaceID2] = make(map[string]int)
				input[workspaceID1][destType1] = addJobWID1
				input[workspaceID2][destType1] = addJobWID2

				tenantStats.ReportProcLoopAddStats(input, "rt")

				Expect(rmetrics.PendingEvents("rt", workspaceID1, destType1).IntValue()).To(Equal(addJobWID1))
				Expect(rmetrics.PendingEvents("rt", workspaceID2, destType1).IntValue()).To(Equal(addJobWID2))
			})

			It("Should Correctly Calculate the Router PickUp Jobs", func() {
				addJobWID1 := 2000
				addJobWID2 := 1000
				addJobWID3 := 500
				input := make(map[string]map[string]int)
				input[workspaceID1] = make(map[string]int)
				input[workspaceID2] = make(map[string]int)
				input[workspaceID3] = make(map[string]int)
				input[workspaceID1][destType1] = addJobWID1
				input[workspaceID2][destType1] = addJobWID2
				input[workspaceID3][destType1] = addJobWID3
				tenantStats.ReportProcLoopAddStats(input, "rt")
				tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID1, 0)
				tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID2, 0)
				tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID3, 0)
				routerPickUpJobs := tenantStats.GetRouterPickupJobs(destType1, noOfWorkers, routerTimeOut, jobQueryBatchSize)
				Expect(routerPickUpJobs[workspaceID1]).To(Equal(addJobWID1))
				Expect(routerPickUpJobs[workspaceID2]).To(Equal(addJobWID2))
				Expect(routerPickUpJobs[workspaceID3]).To(Equal(addJobWID3))
			})

			It("Should Pick BETA for slower jobs", func() {
				addJobWID1 := 300
				addJobWID2 := 2000
				addJobWID3 := 1500
				input := make(map[string]map[string]int)
				input[workspaceID1] = make(map[string]int)
				input[workspaceID2] = make(map[string]int)
				input[workspaceID3] = make(map[string]int)
				input[workspaceID1][destType1] = addJobWID1
				input[workspaceID2][destType1] = addJobWID2
				input[workspaceID3][destType1] = addJobWID3
				tenantStats.ReportProcLoopAddStats(input, "rt")
				for i := 0; i < int(metric.AVG_METRIC_AGE); i++ {
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID1, 1)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID2, 2)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID3, 3)
				}
				routerPickUpJobs := tenantStats.GetRouterPickupJobs(destType1, noOfWorkers, routerTimeOut, 300)
				Expect(routerPickUpJobs[workspaceID1]).To(Equal(addJobWID1))
				Expect(routerPickUpJobs[workspaceID2]).To(Equal(1))
				Expect(routerPickUpJobs[workspaceID3]).To(Equal(1))
			})

			It("Should de-prioritise slower jobs for faster jobs", func() {
				addJobWID1 := 10000
				addJobWID2 := 1000
				addJobWID3 := 1000
				input := make(map[string]map[string]int)
				input[workspaceID1] = make(map[string]int)
				input[workspaceID2] = make(map[string]int)
				input[workspaceID3] = make(map[string]int)
				input[workspaceID1][destType1] = addJobWID1
				input[workspaceID2][destType1] = addJobWID2
				input[workspaceID3][destType1] = addJobWID3
				tenantStats.ReportProcLoopAddStats(input, "rt")
				for i := 0; i < int(metric.AVG_METRIC_AGE); i++ {
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID1, 1)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID2, 0.3)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID3, 0.1)
				}
				routerPickUpJobs := tenantStats.GetRouterPickupJobs(destType1, noOfWorkers, routerTimeOut, 10000)
				// 64(numberOfWorkers) * 1.3(boostedRouterTime) * 10(routerTimeout)
				//    				= 432(Jobs For WS1) * 1(Latency of WS1)
				//					+ 1000(Jobs For WS2) * 0.3(Latency of WS2)
				//					+ 1000(Jobs For WS3) * 0.1(Latency of WS3))
				Expect(routerPickUpJobs[workspaceID1]).To(Equal(432))
				Expect(routerPickUpJobs[workspaceID2]).To(Equal(1000))
				Expect(routerPickUpJobs[workspaceID3]).To(Equal(1000))
			})

			It("Should Pick Up Jobs from PileUp based on latency if the in-rate is satisfied", func() {
				addJobWID1 := 1000
				addJobWID2 := 1000
				addJobWID3 := 1000
				input := make(map[string]map[string]int)
				input[workspaceID1] = make(map[string]int)
				input[workspaceID2] = make(map[string]int)
				input[workspaceID3] = make(map[string]int)
				input[workspaceID1][destType1] = addJobWID1
				input[workspaceID2][destType1] = addJobWID2
				input[workspaceID3][destType1] = addJobWID3
				tenantStats.processorStageTime = time.Now().Add(time.Duration(-1) * time.Minute)
				tenantStats.ReportProcLoopAddStats(input, "rt")
				// Explicitly Making the Input Rate Lower to pick from PileUp
				for i := 0; i < int(metric.AVG_METRIC_AGE); i++ {
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID1, 1)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID2, 0.3)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID3, 0.1)
				}
				routerPickUpJobs := tenantStats.GetRouterPickupJobs(destType1, noOfWorkers, routerTimeOut, 10000)
				// Jobs are picked both from Input Loop and Pile Up Loop
				assert.InDelta(GinkgoT(), tenantStats.routerInputRates["rt"][workspaceID1][destType1].Value(), 16.66666, float64EqualityThreshold)
				assert.InDelta(GinkgoT(), tenantStats.routerInputRates["rt"][workspaceID2][destType1].Value(), 16.66666, float64EqualityThreshold)
				assert.InDelta(GinkgoT(), tenantStats.routerInputRates["rt"][workspaceID3][destType1].Value(), 16.66666, float64EqualityThreshold)
				Expect(routerPickUpJobs[workspaceID1]).To(Equal(432))
				Expect(routerPickUpJobs[workspaceID2]).To(Equal(1000))
				Expect(routerPickUpJobs[workspaceID3]).To(Equal(1000))
			})

			It("Customer with high traffic should not block smaller workspaces", func() {
				addJobWID1 := 100000
				addJobWID2 := 1000
				addJobWID3 := 1000
				input := make(map[string]map[string]int)
				input[workspaceID1] = make(map[string]int)
				input[workspaceID2] = make(map[string]int)
				input[workspaceID3] = make(map[string]int)
				input[workspaceID1][destType1] = addJobWID1
				input[workspaceID2][destType1] = addJobWID2
				input[workspaceID3][destType1] = addJobWID3
				tenantStats.ReportProcLoopAddStats(input, "rt")
				for i := 0; i < int(metric.AVG_METRIC_AGE); i++ {
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID1, 0.1)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID2, 0.11)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID3, 0.12)
				}
				routerPickUpJobs := tenantStats.GetRouterPickupJobs(destType1, noOfWorkers, routerTimeOut, 10000)
				// Jobs are picked both from Input Loop
				// The whole time was consumed by workspaceID1
				// So the other two workspaces had the minimum picked up jobs(1)
				Expect(routerPickUpJobs[workspaceID1]).To(Equal(8319))
				Expect(routerPickUpJobs[workspaceID2]).To(Equal(1))
				Expect(routerPickUpJobs[workspaceID3]).To(Equal(1))
			})

			It("Customer with high drain rate should be de-prioritised", func() {
				addJobWID1 := 100000
				addJobWID2 := 1000
				addJobWID3 := 1000
				input := make(map[string]map[string]int)
				input[workspaceID1] = make(map[string]int)
				input[workspaceID2] = make(map[string]int)
				input[workspaceID3] = make(map[string]int)
				tenantStats.lastDrainedTimestamps[workspaceID1] = make(map[string]time.Time)
				input[workspaceID1][destType1] = addJobWID1
				input[workspaceID2][destType1] = addJobWID2
				input[workspaceID3][destType1] = addJobWID3
				tenantStats.ReportProcLoopAddStats(input, "rt")
				tenantStats.lastDrainedTimestamps[workspaceID1][destType1] = time.Now().Add(time.Duration(-1) * time.Minute)
				for i := 0; i < int(metric.AVG_METRIC_AGE); i++ {
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID1, 0.1)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID2, 0.11)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID3, 0.12)
				}
				routerPickUpJobs := tenantStats.GetRouterPickupJobs(destType1, noOfWorkers, routerTimeOut, 10000)
				// Jobs are picked from Input Loop
				// workspaceID1 has the least latency but also the least priority
				// as it has draining jobs within the last minute
				// The algorithm tries to pick the jobs likely to succeed
				// So the other two workspaces had the maximum picked up jobs(1000)
				Expect(routerPickUpJobs[workspaceID1]).To(Equal(6019))
				Expect(routerPickUpJobs[workspaceID2]).To(Equal(1000))
				Expect(routerPickUpJobs[workspaceID3]).To(Equal(1000))
			})

			It("Customer with high failure rate should be de-prioritised", func() {
				addJobWID1 := 1000
				addJobWID2 := 1000
				addJobWID3 := 1000
				input := make(map[string]map[string]int)
				input[workspaceID1] = make(map[string]int)
				input[workspaceID2] = make(map[string]int)
				input[workspaceID3] = make(map[string]int)
				input[workspaceID1][destType1] = addJobWID1
				input[workspaceID2][destType1] = addJobWID2
				input[workspaceID3][destType1] = addJobWID3
				// Explicitly Making the Input Rate Lower to pick from PileUp
				tenantStats.processorStageTime = time.Now().Add(time.Duration(-1) * time.Minute)
				tenantStats.ReportProcLoopAddStats(input, "rt")
				for i := 0; i < int(metric.AVG_METRIC_AGE); i++ {
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID1, 0.4)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID2, 0.41)
					tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID3, 0.42)
					// Making the failure rate for workspaceID1 high
					tenantStats.CalculateSuccessFailureCounts(workspaceID1, destType1, false, false)
				}
				routerPickUpJobs := tenantStats.GetRouterPickupJobs(destType1, noOfWorkers, routerTimeOut, 10000)
				// Jobs are picked from Input Loop
				// Router In Rate was very low so the jobs had to be picked up from PileUp
				// workspaceID1 has the highest failure rate so the least priority in PileUp
				// The algorithm tries to pick the jobs likely to succeed from PileUp
				// So the other two workspaces had the maximum picked up jobs(1000)
				Expect(routerPickUpJobs[workspaceID1]).To(Equal(166))
				Expect(routerPickUpJobs[workspaceID2]).To(Equal(1000))
				Expect(routerPickUpJobs[workspaceID3]).To(Equal(846))
			})
		},
	)
})

func Benchmark_Counts(b *testing.B) {
	b.ResetTimer()
	metric.Instance.Reset()
	const writeRatio = 1000
	gauge := rmetrics.PendingEvents("rt", workspaceID1, destType1)
	errgroup := errgroup.Group{}
	errgroup.Go(func() error {
		for i := 0; i < b.N; i++ {
			gauge.Add(float64(writeRatio + 1))
		}
		return nil
	})
	for i := 0; i < writeRatio; i++ {
		errgroup.Go(func() error {
			for i := 0; i < b.N; i++ {
				gauge.Sub(float64(1))
			}
			return nil
		})
	}
	err := errgroup.Wait()
	if err != nil {
		return
	}

	require.Equal(b, b.N, gauge.IntValue())
}

func Benchmark_Counts_Atomic(b *testing.B) {
	m := sync.Map{}
	b.ResetTimer()

	const writeRatio = 10

	group := errgroup.Group{}
	group.Go(func() error {
		for i := 0; i < b.N; i++ {
			aa := int64(0)
			a, _ := m.LoadOrStore(workspaceID1, &aa)
			atomic.AddInt64(a.(*int64), writeRatio+1)
		}
		return nil
	})
	for i := 0; i < writeRatio; i++ {
		group.Go(func() error {
			for i := 0; i < b.N; i++ {
				aa := int64(0)
				a, _ := m.LoadOrStore(workspaceID1, &aa)
				atomic.AddInt64(a.(*int64), -1)
			}
			return nil
		})
	}
	err := group.Wait()
	if err != nil {
		return
	}
	a, _ := m.LoadOrStore(workspaceID1, int64(0))

	require.Equal(b, int64(b.N), atomic.LoadInt64(a.(*int64)))
}
