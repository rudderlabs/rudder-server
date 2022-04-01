package multitenant

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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
		metric.GetManager().Reset()
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
			tenantStats = NewStats(map[string]jobsdb.MultiTenantJobsDB{"rt": mockRouterJobsDB})
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
			addJobWID1 := rand.Intn(10)
			addJobWID2 := rand.Intn(10)
			input := make(map[string]map[string]int)
			input[workspaceID1] = make(map[string]int)
			input[workspaceID2] = make(map[string]int)
			input[workspaceID1][destType1] = addJobWID1
			input[workspaceID2][destType1] = addJobWID2

			tenantStats.ReportProcLoopAddStats(input, "rt")

			Expect(metric.GetPendingEventsMeasurement("rt", workspaceID1, destType1).IntValue()).To(Equal(addJobWID1))
			Expect(metric.GetPendingEventsMeasurement("rt", workspaceID2, destType1).IntValue()).To(Equal(addJobWID2))
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
			tenantStats.ReportProcLoopAddStats(input, "rt")
			tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID1, 0)
			tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID2, 0)
			tenantStats.UpdateWorkspaceLatencyMap(destType1, workspaceID3, 0)
			routerPickUpJobs, usedLatencies := tenantStats.GetRouterPickupJobs(destType1, noOfWorkers, routerTimeOut, jobQueryBatchSize, timeGained)
			Expect(routerPickUpJobs[workspaceID1]).To(Equal(addJobWID1))
			Expect(routerPickUpJobs[workspaceID2]).To(Equal(addJobWID2))
			Expect(routerPickUpJobs[workspaceID3]).To(Equal(addJobWID3))
			Expect(usedLatencies[workspaceID1]).To(Equal(0.0))
			Expect(usedLatencies[workspaceID2]).To(Equal(0.0))
			Expect(usedLatencies[workspaceID3]).To(Equal(0.0))
		})

		It("Should Pick BETA for slower jobs", func() {
			addJobWID1 := 300
			addJobWID2 := rand.Intn(2000)
			addJobWID3 := rand.Intn(2000)
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
			routerPickUpJobs, usedLatencies := tenantStats.GetRouterPickupJobs(destType1, noOfWorkers, routerTimeOut, 300, timeGained)
			Expect(routerPickUpJobs[workspaceID1]).To(Equal(addJobWID1))
			Expect(routerPickUpJobs[workspaceID2]).To(Equal(1))
			Expect(routerPickUpJobs[workspaceID3]).To(Equal(1))
			Expect(usedLatencies[workspaceID1]).To(Equal(1.0))
		})
	})
})

func Benchmark_Counts(b *testing.B) {

	b.ResetTimer()
	metric.GetManager().Reset()
	const writeRatio = 1000
	gauge := metric.GetPendingEventsMeasurement("rt", workspaceID1, destType1)
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
	errgroup.Wait()

	require.Equal(b, b.N, gauge.IntValue())
}

func Benchmark_Counts_Atomic(b *testing.B) {
	m := sync.Map{}
	b.ResetTimer()

	const writeRatio = 10

	errgroup := errgroup.Group{}
	errgroup.Go(func() error {
		for i := 0; i < b.N; i++ {
			aa := int64(0)
			a, _ := m.LoadOrStore(workspaceID1, &aa)
			atomic.AddInt64(a.(*int64), writeRatio+1)
		}
		return nil
	})
	for i := 0; i < writeRatio; i++ {
		errgroup.Go(func() error {
			for i := 0; i < b.N; i++ {
				aa := int64(0)
				a, _ := m.LoadOrStore(workspaceID1, &aa)
				atomic.AddInt64(a.(*int64), -1)
			}
			return nil
		})
	}
	errgroup.Wait()

	a, _ := m.LoadOrStore(workspaceID1, int64(0))

	require.Equal(b, int64(b.N), atomic.LoadInt64(a.(*int64)))
}
