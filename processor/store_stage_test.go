package processor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	"github.com/rudderlabs/rudder-server/enterprise/activationrecords"
	"github.com/rudderlabs/rudder-server/enterprise/trackedusers"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/tracing"
)

func TestStoreStageBatchRouterDestinationLock(t *testing.T) {
	job := func(destType, destID string) *jobsdb.JobT {
		return &jobsdb.JobT{
			CustomVal:  destType,
			Parameters: fmt.Appendf(nil, `{"destination_id":%q}`, destID),
		}
	}
	for _, concurrentStore := range []bool{false, true} {
		t.Run(fmt.Sprintf("concurrentStore=%t", concurrentStore), func(t *testing.T) {
			for _, tc := range []struct {
				name           string
				firstJobs      []*jobsdb.JobT
				secondJobs     []*jobsdb.JobT
				secondPipeline int
				includeRouter  bool
				serialize      bool
			}{
				{
					name:       "same destination",
					firstJobs:  []*jobsdb.JobT{job("SNOWPIPE_STREAMING", "dest-1")},
					secondJobs: []*jobsdb.JobT{job("SNOWPIPE_STREAMING", "dest-1")},
					serialize:  true,
				},
				{
					name:           "same destination across pipelines",
					firstJobs:      []*jobsdb.JobT{job("SNOWPIPE_STREAMING", "dest-1")},
					secondJobs:     []*jobsdb.JobT{job("SNOWPIPE_STREAMING", "dest-1")},
					secondPipeline: 1,
					serialize:      true,
				},
				{
					name:       "different destinations",
					firstJobs:  []*jobsdb.JobT{job("SNOWPIPE_STREAMING", "dest-1")},
					secondJobs: []*jobsdb.JobT{job("SNOWPIPE_STREAMING", "dest-2")},
				},
				{
					name:       "other destination types remain concurrent",
					firstJobs:  []*jobsdb.JobT{job("S3", "dest-1")},
					secondJobs: []*jobsdb.JobT{job("S3", "dest-1")},
				},
				{
					name: "mixed types and repeated destination IDs",
					firstJobs: []*jobsdb.JobT{
						job("S3", "dest-3"),
						job("SNOWPIPE_STREAMING", "dest-2"),
						job("SNOWPIPE_STREAMING", "dest-1"),
						job("SNOWPIPE_STREAMING", "dest-2"),
					},
					secondJobs: []*jobsdb.JobT{
						job("SNOWPIPE_STREAMING", "dest-1"),
						job("SNOWPIPE_STREAMING", "dest-2"),
					},
					secondPipeline: 1,
					includeRouter:  true,
					serialize:      true,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctrl := gomock.NewController(t)
					gatewayDB := mocksJobsDB.NewMockJobsDB(ctrl)
					routerDB := mocksJobsDB.NewMockJobsDB(ctrl)
					batchDB := mocksJobsDB.NewMockJobsDB(ctrl)
					proc := newStoreStageTestHandle(gatewayDB, routerDB, batchDB, concurrentStore)

					var statusTransactions atomic.Int32
					gatewayDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Times(2).
						DoAndReturn(func(_ context.Context, f func(jobsdb.UpdateSafeTx) error) error {
							statusTransactions.Add(1)
							return f(jobsdb.EmptyUpdateSafeTx())
						})
					gatewayDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
					if tc.includeRouter {
						routerDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(2).
							DoAndReturn(func(_ context.Context, f func(jobsdb.StoreSafeTx) error) error {
								return f(jobsdb.EmptyStoreSafeTx())
							})
						routerDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
					}

					firstStored := make(chan struct{}, 1)
					secondStarted := make(chan struct{}, 1)
					releaseCommit := make(chan struct{})
					release := sync.OnceFunc(func() { close(releaseCommit) })
					var workers sync.WaitGroup
					t.Cleanup(func() {
						release()
						workers.Wait()
					})
					var transactions atomic.Int32
					batchDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(2).
						DoAndReturn(func(_ context.Context, f func(jobsdb.StoreSafeTx) error) error {
							first := transactions.Add(1) == 1
							if !first {
								secondStarted <- struct{}{}
							}
							err := f(jobsdb.EmptyStoreSafeTx())
							if first {
								// StoreInTx has returned, but the transaction has not committed yet.
								firstStored <- struct{}{}
								<-releaseCommit
							}
							return err
						})
					batchDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)

					start := func(partition string, pipeline int, jobs []*jobsdb.JobT) {
						collector := rsources.NewStatsCollector(proc.rsourcesService, "processor", stats.NOP)
						collector.BeginProcessing(nil)
						message := &storeMessage{ctx: t.Context(), batchDestJobs: jobs, rsourcesStats: collector}
						if tc.includeRouter {
							message.routerDestIDs = []string{"router-" + partition}
							message.destJobs = []*jobsdb.JobT{job("WEBHOOK", message.routerDestIDs[0])}
						}
						workers.Go(func() { proc.storeStage(partition, pipeline, message) })
					}
					waitFor := func(ch <-chan struct{}) {
						t.Helper()
						require.Eventually(t, func() bool {
							select {
							case <-ch:
								return true
							default:
								return false
							}
						}, 5*time.Second, time.Millisecond)
					}
					start("partition-1", 0, tc.firstJobs)
					waitFor(firstStored)
					if concurrentStore {
						require.Eventually(t, func() bool { return statusTransactions.Load() == 1 }, 5*time.Second, time.Millisecond)
					}
					start("partition-2", tc.secondPipeline, tc.secondJobs)
					if tc.serialize {
						select {
						case <-secondStarted:
							t.Error("second store began before the first transaction committed")
						case <-time.After(100 * time.Millisecond):
						}
						if concurrentStore {
							require.EqualValues(t, 1, statusTransactions.Load(), "waiting for a destination lock must not hold a status transaction")
						}
						release()
						require.Eventually(t, func() bool { return transactions.Load() == 2 }, 5*time.Second, time.Millisecond)
					} else {
						waitFor(secondStarted)
						release()
					}
					workers.Wait()
				})
			}
		})
	}
}

func newStoreStageTestHandle(gatewayDB, routerDB, batchDB jobsdb.JobsDB, concurrentStore bool) *Handle {
	proc := &Handle{
		gatewayDB:                 gatewayDB,
		routerDB:                  routerDB,
		batchRouterDB:             batchDB,
		storePlocker:              *kitsync.NewPartitionLocker(),
		logger:                    logger.NOP,
		statsFactory:              stats.NOP,
		tracer:                    tracing.New(stats.NOPTracer),
		rsourcesService:           rsources.NewNoOpService(),
		trackedUsersReporter:      trackedusers.NewNoopDataCollector(),
		activationRecordsReporter: activationrecords.NewNoopActivationRecordsReporter(),
	}
	proc.config.enableConcurrentStore = config.SingleValueLoader(concurrentStore)
	proc.jobsDBCommandTimeout = config.SingleValueLoader(time.Second)
	proc.jobdDBMaxRetries = config.SingleValueLoader(1)
	measurement := func(_, _ string) stats.Measurement { return stats.NOP.NewStat("store", stats.CountType) }
	proc.stats.statBatchDestNumOutputEvents = measurement
	proc.stats.statDBWriteBatchPayloadBytes = measurement
	proc.stats.statDestNumOutputEvents = measurement
	proc.stats.statDBWriteRouterPayloadBytes = measurement
	proc.stats.statDBW = measurement
	proc.stats.statGatewayDBW = measurement
	return proc
}
