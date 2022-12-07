package jobsdb

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
)

func TestMultiTenantHandleT_GetAllJobs(t *testing.T) {
	_ = startPostgres(t)
	maxDSSize := 2
	jobDB := MultiTenantHandleT{HandleT: &HandleT{MaxDSSize: &maxDSSize}}

	err := jobDB.Setup(ReadWrite, false, "rt", true, []prebackup.Handler{}, fileuploader.NewDefaultProvider())
	require.NoError(t, err, "expected no error while jobsDB setup")
	defer jobDB.TearDown()

	customVal := "MOCKDS"
	testWID := "testWorkspaceID"
	sampleTestJob1 := JobT{
		WorkspaceId:  testWID,
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    customVal,
	}
	sampleTestJob2 := JobT{
		WorkspaceId:  testWID,
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    customVal,
	}
	sampleTestJob3 := JobT{
		WorkspaceId:  testWID,
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.New(),
		CustomVal:    customVal,
	}

	workspaceCountMap := make(map[string]int)
	workspaceCountMap[testWID] = 1
	payloadLimit := 100 * bytesize.MB
	unprocessedListEmpty, err := jobDB.GetAllJobs(context.Background(), workspaceCountMap, GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobsLimit:        1,
		ParameterFilters: []ParameterFilterT{},
		PayloadSizeLimit: payloadLimit,
	}, 10, nil)
	require.NoError(t, err, "Error getting All jobs")
	require.Equal(t, 0, len(unprocessedListEmpty.Jobs))

	err = jobDB.Store(context.Background(), []*JobT{&sampleTestJob1, &sampleTestJob2, &sampleTestJob3})
	require.NoError(t, err)

	payloadLimit = 100 * bytesize.MB
	workspaceCountMap[testWID] = 3
	unprocessedList, err := jobDB.GetAllJobs(context.Background(), workspaceCountMap, GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobsLimit:        3,
		ParameterFilters: []ParameterFilterT{},
		PayloadSizeLimit: payloadLimit,
	}, 10, nil)
	require.NoError(t, err, "Error getting All jobs")
	require.Equal(t, 3, len(unprocessedList.Jobs))

	status1 := JobStatusT{
		JobID:         unprocessedList.Jobs[0].JobID,
		JobState:      "waiting",
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "202",
		ErrorResponse: []byte(`{"success":"OK"}`),
		Parameters:    []byte(`{}`),
		WorkspaceId:   "testWorkspace",
	}
	status2 := JobStatusT{
		JobID:         unprocessedList.Jobs[1].JobID,
		JobState:      "failed",
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "202",
		ErrorResponse: []byte(`{"success":"OK"}`),
		Parameters:    []byte(`{}`),
		WorkspaceId:   "testWorkspace",
	}

	err = jobDB.UpdateJobStatus(context.Background(), []*JobStatusT{&status1, &status2}, []string{customVal}, []ParameterFilterT{})
	require.NoError(t, err)

	payloadLimit = 100 * bytesize.MB
	workspaceCountMap[testWID] = 3
	jobs, err := jobDB.GetAllJobs(context.Background(), workspaceCountMap, GetQueryParamsT{
		CustomValFilters: []string{customVal},
		JobsLimit:        3,
		ParameterFilters: []ParameterFilterT{},
		PayloadSizeLimit: payloadLimit,
	}, 10, nil)
	require.NoError(t, err, "Error getting All jobs")
	require.Equal(t, 3, len(jobs.Jobs))
}

/*
rt

BenchmarkUnionQuery/UnionQuery-1000-8         	                    	                        1358249041 ns/op
BenchmarkUnionQuery/Concurrent_Union_Query__-1000_-16-8         	                1666216834 ns/op
BenchmarkUnionQuery/Concurrent_Union_Query__-1000_-32-8         	                3847614042 ns/op
BenchmarkUnionQuery/ParallelQuery-1000-8                        	                                 1538826083 ns/op

BenchmarkUnionQuery/UnionQuery-3000-8                           	                                 4177400417 ns/op
BenchmarkUnionQuery/Concurrent_Union_Query__-3000_-16-8         	                 5335373167 ns/op
BenchmarkUnionQuery/Concurrent_Union_Query__-3000_-32-8         	                 11321719167 ns/op
BenchmarkUnionQuery/ParallelQuery-3000-8                        	                                 3538207958 ns/op

BenchmarkUnionQuery/UnionQuery-5000-8                           	                                6365580875 ns/op
BenchmarkUnionQuery/Concurrent_Union_Query__-5000_-16-8         	                8497499458 ns/op
BenchmarkUnionQuery/Concurrent_Union_Query__-5000_-32-8         	                17646324750 ns/op
BenchmarkUnionQuery/ParallelQuery-5000-8                        	                                 5540005584 ns/op

BenchmarkUnionQuery/UnionQuery-10000-8                          	                                12698598542 ns/op
BenchmarkUnionQuery/Concurrent_Union_Query__-10000_-16-8        	                19103076166 ns/op
BenchmarkUnionQuery/Concurrent_Union_Query__-10000_-32-8        	                38923613417 ns/op
BenchmarkUnionQuery/ParallelQuery-10000-8                       	                                 11816298834 ns/op

BenchmarkUnionQuery/UnionQuery-100000-8                         	                                 143067570417 ns/op
BenchmarkUnionQuery/Concurrent_Union_Query__-100000_-16-8       	                 200310106917 ns/op
BenchmarkUnionQuery/Concurrent_Union_Query__-100000_-32-8       	                 409845463834 ns/op
BenchmarkUnionQuery/ParallelQuery-100000-8                      	                                  118793139708 ns/op
*/

// BenchmarkUnionQuery is a benchmark for UnionQuery with 20 different workspaces
// and jobSize is the total number of jobs split across the workspaces
func BenchmarkUnionQuery(b *testing.B) {
	_ = startPostgres(b)
	jobSize := []int{10000}
	workspacesCounts := []int{30}
	for _, size := range jobSize {
		for _, workspacesCount := range workspacesCounts {
			workspaces := generateWorkspaces(workspacesCount)
			workspaceSize := (size + workspacesCount - 1) / workspacesCount
			var workspaceDistribution []string
			var j int
			for i, workspace := range workspaces {
				for j = 0; j < workspaceSize && i*j < size; j++ {
					workspaceDistribution = append(workspaceDistribution, workspace)
				}
				if i*j >= size {
					break
				}
			}
			jobs, testWorkspacePickup := generateJobs(size, workspaceDistribution)
			jobStatuses := generateJobStatuses(jobs)
			b.Setenv("RSERVER_JOBS_DB_ENABLE_WRITER_QUEUE", "true")
			b.Setenv("RSERVER_JOBS_DB_ENABLE_READER_QUEUE", "true")
			maxDSSize := 500
			jobsDb1 := MultiTenantHandleT{HandleT: &HandleT{
				MaxDSSize: &maxDSSize,
			}}
			err := jobsDb1.Setup(ReadWrite, true, "rt", true, []prebackup.Handler{}, fileuploader.NewDefaultProvider())
			require.NoError(b, err)
			err = jobsDb1.Store(context.Background(), jobs)
			require.NoError(b, err)
			err = jobsDb1.UpdateJobStatus(context.Background(), jobStatuses, []string{"testCustomVal"}, nil)
			require.NoError(b, err)
			ctx, cancel := context.WithCancel(context.Background())
			go cronStoreJobs(ctx, MultiTenantLegacy(jobsDb1), workspaces)

			b.Run(fmt.Sprintf("UnionQuery-%d-%s", size, "rt"), func(b *testing.B) {
				benchmarkUnionQuery(b, jobsDb1, testWorkspacePickup, "testCustomVal")
			})
			cancel()
			jobsDb1.TearDown()

			jobsDb2 := MultiTenantLegacy{HandleT: &HandleT{
				MaxDSSize: &maxDSSize,
			}}
			err = jobsDb2.Setup(ReadWrite, true, "rt", true, []prebackup.Handler{}, fileuploader.NewDefaultProvider())
			require.NoError(b, err)
			err = jobsDb2.Store(context.Background(), jobs)
			require.NoError(b, err)
			err = jobsDb2.UpdateJobStatus(context.Background(), jobStatuses, []string{"testCustomVal"}, nil)
			require.NoError(b, err)
			ctx, cancel = context.WithCancel(context.Background())
			go cronStoreJobs(ctx, jobsDb2, workspaces)
			b.Run(fmt.Sprintf("NewQuery-%d-%d-%s", size, workspacesCount, "rt"), func(b *testing.B) {
				benchmarkParallelQuery(b, jobsDb2, testWorkspacePickup, "testCustomVal")
			})

			cancel()
			jobsDb2.TearDown()

			b.Setenv("RSERVER_JOBS_DB_PARTITION_COUNT", "10")
			jobsDb3 := MultiTenantLegacy{HandleT: &HandleT{
				MaxDSSize: &maxDSSize,
			}}
			err = jobsDb3.Setup(ReadWrite, true, "rt", true, []prebackup.Handler{}, fileuploader.NewDefaultProvider(), "HASH")
			require.NoError(b, err)
			err = jobsDb3.Store(context.Background(), jobs)
			require.NoError(b, err)
			err = jobsDb3.UpdateJobStatus(context.Background(), jobStatuses, []string{"testCustomVal"}, nil)
			require.NoError(b, err)
			ctx, cancel = context.WithCancel(context.Background())
			go cronStoreJobs(ctx, jobsDb3, workspaces)
			b.Run(fmt.Sprintf("NewQueryPartition-%d-%d-%s-%s", size, workspacesCount, "HASH", "rt"), func(b *testing.B) {
				benchmarkParallelQuery(b, jobsDb3, testWorkspacePickup, "testCustomVal")
			})

			cancel()
			jobsDb3.TearDown()
		}
	}
}

func generateJobStatuses(jobs []*JobT) []*JobStatusT {
	var jobStatuses []*JobStatusT
	jobStates := []string{"waiting", "failed"}
	for _, job := range jobs {
		jobStatuses = append(jobStatuses, &JobStatusT{
			JobID:         job.JobID,
			JobState:      jobStates[rand.Int()%2],
			AttemptNum:    1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "0",
			ErrorResponse: []byte(`{}`),
		})
	}
	return jobStatuses
}

func benchmarkUnionQuery(b *testing.B, jobsdb MultiTenantHandleT, testWorkspacePickup map[string]int, customVal string) {
	b.ResetTimer()
	for i := 0; i < 10; i++ {
		_, err := jobsdb.GetAllJobs(context.Background(), testWorkspacePickup, GetQueryParamsT{
			CustomValFilters: []string{customVal},
			JobsLimit:        10,
		}, 10, nil)
		require.NoError(b, err)
	}
}

func benchmarkConcurrentUnionQuery(b *testing.B, jobsdb MultiTenantHandleT, testWorkspacePickup map[string]int, customVal string, concurrency int) {
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(concurrency)
		for i = 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				_, err := jobsdb.GetAllJobs(context.Background(), testWorkspacePickup, GetQueryParamsT{
					CustomValFilters: []string{customVal},
					JobsLimit:        10,
				}, 10, nil)
				require.NoError(b, err)
			}()
		}
		wg.Wait()
	}
}

func benchmarkParallelQuery(b *testing.B, jobsdb MultiTenantLegacy, testWorkspacePickup map[string]int, customVal string) {
	b.ResetTimer()
	for i := 0; i < 10; i++ {
		concurrencies := len(testWorkspacePickup)
		var wg sync.WaitGroup
		wg.Add(concurrencies)
		for workspaceID, count := range testWorkspacePickup {
			go func(workspaceID string, count int) {
				defer wg.Done()
				res, err := jobsdb.GetAllJobs(context.Background(), map[string]int{workspaceID: count}, GetQueryParamsT{
					CustomValFilters: []string{customVal},
					WorkspaceFilter:  []string{workspaceID},
					JobsLimit:        10,
				}, 10, nil)
				require.NoError(b, err)
				require.Equal(b, count, len(res.Jobs))
			}(workspaceID, count)
		}
		wg.Wait()
	}
}

func benchmarkConcurrentParallelQuery(b *testing.B, jobsdb MultiTenantLegacy, testWorkspacePickup map[string]int, customVal string, concurrency int) {
	b.ResetTimer()
	for i := 0; i < 10; i++ {
		var wg sync.WaitGroup
		wg.Add(concurrency)
		for i = 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				parallelism := len(testWorkspacePickup)
				var wgInternal sync.WaitGroup
				wgInternal.Add(parallelism)
				for workspaceID, count := range testWorkspacePickup {
					go func(workspaceID string, count int) {
						defer wg.Done()
						_, err := jobsdb.GetAllJobs(context.Background(), map[string]int{workspaceID: count}, GetQueryParamsT{
							CustomValFilters: []string{customVal},
							WorkspaceFilter:  []string{workspaceID},
							JobsLimit:        10,
						}, 10, nil)
						require.NoError(b, err)
					}(workspaceID, count)
				}
				wgInternal.Wait()
			}()
		}
		wg.Wait()
	}
}

func cronStoreJobs(ctx context.Context, jobsDb MultiTenantLegacy, workspaces []string) {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			jobs, _ := generateJobs(len(workspaces), workspaces)
			err := jobsDb.Store(ctx, jobs)
			if err != nil && err != context.Canceled {
				panic(err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func generateJobs(size int, workspaces []string) ([]*JobT, map[string]int) {
	jobs := make([]*JobT, size)
	testWorkspacePickup := map[string]int{}
	for i := 0; i < size; i++ {
		testWorkspacePickup[workspaces[i]]++
		jobs[i] = &JobT{
			WorkspaceId:  workspaces[i],
			Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.Must(uuid.NewRandom()),
			CustomVal:    "testCustomVal",
		}
	}
	return jobs, testWorkspacePickup
}

func generateWorkspaces(size int) []string {
	workspaces := make([]string, size)
	for i := 0; i < size; i++ {
		workspaces[i] = fmt.Sprintf("testWorkspace%d", i)
	}
	return workspaces
}

// BenchmarkUnionQuery/ParallelQuery-10000-30-HASH-rt-8         	       1	1104393041 ns/op
// BenchmarkUnionQuery/ParallelQuery-10000-50-HASH-rt-8         	       1	1158288708 ns/op
// BenchmarkUnionQuery/ParallelQuery-10000-100-HASH-rt-8        	       1	1410090625 ns/op
// BenchmarkUnionQuery/ParallelQuery-10000-150-HASH-rt-8        	       1	1335240708 ns/op
// BenchmarkUnionQuery/ParallelQuery-10000-200-HASH-rt-8        	       1	1522682042 ns/op
// BenchmarkUnionQuery/ParallelQuery-100000-30-HASH-rt-8        	       1	11641253083 ns/op
// BenchmarkUnionQuery/ParallelQuery-100000-50-HASH-rt-8        	       1	11223804541 ns/op
// BenchmarkUnionQuery/ParallelQuery-100000-100-HASH-rt-8       	       1	11641310875 ns/op
// BenchmarkUnionQuery/ParallelQuery-100000-150-HASH-rt-8       	       1	11675503250 ns/op
// BenchmarkUnionQuery/ParallelQuery-100000-200-HASH-rt-8       	       1	11124351625 ns/op

// Different scenarios hardcoded in the benchmark
// Different benchmark should run against same data set
// Write and Reads having concurrently : We have non-empty status tables
// Try and have multiple tables
