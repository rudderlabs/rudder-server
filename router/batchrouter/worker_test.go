package batchrouter

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
)

// generateTestDestination creates a test destination with the specified number of sources
func generateTestDestination(sourcesCount int) routerutils.DestinationWithSources {
	dest := routerutils.DestinationWithSources{
		Destination: backendconfig.DestinationT{
			ID:      "test_dest",
			Enabled: true,
		},
		Sources: make([]backendconfig.SourceT, sourcesCount),
	}

	for i := 0; i < sourcesCount; i++ {
		dest.Sources[i] = backendconfig.SourceT{
			ID: fmt.Sprintf("source_%d", i),
		}
	}

	return dest
}

// generateTestJobs creates test jobs with specified characteristics
func generateTestJobs(count, sourcesCount int) []*jobsdb.JobT {
	jobs := make([]*jobsdb.JobT, count)
	baseTime := time.Now()

	for i := 0; i < count; i++ {
		// Distribute jobs across sources
		sourceIdx := i % sourcesCount

		jobs[i] = &jobsdb.JobT{
			JobID:      int64(i + 1),
			CreatedAt:  baseTime.Add(time.Duration(i) * time.Millisecond), // Ensure unique timestamps
			Parameters: []byte(fmt.Sprintf(`{"source_id":"source_%d","source_job_run_id":"run_%d"}`, sourceIdx, i)),
			LastJobStatus: jobsdb.JobStatusT{
				AttemptNum: 1,
			},
			WorkspaceId: "test_workspace",
		}
	}
	return jobs
}

// createDrainer creates a real drainer configured to never drain jobs
func createDrainer() routerutils.Drainer {
	conf := config.New()
	// Set a very long job retention to prevent time-based draining
	conf.Set("Router.jobRetention", "720h") // 30 days
	// Empty list of job run IDs to drain
	conf.Set("drain.jobRunIDs", []string{})
	// Empty list of destination IDs to abort
	conf.Set("Router.toAbortDestinationIDs", []string{})

	// Create destination resolver that always returns the test destination
	destMap := make(map[string]*routerutils.DestinationWithSources)
	destResolver := func(destinationID string) (*routerutils.DestinationWithSources, bool) {
		dest, ok := destMap[destinationID]
		return dest, ok
	}

	return routerutils.NewDrainer(conf, destResolver)
}

// BenchmarkGetDrainList runs comprehensive benchmarks for getDrainList
func BenchmarkGetDrainList(b *testing.B) {
	testCases := []struct {
		name         string
		jobCount     int
		sourcesCount int
	}{
		{
			name:         "Small_100_Jobs",
			jobCount:     100,
			sourcesCount: 5,
		},
		{
			name:         "Medium_1K_Jobs",
			jobCount:     1000,
			sourcesCount: 10,
		},
		{
			name:         "Large_10K_Jobs",
			jobCount:     10000,
			sourcesCount: 20,
		},
		{
			name:         "VeryLarge_100K_Jobs",
			jobCount:     100000,
			sourcesCount: 50,
		},
	}

	// Create real drainer configured to never drain
	drainer := createDrainer()

	for _, tc := range testCases {
		// Generate test data once per test case
		dest := generateTestDestination(tc.sourcesCount)
		jobs := generateTestJobs(tc.jobCount, tc.sourcesCount)

		destinationJobs := &DestinationJobs{
			jobs:            jobs,
			destWithSources: dest,
		}

		// Create Handle with real drainer
		brt := &Handle{
			drainer: drainer,
		}

		b.Run(tc.name, func(b *testing.B) {
			// Warmup run to ensure everything is initialized
			drainList, drainJobList, statusList, jobIDConnectionDetailsMap, jobsBySource := brt.getDrainList(destinationJobs, dest)

			// Verify the non-draining behavior
			require.Empty(b, drainList, "No jobs should be drained")
			require.Empty(b, drainJobList, "No jobs should be in drain job list")
			require.Equal(b, tc.jobCount, len(statusList), "All jobs should be in status list")
			require.Equal(b, tc.jobCount, len(jobIDConnectionDetailsMap), "Should have connection details for all jobs")

			// Verify jobs by source distribution
			totalJobsBySource := 0
			for _, jobs := range jobsBySource {
				totalJobsBySource += len(jobs)
			}
			require.Equal(b, tc.jobCount, totalJobsBySource, "All jobs should be in jobsBySource")

			// Reset timer and run actual benchmark
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				brt.getDrainList(destinationJobs, dest)
			}
		})
	}
}

// TestGetDrainList verifies the correctness of getDrainList in non-draining scenario
func TestGetDrainList(t *testing.T) {
	jobCount := 100
	sourcesCount := 5

	jobs := generateTestJobs(jobCount, sourcesCount)
	dest := generateTestDestination(sourcesCount)

	// Create real drainer configured to never drain
	brt := &Handle{
		drainer: createDrainer(),
	}

	destinationJobs := &DestinationJobs{
		jobs:            jobs,
		destWithSources: dest,
	}

	drainList, drainJobList, statusList, jobIDConnectionDetailsMap, jobsBySource := brt.getDrainList(destinationJobs, dest)

	// Verify no draining occurred
	require.Empty(t, drainList, "No jobs should be drained")
	require.Empty(t, drainJobList, "No jobs should be in drain job list")

	// Verify all jobs are in status list
	require.Equal(t, jobCount, len(statusList), "All jobs should be in status list")
	for _, status := range statusList {
		require.Equal(t, jobsdb.Executing.State, status.JobState, "All jobs should be in executing state")
		require.Empty(t, status.ErrorCode, "No error code should be set")
		require.Equal(t, []byte(`{}`), status.ErrorResponse, "No error response should be set")
	}

	// Verify job ID connection details
	require.Equal(t, jobCount, len(jobIDConnectionDetailsMap), "Should have connection details for all jobs")

	// Verify jobs are correctly distributed by source
	totalJobsBySource := 0
	for sourceID, jobs := range jobsBySource {
		totalJobsBySource += len(jobs)
		// Verify all jobs in this source have the correct source ID
		for _, job := range jobs {
			require.Equal(t, sourceID, gjson.GetBytes(job.Parameters, "source_id").String())
		}
	}
	require.Equal(t, jobCount, totalJobsBySource, "All jobs should be in jobsBySource")

	// Verify source distribution
	expectedJobsPerSource := jobCount / sourcesCount
	for _, jobs := range jobsBySource {
		// Allow for one extra job due to uneven distribution
		require.True(t, len(jobs) >= expectedJobsPerSource-1 && len(jobs) <= expectedJobsPerSource+1,
			"Jobs should be evenly distributed across sources")
	}
}

// getDrainListMaster is the original implementation from master branch
func getDrainListMaster(destinationJobs *DestinationJobs, destWithSources routerutils.DestinationWithSources, drainer routerutils.Drainer) ([]*jobsdb.JobStatusT, []*jobsdb.JobT, []*jobsdb.JobStatusT, map[int64]jobsdb.ConnectionDetails, map[string][]*jobsdb.JobT) {
	jobs := destinationJobs.jobs
	drainList := make([]*jobsdb.JobStatusT, 0)
	drainJobList := make([]*jobsdb.JobT, 0)
	statusList := make([]*jobsdb.JobStatusT, 0)
	jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionDetails)
	jobsBySource := make(map[string][]*jobsdb.JobT)

	for _, job := range jobs {
		sourceID := gjson.GetBytes(job.Parameters, "source_id").String()
		sourceJobRunID := gjson.GetBytes(job.Parameters, "source_job_run_id").String()
		destinationID := destWithSources.Destination.ID
		jobIDConnectionDetailsMap[job.JobID] = jobsdb.ConnectionDetails{
			SourceID:      sourceID,
			DestinationID: destinationID,
		}

		if drain, reason := drainer.Drain(destinationID, sourceJobRunID, job.CreatedAt); drain {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				JobState:      jobsdb.Aborted.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     routerutils.DRAIN_ERROR_CODE,
				ErrorResponse: routerutils.EnhanceJSON([]byte(`{}`), "reason", reason),
				Parameters:    []byte(`{}`),
				JobParameters: job.Parameters,
				WorkspaceId:   job.WorkspaceId,
			}
			job.Parameters = routerutils.EnhanceJSON(job.Parameters, "stage", "batch_router")
			job.Parameters = routerutils.EnhanceJSON(job.Parameters, "reason", reason)
			drainList = append(drainList, &status)
			drainJobList = append(drainJobList, job)
		} else {
			if _, ok := jobsBySource[sourceID]; !ok {
				jobsBySource[sourceID] = make([]*jobsdb.JobT, 0)
			}
			jobsBySource[sourceID] = append(jobsBySource[sourceID], job)

			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				JobState:      jobsdb.Executing.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`),
				Parameters:    []byte(`{}`),
				JobParameters: job.Parameters,
				WorkspaceId:   job.WorkspaceId,
			}
			statusList = append(statusList, &status)
		}
	}

	return drainList, drainJobList, statusList, jobIDConnectionDetailsMap, jobsBySource
}

// BenchmarkCompareDrainList compares the performance of master vs current implementation
func BenchmarkCompareDrainList(b *testing.B) {
	testCases := []struct {
		name         string
		jobCount     int
		sourcesCount int
	}{
		{
			name:         "Small_100_Jobs",
			jobCount:     100,
			sourcesCount: 5,
		},
		{
			name:         "Medium_1K_Jobs",
			jobCount:     1000,
			sourcesCount: 10,
		},
		{
			name:         "Large_10K_Jobs",
			jobCount:     10000,
			sourcesCount: 20,
		},
		{
			name:         "VeryLarge_100K_Jobs",
			jobCount:     100000,
			sourcesCount: 50,
		},
	}

	// Create real drainer configured to never drain
	drainer := createDrainer()

	for _, tc := range testCases {
		// Generate test data once per test case
		dest := generateTestDestination(tc.sourcesCount)
		jobs := generateTestJobs(tc.jobCount, tc.sourcesCount)

		destinationJobs := &DestinationJobs{
			jobs:            jobs,
			destWithSources: dest,
		}

		// Benchmark master implementation
		b.Run(fmt.Sprintf("Master_%s", tc.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				getDrainListMaster(destinationJobs, dest, drainer)
			}
		})

		// Benchmark current implementation
		brt := &Handle{
			drainer: drainer,
		}

		b.Run(fmt.Sprintf("Current_%s", tc.name), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				brt.getDrainList(destinationJobs, dest)
			}
		})
	}
}

// TestDrainListProfiling runs CPU profiling for both implementations
func TestDrainListProfiling(t *testing.T) {
	// Test configuration
	jobCount := 100000 // Large number of jobs for better profiling
	sourcesCount := 50 // Multiple sources
	iterations := 100  // Number of iterations to run

	// Setup test data
	dest := generateTestDestination(sourcesCount)
	jobs := generateTestJobs(jobCount, sourcesCount)
	drainer := createDrainer()

	destinationJobs := &DestinationJobs{
		jobs:            jobs,
		destWithSources: dest,
	}

	brt := &Handle{
		drainer: drainer,
	}

	// Create profiles directory if it doesn't exist
	profilesDir := "/Users/rohithbcs/Desktop/rudder/rudder-server/profiles"
	if err := os.MkdirAll(profilesDir, 0755); err != nil {
		t.Fatalf("Could not create profiles directory: %v", err)
	}

	// Profile master implementation
	masterFilePath := fmt.Sprintf("%s/cpu_master.prof", profilesDir)
	masterFile, err := os.Create(masterFilePath)
	if err != nil {
		t.Fatalf("Could not create CPU profile for master: %v", err)
	}
	defer masterFile.Close()

	if err := pprof.StartCPUProfile(masterFile); err != nil {
		t.Fatalf("Could not start CPU profile for master: %v", err)
	}

	// Run master implementation multiple times
	for i := 0; i < iterations; i++ {
		getDrainListMaster(destinationJobs, dest, drainer)
	}
	pprof.StopCPUProfile()

	// Wait a bit to ensure profiles are separate
	time.Sleep(time.Second)

	// Profile current implementation
	currentFilePath := fmt.Sprintf("%s/cpu_current.prof", profilesDir)
	currentFile, err := os.Create(currentFilePath)
	if err != nil {
		t.Fatalf("Could not create CPU profile for current: %v", err)
	}
	defer currentFile.Close()

	if err := pprof.StartCPUProfile(currentFile); err != nil {
		t.Fatalf("Could not start CPU profile for current: %v", err)
	}

	// Run current implementation multiple times
	for i := 0; i < iterations; i++ {
		brt.getDrainList(destinationJobs, dest)
	}
	pprof.StopCPUProfile()

	//// Basic correctness check
	//drainList, drainJobList, statusList, jobIDConnectionDetailsMap, jobsBySource := brt.getDrainList(destinationJobs, dest)
	//
	//// Verify results
	//require.Empty(t, drainList, "No jobs should be drained")
	//require.Empty(t, drainJobList, "No jobs should be in drain job list")
	//require.Equal(t, jobCount, len(statusList), "All jobs should be in status list")
	//require.Equal(t, jobCount, len(jobIDConnectionDetailsMap), "Should have connection details for all jobs")
	//
	//totalJobsBySource := 0
	//for _, jobs := range jobsBySource {
	//	totalJobsBySource += len(jobs)
	//}
	//require.Equal(t, jobCount, totalJobsBySource, "All jobs should be in jobsBySource")

	t.Logf("CPU profiles generated in %s:\n- Master implementation: cpu_master.prof\n- Current implementation: cpu_current.prof", profilesDir)
	t.Logf("To analyze the profiles, run:\n- go tool pprof %s/cpu_master.prof\n- go tool pprof %s/cpu_current.prof", profilesDir, profilesDir)
}
