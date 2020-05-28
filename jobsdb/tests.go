package jobsdb

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	uuid "github.com/satori/go.uuid"
)

// TODO: Internal testing, should be moved to an integration test

/*
================================================
==============Test Functions Below==============
================================================
*/

func (jd *HandleT) dropTables() error {
	sqlStatement := `DROP TABLE IF EXISTS job_status`
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)

	sqlStatement = `DROP TABLE IF EXISTS  jobs`
	_, err = jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)

	return nil

}

func (jd *HandleT) createTestTables() error {
	sqlStatement := `CREATE TABLE jobs (
                             job_id BIGSERIAL PRIMARY KEY,
							 uuid UUID NOT NULL,
							 parameters JSONB NOT NULL,
                             custom_val INT NOT NULL,
                             event_payload JSONB NOT NULL,
                             created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                             expire_at TIMESTAMP NOT NULL DEFAULT NOW());`
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)

	sqlStatement = `CREATE TABLE job_status (
                            id BIGSERIAL PRIMARY KEY,
                            job_id INT REFERENCES jobs(job_id),
                            job_state job_state_type,
                            attempt SMALLINT,
                            exec_time TIMESTAMP,
                            retry_time TIMESTAMP,
                            error_code VARCHAR(32),
                            error_response JSONB);`
	_, err = jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)

	return nil
}

func (jd *HandleT) staticDSTest() {

	testEndPoint := "4"
	testNumRecs := 100001
	testNumQuery := 10000
	testFailRatio := 100

	jd.dropTables()
	jd.createTestTables()

	var jobList []*JobT
	for i := 0; i < testNumRecs; i++ {
		id := uuid.NewV4()
		newJob := JobT{
			UUID:         id,
			CreatedAt:    time.Now(),
			ExpireAt:     time.Now(),
			CustomVal:    testEndPoint,
			EventPayload: []byte(`{"event_type":"click"}`),
		}
		jobList = append(jobList, &newJob)
	}

	testDS := dataSetT{JobTable: "jobs", JobStatusTable: "job_status"}

	start := time.Now()
	jd.storeJobsDS(testDS, false, true, jobList)
	elapsed := time.Since(start)
	fmt.Println("Save", elapsed)

	for {
		start = time.Now()
		fmt.Println("Full:", jd.checkIfFullDS(testDS))
		elapsed = time.Since(start)
		fmt.Println("Checking DS", elapsed)

		start = time.Now()
		unprocessedList, _ := jd.getUnprocessedJobsDS(testDS, []string{testEndPoint}, true, testNumQuery, nil)
		fmt.Println("Got unprocessed events:", len(unprocessedList))

		retryList, _ := jd.getProcessedJobsDS(testDS, false, []string{"failed"},
			[]string{testEndPoint}, testNumQuery, nil)
		fmt.Println("Got retry events:", len(retryList))
		if len(unprocessedList)+len(retryList) == 0 {
			break
		}
		elapsed = time.Since(start)
		fmt.Println("Getting jobs", elapsed)

		//Mark call as executing
		var statusList []*JobStatusT
		for _, job := range append(unprocessedList, retryList...) {
			newStatus := JobStatusT{
				JobID:         job.JobID,
				JobState:      ExecutingState,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "202",
				ErrorResponse: []byte(`{"success":"OK"}`),
			}
			statusList = append(statusList, &newStatus)
		}

		jd.updateJobStatusDS(testDS, statusList, []string{testEndPoint}, nil)

		//Mark call as failed
		statusList = nil
		var maxAttempt = 0
		for _, job := range append(unprocessedList, retryList...) {
			stat := SucceededState
			if rand.Intn(testFailRatio) == 0 {
				stat = FailedState
			}
			if job.LastJobStatus.AttemptNum > maxAttempt {
				maxAttempt = job.LastJobStatus.AttemptNum
			}
			newStatus := JobStatusT{
				JobID:         job.JobID,
				JobState:      stat,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "202",
				ErrorResponse: []byte(`{"success":"OK"}`),
			}
			statusList = append(statusList, &newStatus)
		}
		fmt.Println("Max attempt", maxAttempt)
		jd.updateJobStatusDS(testDS, statusList, []string{testEndPoint}, nil)
	}

}

func (jd *HandleT) dynamicDSTestMigrate() {

	testNumRecs := 10000
	testRuns := 5
	testEndPoint := "4"

	for i := 0; i < testRuns; i++ {
		jd.addNewDS(true, dataSetT{})
		var jobList []*JobT
		for i := 0; i < testNumRecs; i++ {
			id := uuid.NewV4()
			newJob := JobT{
				UUID:         id,
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
				CustomVal:    testEndPoint,
				EventPayload: []byte(`{"event_type":"click"}`),
			}
			jobList = append(jobList, &newJob)
		}
		jd.Store(jobList)
		fmt.Println(jd.getDSList(false))
		fmt.Println(jd.getDSRangeList(false))
		dsList := jd.getDSList(false)
		if i > 0 {
			jd.migrateJobs(dsList[0], dsList[1])
			jd.postMigrateHandleDS([]dataSetT{dsList[0]})
		}
	}

}

func (jd *HandleT) dynamicTest() {

	testNumRecs := 10000
	testRuns := 20
	testEndPoint := "4"
	testNumQuery := 10000
	testFailRatio := 5

	for i := 0; i < testRuns; i++ {
		fmt.Println("Main process running")
		time.Sleep(1 * time.Second)
		var jobList []*JobT
		for i := 0; i < testNumRecs; i++ {
			id := uuid.NewV4()
			newJob := JobT{
				UUID:         id,
				CreatedAt:    time.Now(),
				ExpireAt:     time.Now(),
				CustomVal:    testEndPoint,
				EventPayload: []byte(`{"event_type":"click"}`),
			}
			jobList = append(jobList, &newJob)
		}
		jd.Store(jobList)
		jd.printLists(false)
	}

	for {

		time.Sleep(1 * time.Second)
		jd.printLists(false)

		start := time.Now()
		unprocessedList := jd.GetUnprocessed([]string{testEndPoint}, testNumQuery, nil)
		fmt.Println("Got unprocessed events:", len(unprocessedList))

		retryList := jd.GetToRetry([]string{testEndPoint}, testNumQuery, nil)
		fmt.Println("Got retry events:", len(retryList))
		if len(unprocessedList)+len(retryList) == 0 {
			break
		}
		elapsed := time.Since(start)
		fmt.Println("Getting jobs", elapsed)

		//Mark call as failed
		var statusList []*JobStatusT

		combinedList := append(unprocessedList, retryList...)
		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})
		fmt.Println("Total:", len(combinedList), combinedList[0].JobID,
			combinedList[len(combinedList)-1].JobID)

		for _, job := range append(unprocessedList, retryList...) {
			state := SucceededState
			if rand.Intn(testFailRatio) == 0 {
				state = FailedState
			}
			newStatus := JobStatusT{
				JobID:         job.JobID,
				JobState:      state,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "202",
				ErrorResponse: []byte(`{"success":"OK"}`),
			}
			statusList = append(statusList, &newStatus)
		}
		jd.UpdateJobStatus(statusList, []string{testEndPoint}, nil)
	}
}

/*
RunTest runs some internal tests
*/
func (jd *HandleT) RunTest() {
	jd.dynamicTest()
}
