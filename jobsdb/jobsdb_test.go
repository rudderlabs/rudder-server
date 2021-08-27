package jobsdb

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jinzhu/copier"
	"github.com/lib/pq"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/services/stats"
	uuid "github.com/satori/go.uuid"
)

var _ = Describe("Calculate newDSIdx for internal migrations", func() {
	var _ = DescribeTable("newDSIdx tests",
		func(before, after, expected string) {
			computedIdx, err := computeInsertIdx(before, after)
			Expect(computedIdx).To(Equal(expected))
			Expect(err).To(BeNil())
		},
		//dList => 1 2 3 4 5
		Entry("Internal Migration for regular tables 1 Test 1 : ", "1", "2", "1_1"),
		Entry("Internal Migration for regular tables 1 Test 2 : ", "2", "3", "2_1"),

		//dList => 1_1 2 3 4 5
		Entry("Internal Migration for regular tables 2 Test 1 : ", "1_1", "2", "1_2"),
		Entry("Internal Migration for regular tables 2 Test 2 : ", "2", "3", "2_1"),

		//dList => 1 2_1 3 4 5
		Entry("Internal Migration for regular tables 3 Test 1 : ", "1", "2_1", "1_1"),
		Entry("Internal Migration for regular tables 3 Test 2 : ", "2_1", "3", "2_2"),
		Entry("Internal Migration for regular tables 3 Test 3 : ", "3", "4", "3_1"),

		//dList => 1_1 2_1 3 4 5
		Entry("Internal Migration for regular tables 4 Test 1 : ", "1_1", "2_1", "1_2"),

		//dList => 0_1 1 2 3 4 5
		Entry("Internal Migration for import tables Case 1 Test 1 : ", "0_1", "1", "0_1_1"),
		Entry("Internal Migration for import tables Case 1 Test 2 : ", "1", "2", "1_1"),

		//dList => 0_1 0_2 1 2 3 4 5
		Entry("Internal Migration for import tables Case 2 Test 1 : ", "0_1", "0_2", "0_1_1"),
		Entry("Internal Migration for import tables Case 2 Test 2 : ", "0_2", "1", "0_2_1"),
		Entry("Internal Migration for import tables Case 2 Test 3 : ", "1", "2", "1_1"),

		//dList => 0_1_1 0_2 1 2 3 4 5
		Entry("Internal Migration for import tables Case 3 Test 1 : ", "0_1_1", "0_2", "0_1_2"),
		Entry("Internal Migration for import tables Case 3 Test 2 : ", "0_2", "1", "0_2_1"),

		//dList => 0_1_1 0_2_1 1 2 3 4 5
		Entry("Internal Migration for import tables Case 4 Test 1 : ", "0_2_1", "1", "0_2_2"),
		Entry("Internal Migration for import tables Case 4 Test 2 : ", "0_1_1", "0_2_1", "0_1_2"),

		//dList => 0_1 0_2_1 1 2 3
		Entry("Internal Migration for import tables Case 5 Test 1 : ", "0_1", "0_2_1", "0_1_1"),

		Entry("OrderTest Case 1 Test 1 : ", "9", "10", "9_1"),
	)
})

var _ = Describe("Calculate newDSIdx for cluster migrations", func() {
	var _ = DescribeTable("newDSIdx tests",
		func(dList []dataSetT, after dataSetT, expected string) {
			computedIdx, err := computeIdxForClusterMigration("table_prefix", dList, after)
			Expect(computedIdx).To(Equal(expected))
			Expect(err).To(BeNil())
		},

		Entry("ClusterMigration Case 1",
			[]dataSetT{
				dataSetT{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1",
				},
			},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1",
			}, "0_1"),

		Entry("ClusterMigration Case 2",
			[]dataSetT{
				dataSetT{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "0_1",
				},
				dataSetT{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1",
				},
				dataSetT{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "2",
				},
			},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1",
			}, "0_2"),
	)

	var _ = DescribeTable("Error cases",
		func(dList []dataSetT, after dataSetT) {
			_, err := computeIdxForClusterMigration("table_prefix", dList, after)
			Expect(err != nil).Should(BeTrue())
		},

		Entry("ClusterMigration Case 1",
			[]dataSetT{
				dataSetT{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1_1",
				},
			},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1_1",
			},
		),

		Entry("ClusterMigration Case 2",
			[]dataSetT{
				dataSetT{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1",
				},
				dataSetT{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1_1",
				},
			},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1_1",
			},
		),

		Entry("ClusterMigration Case 4",
			[]dataSetT{},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1_1",
			},
		),

		Entry("ClusterMigration Case 5",
			[]dataSetT{},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1_1_1_1",
			},
		),

		Entry("ClusterMigration Case 6",
			[]dataSetT{},
			dataSetT{
				JobTable:       "",
				JobStatusTable: "",
				Index:          "1_1_!_1",
			},
		),
	)
})

var sampleTestJob = JobT{
	Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
	EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
	UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
	UUID:         uuid.NewV4(),
	CustomVal:    "MOCKDS",
}

type tContext struct {
	mock       sqlmock.Sqlmock
	db         *sql.DB
	globalMock sqlmock.Sqlmock
	globalDB   *sql.DB
}

func (c *tContext) Setup() {
	c.db, c.mock, _ = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	c.globalDB, c.globalMock, _ = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
}

func (c *tContext) Finish() {
	c.db.Close()
}

var _ = Describe("jobsdb", func() {

	var c *tContext

	BeforeEach(func() {
		c = &tContext{}
		c.Setup()

		// setup static requirements of dependencies
		stats.Setup()

		globalDBHandle = c.globalDB
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("getDSList", func() {
		var jd *HandleT

		BeforeEach(func() {
			jd = &HandleT{}
			jd.dbHandle = c.db

			jd.workersAndAuxSetup(ReadWrite, "tt", 0*time.Hour, "", false, QueryFiltersT{})
		})

		It("doesn't make db calls if !refreshFromDB", func() {
			jd.datasetList = dsListInMemory

			Expect(jd.getDSList(false)).To(Equal(dsListInMemory))

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})

		It("makes some db calls if refreshFromDB", func() {
			//Prepare and execute. Note that tables in DB is different from that in memory.
			c.mock.ExpectPrepare(`SELECT tablename
			FROM pg_catalog.pg_tables
			WHERE schemaname != 'pg_catalog' AND
			schemaname != 'information_schema'`).ExpectQuery().WillReturnRows(mockRows())

			Expect(jd.getDSList(true)).To(Equal(dsListInDB))

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})
	})

	Context("Store", func() {
		var jd *HandleT
		var ds dataSetT

		BeforeEach(func() {
			jd = &HandleT{}
			jd.dbHandle = c.db
			jd.datasetList = dsListInMemory
			jd.enableWriterQueue = false
			jd.workersAndAuxSetup(ReadWrite, "tt", 0*time.Hour, "", false, QueryFiltersT{})

			ds = jd.datasetList[len(jd.datasetList)-1]
		})

		It("should store jobs to db through workers", func() {
			jd.enableWriterQueue = true

			c.mock.ExpectBegin()
			stmt := c.mock.ExpectPrepare(fmt.Sprintf(`COPY "%s" ("uuid", "user_id", "custom_val", "parameters", "event_payload") FROM STDIN`, ds.JobTable))
			for _, job := range mockedStoreJobs {
				stmt.ExpectExec().WithArgs(job.UUID, job.UserID, job.CustomVal, string(job.Parameters), string(job.EventPayload)).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			stmt.ExpectExec().WithArgs().WillReturnResult(sqlmock.NewResult(0, int64(len(mockedStoreJobs))))
			c.mock.ExpectCommit()

			err := jd.Store(mockedStoreJobs)
			Expect(err).ShouldNot(HaveOccurred())

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})
		It("should store jobs to db directly and not through workers", func() {
			c.mock.ExpectBegin()
			stmt := c.mock.ExpectPrepare(fmt.Sprintf(`COPY "%s" ("uuid", "user_id", "custom_val", "parameters", "event_payload") FROM STDIN`, ds.JobTable))
			for _, job := range mockedStoreJobs {
				stmt.ExpectExec().WithArgs(job.UUID, job.UserID, job.CustomVal, string(job.Parameters), string(job.EventPayload)).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			stmt.ExpectExec().WithArgs().WillReturnResult(sqlmock.NewResult(0, int64(len(mockedStoreJobs))))
			c.mock.ExpectCommit()

			err := jd.Store(mockedStoreJobs)
			Expect(err).ShouldNot(HaveOccurred())

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})
		It("should return error if prepare fails", func() {
			c.mock.ExpectBegin().WillReturnError(errors.New("failed to prepare"))

			err := jd.Store(mockedStoreJobs)
			Expect(err).To(Equal(errors.New("failed to prepare")))

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})
	})

	Context("StoreWithRetryEach", func() {
		var jd *HandleT
		var ds dataSetT

		BeforeEach(func() {
			jd = &HandleT{}
			jd.dbHandle = c.db
			jd.datasetList = dsListInMemory
			jd.enableWriterQueue = true
			jd.workersAndAuxSetup(ReadWrite, "tt", 0*time.Hour, "", false, QueryFiltersT{})

			ds = jd.datasetList[len(jd.datasetList)-1]
		})

		It("should store jobs to db with storeJobsDS", func() {
			c.mock.ExpectBegin()
			stmt := c.mock.ExpectPrepare(fmt.Sprintf(`COPY "%s" ("uuid", "user_id", "custom_val", "parameters", "event_payload") FROM STDIN`, ds.JobTable))
			for _, job := range mockedStoreJobs {
				stmt.ExpectExec().WithArgs(job.UUID, job.UserID, job.CustomVal, string(job.Parameters), string(job.EventPayload)).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			stmt.ExpectExec().WithArgs().WillReturnResult(sqlmock.NewResult(0, int64(len(mockedStoreJobs))))
			c.mock.ExpectCommit()

			errorMessagesMap := jd.StoreWithRetryEach(mockedStoreJobs)
			Expect(errorMessagesMap).To(BeEmpty())

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})
		It("should store jobs to db even when bulk store(storeJobsDS) returns error", func() {
			c.mock.ExpectBegin().WillReturnError(errors.New("failed to prepare"))

			for _, job := range mockedStoreJobs {
				stmt := c.mock.ExpectPrepare(fmt.Sprintf(`INSERT INTO %s (uuid, user_id, custom_val, parameters, event_payload)
			VALUES ($1, $2, $3, $4, (regexp_replace($5::text, '\\u0000', '', 'g'))::json) RETURNING job_id`, ds.JobTable))
				stmt.ExpectExec().WithArgs(job.UUID, job.UserID, job.CustomVal, string(job.Parameters), string(job.EventPayload)).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			errorMessagesMap := jd.StoreWithRetryEach(mockedStoreJobs)
			Expect(errorMessagesMap).To(BeEmpty())

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})
		It("should store jobs partially because one job has invalid json payload", func() {
			c.mock.ExpectBegin().WillReturnError(errors.New("failed to prepare"))

			job1 := mockedPartiallyStoredJobs[0]
			job2 := mockedPartiallyStoredJobs[1]
			stmt := c.mock.ExpectPrepare(fmt.Sprintf(`INSERT INTO %s (uuid, user_id, custom_val, parameters, event_payload)
			VALUES ($1, $2, $3, $4, (regexp_replace($5::text, '\\u0000', '', 'g'))::json) RETURNING job_id`, ds.JobTable))
			stmt.ExpectExec().WithArgs(job1.UUID, job1.UserID, job1.CustomVal, string(job1.Parameters), string(job1.EventPayload)).WillReturnResult(sqlmock.NewResult(0, 1))
			stmt = c.mock.ExpectPrepare(fmt.Sprintf(`INSERT INTO %s (uuid, user_id, custom_val, parameters, event_payload)
			VALUES ($1, $2, $3, $4, (regexp_replace($5::text, '\\u0000', '', 'g'))::json) RETURNING job_id`, ds.JobTable))
			err := &pq.Error{}
			err.Code = "22P02" //Invalid JSON syntax
			stmt.ExpectExec().WithArgs(job2.UUID, job2.UserID, job2.CustomVal, string(job2.Parameters), string(job2.EventPayload)).WillReturnError(err)

			errorMessagesMap := jd.StoreWithRetryEach(mockedPartiallyStoredJobs)
			Expect(errorMessagesMap).To(Equal(map[uuid.UUID]string{s: "Invalid JSON"}))

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})
	})

	Context("UpdateJobStatus", func() {
		var jd *HandleT

		BeforeEach(func() {
			jd = &HandleT{}
			jd.dbHandle = c.db
			jd.datasetList = dsListInMemory
			jd.datasetRangeList = dsRangeList
			jd.enableWriterQueue = true
			jd.workersAndAuxSetup(ReadWrite, "tt", 0*time.Hour, "", false, QueryFiltersT{})
		})

		It("should update job statuses to db", func() {
			c.mock.ExpectBegin()

			ds := dsListInMemory[0]
			stmt := c.mock.ExpectPrepare(fmt.Sprintf(`COPY "%s" ("job_id", "job_state", "attempt", "exec_time",
			"retry_time", "error_code", "error_response", "parameters") FROM STDIN`, ds.JobStatusTable))
			status := statusList[0]
			stmt.ExpectExec().WithArgs(status.JobID, status.JobState, status.AttemptNum, status.ExecTime,
				status.RetryTime, status.ErrorCode, string(status.ErrorResponse), string(status.Parameters)).WillReturnResult(sqlmock.NewResult(0, 1))
			stmt.ExpectExec().WithArgs().WillReturnResult(sqlmock.NewResult(0, int64(len(mockedStoreJobs))))

			ds = dsListInMemory[1]
			stmt = c.mock.ExpectPrepare(fmt.Sprintf(`COPY "%s" ("job_id", "job_state", "attempt", "exec_time",
			"retry_time", "error_code", "error_response", "parameters") FROM STDIN`, ds.JobStatusTable))
			status = statusList[1]
			stmt.ExpectExec().WithArgs(status.JobID, status.JobState, status.AttemptNum, status.ExecTime,
				status.RetryTime, status.ErrorCode, string(status.ErrorResponse), string(status.Parameters)).WillReturnResult(sqlmock.NewResult(0, 1))
			stmt.ExpectExec().WithArgs().WillReturnResult(sqlmock.NewResult(0, int64(len(mockedStoreJobs))))

			c.mock.ExpectCommit()

			err := jd.UpdateJobStatus(statusList, []string{"MOCKDS"}, nil)
			Expect(err).ShouldNot(HaveOccurred())

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})

		It("should fail to update job status", func() {
			c.mock.ExpectBegin()

			ds := dsListInMemory[0]
			stmt := c.mock.ExpectPrepare(fmt.Sprintf(`COPY "%s" ("job_id", "job_state", "attempt", "exec_time",
			"retry_time", "error_code", "error_response", "parameters") FROM STDIN`, ds.JobStatusTable))
			status := statusList[0]
			stmt.ExpectExec().WithArgs(status.JobID, status.JobState, status.AttemptNum, status.ExecTime,
				status.RetryTime, status.ErrorCode, string(status.ErrorResponse), string(status.Parameters)).WillReturnResult(sqlmock.NewResult(0, 1))
			stmt.ExpectExec().WithArgs().WillReturnResult(sqlmock.NewResult(0, int64(len(mockedStoreJobs))))

			ds = dsListInMemory[1]
			stmt = c.mock.ExpectPrepare(fmt.Sprintf(`COPY "%s" ("job_id", "job_state", "attempt", "exec_time",
			"retry_time", "error_code", "error_response", "parameters") FROM STDIN`, ds.JobStatusTable))
			status = statusList[1]
			stmt.ExpectExec().WithArgs(status.JobID, status.JobState, status.AttemptNum, status.ExecTime,
				status.RetryTime, status.ErrorCode, string(status.ErrorResponse), string(status.Parameters)).WillReturnResult(sqlmock.NewResult(0, 1))
			stmt.ExpectExec().WithArgs().WillReturnError(errors.New("exec failed"))

			c.mock.ExpectRollback()

			err := jd.UpdateJobStatus(statusList, []string{"MOCKDS"}, nil)
			Expect(err).To(Equal(errors.New("exec failed")))

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})
	})

	Context("GetProcesed", func() {
		var jd *HandleT

		BeforeEach(func() {
			jd = &HandleT{}
			jd.dbHandle = c.db
			jd.datasetList = dsListInMemory
			jd.datasetRangeList = dsRangeList
			jd.enableWriterQueue = true
			jd.workersAndAuxSetup(ReadWrite, "tt", 0*time.Hour, "", false, QueryFiltersT{})
		})

		assertGetProcessedJobsWithCustomVal := func(state string) {
			It("should return jobs with non terminal last state when queried with customval", func() {
				var stateQuery, customValQuery, sourceQuery, limitQuery string
				stateQuery = " AND ((job_state='failed'))"
				customValQuery = " AND ((tt_jobs_1.custom_val='MOCKDS'))"
				limitQuery = " LIMIT 2 "
				timeNow := time.Now()

				getTimeNowFunc = func() time.Time {
					return timeNow
				}

				ds := dsListInMemory[0]
				stmt := fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at, job_latest_state.job_state, job_latest_state.attempt, job_latest_state.exec_time, job_latest_state.retry_time, job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters FROM %[1]s, (SELECT job_id, job_state, attempt, exec_time, retry_time, error_code, error_response, parameters FROM %[2]s WHERE id IN (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s) AS job_latest_state WHERE %[1]s.job_id=job_latest_state.job_id %[4]s %[5]s AND job_latest_state.retry_time < $1 ORDER BY %[1]s.job_id %[6]s`,
					ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery)
				c.mock.ExpectPrepare(stmt).
					ExpectQuery().WithArgs(timeNow).WillReturnRows(mockJobsForState(ds, state, 1))

				customValQuery = " AND ((tt_jobs_2.custom_val='MOCKDS'))"
				limitQuery = " LIMIT 1 "
				ds = dsListInMemory[1]
				stmt = fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at, job_latest_state.job_state, job_latest_state.attempt, job_latest_state.exec_time, job_latest_state.retry_time, job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters FROM %[1]s, (SELECT job_id, job_state, attempt, exec_time, retry_time, error_code, error_response, parameters FROM %[2]s WHERE id IN (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s) AS job_latest_state WHERE %[1]s.job_id=job_latest_state.job_id %[4]s %[5]s AND job_latest_state.retry_time < $1 ORDER BY %[1]s.job_id %[6]s`,
					ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery)
				c.mock.ExpectPrepare(stmt).
					ExpectQuery().WithArgs(timeNow).WillReturnRows(mockJobsForState(ds, state, 1))

				jobs := jd.GetToRetry(GetQueryParamsT{CustomValFilters: []string{"MOCKDS"}, Count: 2})
				Expect(len(jobs)).To(Equal(2))
				assertJobs(getJobsWithLastState(state), jobs)

				// we make sure that all expectations were met
				if err := c.mock.ExpectationsWereMet(); err != nil {
					ginkgo.Fail("there were unfulfilled expectations")
				}
			})
		}

		nonTerminalStates := []string{"executing", "failed", "waiting", "throttled", "importing"}
		for _, nonTerminalState := range nonTerminalStates {
			assertGetProcessedJobsWithCustomVal(nonTerminalState)
		}

		assertGetProcessedJobsWithParameters := func(state string) {
			It("should return jobs with non terminal last state when queried with parameters", func() {
				destinationID := "someDestID"
				var stateQuery, customValQuery, sourceQuery, limitQuery string
				stateQuery = " AND ((job_state='failed'))"
				customValQuery = " AND ((tt_jobs_1.custom_val='MOCKDS'))"
				limitQuery = " LIMIT 2 "
				sourceQuery = fmt.Sprintf(` AND (tt_jobs_1.parameters @> '{"destination_id":"%s"}' )`, destinationID)
				timeNow := time.Now()

				getTimeNowFunc = func() time.Time {
					return timeNow
				}

				ds := dsListInMemory[0]
				stmt := fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at, job_latest_state.job_state, job_latest_state.attempt, job_latest_state.exec_time, job_latest_state.retry_time, job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters FROM %[1]s, (SELECT job_id, job_state, attempt, exec_time, retry_time, error_code, error_response, parameters FROM %[2]s WHERE id IN (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s) AS job_latest_state WHERE %[1]s.job_id=job_latest_state.job_id %[4]s %[5]s AND job_latest_state.retry_time < $1 ORDER BY %[1]s.job_id %[6]s`,
					ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery)
				c.mock.ExpectPrepare(stmt).
					ExpectQuery().WithArgs(timeNow).WillReturnRows(mockJobsForState(ds, state, 1))

				customValQuery = " AND ((tt_jobs_2.custom_val='MOCKDS'))"
				limitQuery = " LIMIT 1 "
				sourceQuery = fmt.Sprintf(` AND (tt_jobs_2.parameters @> '{"destination_id":"%s"}' )`, destinationID)
				ds = dsListInMemory[1]
				stmt = fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at, job_latest_state.job_state, job_latest_state.attempt, job_latest_state.exec_time, job_latest_state.retry_time, job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters FROM %[1]s, (SELECT job_id, job_state, attempt, exec_time, retry_time, error_code, error_response, parameters FROM %[2]s WHERE id IN (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s) AS job_latest_state WHERE %[1]s.job_id=job_latest_state.job_id %[4]s %[5]s AND job_latest_state.retry_time < $1 ORDER BY %[1]s.job_id %[6]s`,
					ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery)
				c.mock.ExpectPrepare(stmt).
					ExpectQuery().WithArgs(timeNow).WillReturnRows(mockJobsForState(ds, state, 1))

				parameterFilters := []ParameterFilterT{
					{
						Name:     "destination_id",
						Value:    destinationID,
						Optional: false,
					},
				}

				jobs := jd.GetToRetry(GetQueryParamsT{CustomValFilters: []string{"MOCKDS"}, ParameterFilters: parameterFilters, Count: 2})
				Expect(len(jobs)).To(Equal(2))
				assertJobs(getJobsWithLastState(state), jobs)

				// we make sure that all expectations were met
				if err := c.mock.ExpectationsWereMet(); err != nil {
					ginkgo.Fail("there were unfulfilled expectations")
				}
			})
		}

		for _, nonTerminalState := range nonTerminalStates {
			assertGetProcessedJobsWithParameters(nonTerminalState)
		}
	})

	Context("GetUnProcesed", func() {
		var jd *HandleT

		BeforeEach(func() {
			jd = &HandleT{}
			jd.dbHandle = c.db
			jd.datasetList = dsListInMemory
			jd.datasetRangeList = dsRangeList
			jd.enableWriterQueue = true
			jd.workersAndAuxSetup(ReadWrite, "tt", 0*time.Hour, "", false, QueryFiltersT{})
		})

		It("should return unprocessed jobs with customval", func() {
			var customValQuery, sourceQuery, limitQuery, orderQuery string
			customValQuery = " AND ((tt_jobs_1.custom_val='MOCKDS'))"
			limitQuery = " LIMIT 2 "
			orderQuery = " ORDER BY tt_jobs_1.job_id"
			timeNow := time.Now()

			getTimeNowFunc = func() time.Time {
				return timeNow
			}

			ds := dsListInMemory[0]
			stmt := fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at FROM %[1]s LEFT JOIN %[2]s ON %[1]s.job_id=%[2]s.job_id WHERE %[2]s.job_id is NULL`,
				ds.JobTable, ds.JobStatusTable)
			stmt = stmt + customValQuery + sourceQuery + orderQuery + limitQuery
			c.mock.ExpectQuery(stmt).WillReturnRows(mockUnprocessedJobs(ds, 1))

			customValQuery = " AND ((tt_jobs_2.custom_val='MOCKDS'))"
			limitQuery = " LIMIT 1 "
			orderQuery = " ORDER BY tt_jobs_2.job_id"
			ds = dsListInMemory[1]
			stmt = fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at FROM %[1]s LEFT JOIN %[2]s ON %[1]s.job_id=%[2]s.job_id WHERE %[2]s.job_id is NULL`,
				ds.JobTable, ds.JobStatusTable)
			stmt = stmt + customValQuery + sourceQuery + orderQuery + limitQuery
			c.mock.ExpectQuery(stmt).WillReturnRows(mockUnprocessedJobs(ds, 1))

			jobs := jd.GetUnprocessed(GetQueryParamsT{CustomValFilters: []string{"MOCKDS"}, Count: 2})
			Expect(len(jobs)).To(Equal(2))
			assertJobs(getJobsWithLastState(""), jobs)

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})

		It("should return unprocessed jobs with parameters", func() {
			destinationID := "someDestID"
			var customValQuery, sourceQuery, limitQuery, orderQuery string
			customValQuery = " AND ((tt_jobs_1.custom_val='MOCKDS'))"
			sourceQuery = fmt.Sprintf(` AND (tt_jobs_1.parameters @> '{"destination_id":"%s"}' )`, destinationID)
			limitQuery = " LIMIT 2 "
			orderQuery = " ORDER BY tt_jobs_1.job_id"
			timeNow := time.Now()

			getTimeNowFunc = func() time.Time {
				return timeNow
			}

			ds := dsListInMemory[0]
			stmt := fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at FROM %[1]s LEFT JOIN %[2]s ON %[1]s.job_id=%[2]s.job_id WHERE %[2]s.job_id is NULL`,
				ds.JobTable, ds.JobStatusTable)
			stmt = stmt + customValQuery + sourceQuery + orderQuery + limitQuery
			c.mock.ExpectQuery(stmt).WillReturnRows(mockUnprocessedJobs(ds, 1))

			customValQuery = " AND ((tt_jobs_2.custom_val='MOCKDS'))"
			sourceQuery = fmt.Sprintf(` AND (tt_jobs_2.parameters @> '{"destination_id":"%s"}' )`, destinationID)
			limitQuery = " LIMIT 1 "
			orderQuery = " ORDER BY tt_jobs_2.job_id"
			ds = dsListInMemory[1]
			stmt = fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at FROM %[1]s LEFT JOIN %[2]s ON %[1]s.job_id=%[2]s.job_id WHERE %[2]s.job_id is NULL`,
				ds.JobTable, ds.JobStatusTable)
			stmt = stmt + customValQuery + sourceQuery + orderQuery + limitQuery
			c.mock.ExpectQuery(stmt).WillReturnRows(mockUnprocessedJobs(ds, 1))

			parameterFilters := []ParameterFilterT{
				{
					Name:     "destination_id",
					Value:    destinationID,
					Optional: false,
				},
			}

			jobs := jd.GetUnprocessed(GetQueryParamsT{CustomValFilters: []string{"MOCKDS"}, ParameterFilters: parameterFilters, Count: 2})
			Expect(len(jobs)).To(Equal(2))
			assertJobs(getJobsWithLastState(""), jobs)

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})
	})

	Context("DeleteExecuting", func() {
		var jd *HandleT

		BeforeEach(func() {
			jd = &HandleT{}
			jd.dbHandle = c.db
			jd.datasetList = dsListInMemory
			jd.datasetRangeList = dsRangeList
			jd.enableWriterQueue = true
			jd.workersAndAuxSetup(ReadWrite, "tt", 0*time.Hour, "", false, QueryFiltersT{})
		})

		It("should delete only one executing with simple customVal", func() {
			timeNow := time.Now()
			getTimeNowFunc = func() time.Time {
				return timeNow
			}

			c.mock.ExpectBegin()

			ds := dsListInMemory[0]
			customValQuery := "tt_jobs_1.custom_val='MOCKDEST'"
			stmt := c.mock.ExpectPrepare(fmt.Sprintf(`DELETE FROM %[1]s WHERE id IN (SELECT MAX(id) from %[1]s where job_id IN (SELECT job_id from %[2]s WHERE ((%[3]s)) ) GROUP BY job_id)  AND ((job_state='executing')) AND retry_time < $1`, ds.JobStatusTable, ds.JobTable, customValQuery))
			stmt.ExpectExec().WithArgs(timeNow).WillReturnResult(sqlmock.NewResult(0, 1))

			c.mock.ExpectCommit()

			jd.DeleteExecuting(GetQueryParamsT{CustomValFilters: []string{"MOCKDEST"}, Count: 1})

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})

		It("should delete only one executing with simple customVal and a destID", func() {
			destinationID := "dummy_dest_id"
			timeNow := time.Now()
			getTimeNowFunc = func() time.Time {
				return timeNow
			}

			parameterFilters := []ParameterFilterT{
				{
					Name:     "destination_id",
					Value:    destinationID,
					Optional: false,
				},
			}

			c.mock.ExpectBegin()

			ds := dsListInMemory[0]
			customValQuery := "tt_jobs_1.custom_val='MOCKDEST'"
			sourceQuery := fmt.Sprintf(`tt_jobs_1.parameters @> '{"destination_id":"%s"}' `, destinationID)
			prepareStatement := fmt.Sprintf(`DELETE FROM %[1]s WHERE id IN (SELECT MAX(id) from %[1]s where job_id IN (SELECT job_id from %[2]s WHERE ((%[3]s)) AND (%[4]s)) GROUP BY job_id)  AND ((job_state='executing')) AND retry_time < $1`, ds.JobStatusTable, ds.JobTable, customValQuery, sourceQuery)
			stmt := c.mock.ExpectPrepare(prepareStatement)
			stmt.ExpectExec().WithArgs(timeNow).WillReturnResult(sqlmock.NewResult(0, 1))

			c.mock.ExpectCommit()

			jd.DeleteExecuting(GetQueryParamsT{CustomValFilters: []string{"MOCKDEST"}, ParameterFilters: parameterFilters, Count: 1})

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})

		It("should delete executing with simple customVal", func() {
			timeNow := time.Now()
			getTimeNowFunc = func() time.Time {
				return timeNow
			}

			c.mock.ExpectBegin()

			ds := dsListInMemory[0]
			customValQuery := "tt_jobs_1.custom_val='MOCKDEST'"
			stmt := c.mock.ExpectPrepare(fmt.Sprintf(`DELETE FROM %[1]s WHERE id IN (SELECT MAX(id) from %[1]s where job_id IN (SELECT job_id from %[2]s WHERE ((%[3]s)) ) GROUP BY job_id)  AND ((job_state='executing')) AND retry_time < $1`, ds.JobStatusTable, ds.JobTable, customValQuery))
			stmt.ExpectExec().WithArgs(timeNow).WillReturnResult(sqlmock.NewResult(0, 1))

			ds = dsListInMemory[1]
			customValQuery = "tt_jobs_2.custom_val='MOCKDEST'"
			stmt = c.mock.ExpectPrepare(fmt.Sprintf(`DELETE FROM %[1]s WHERE id IN (SELECT MAX(id) from %[1]s where job_id IN (SELECT job_id from %[2]s WHERE ((%[3]s)) ) GROUP BY job_id)  AND ((job_state='executing')) AND retry_time < $1`, ds.JobStatusTable, ds.JobTable, customValQuery))
			stmt.ExpectExec().WithArgs(timeNow).WillReturnResult(sqlmock.NewResult(0, 1))

			c.mock.ExpectCommit()

			jd.DeleteExecuting(GetQueryParamsT{CustomValFilters: []string{"MOCKDEST"}, Count: -1})

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})

		It("should delete executing with simple customVal and a destID", func() {
			destinationID := "dummy_dest_id"
			timeNow := time.Now()
			getTimeNowFunc = func() time.Time {
				return timeNow
			}

			parameterFilters := []ParameterFilterT{
				{
					Name:     "destination_id",
					Value:    destinationID,
					Optional: false,
				},
			}

			c.mock.ExpectBegin()

			ds := dsListInMemory[0]
			customValQuery := "tt_jobs_1.custom_val='MOCKDEST'"
			sourceQuery := fmt.Sprintf(`tt_jobs_1.parameters @> '{"destination_id":"%s"}' `, destinationID)
			prepareStatement := fmt.Sprintf(`DELETE FROM %[1]s WHERE id IN (SELECT MAX(id) from %[1]s where job_id IN (SELECT job_id from %[2]s WHERE ((%[3]s)) AND (%[4]s)) GROUP BY job_id)  AND ((job_state='executing')) AND retry_time < $1`, ds.JobStatusTable, ds.JobTable, customValQuery, sourceQuery)
			stmt := c.mock.ExpectPrepare(prepareStatement)
			stmt.ExpectExec().WithArgs(timeNow).WillReturnResult(sqlmock.NewResult(0, 1))

			ds = dsListInMemory[1]
			customValQuery = "tt_jobs_2.custom_val='MOCKDEST'"
			sourceQuery = fmt.Sprintf(`tt_jobs_2.parameters @> '{"destination_id":"%s"}' `, destinationID)
			prepareStatement = fmt.Sprintf(`DELETE FROM %[1]s WHERE id IN (SELECT MAX(id) from %[1]s where job_id IN (SELECT job_id from %[2]s WHERE ((%[3]s)) AND (%[4]s)) GROUP BY job_id)  AND ((job_state='executing')) AND retry_time < $1`, ds.JobStatusTable, ds.JobTable, customValQuery, sourceQuery)
			stmt = c.mock.ExpectPrepare(prepareStatement)
			stmt.ExpectExec().WithArgs(timeNow).WillReturnResult(sqlmock.NewResult(0, 1))

			c.mock.ExpectCommit()

			jd.DeleteExecuting(GetQueryParamsT{CustomValFilters: []string{"MOCKDEST"}, ParameterFilters: parameterFilters, Count: -1})

			// we make sure that all expectations were met
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail("there were unfulfilled expectations")
			}
		})
	})
})

func assertJobs(expected, actual []*JobT) {
	Expect(len(actual)).To(Equal(len(expected)))
	for i, job := range actual {
		actualJob := actual[i]
		Expect(job.JobID).To(Equal(actualJob.JobID))
		Expect(job.UserID).To(Equal(actualJob.UserID))
		Expect(job.UUID).To(Equal(actualJob.UUID))
		Expect(job.Parameters).To(Equal(actualJob.Parameters))
		Expect(job.EventPayload).To(Equal(actualJob.EventPayload))
		Expect(job.LastJobStatus.JobState).To(Equal(actualJob.LastJobStatus.JobState))
	}
}

var d1 = dataSetT{JobTable: "tt_jobs_1",
	JobStatusTable: "tt_job_status_1"}

var d2 = dataSetT{
	JobTable:       "tt_jobs_2",
	JobStatusTable: "tt_job_status_2",
}

var dsListInMemory = []dataSetT{
	d1,
	d2,
}

var dsRangeList = []dataSetRangeT{
	{
		minJobID:  int64(1),
		maxJobID:  int64(10),
		startTime: time.Now().UnixNano() / int64(time.Millisecond),
		endTime:   time.Now().UnixNano() / int64(time.Millisecond),
		ds:        d1,
	},
	{
		minJobID:  int64(10),
		maxJobID:  int64(20),
		startTime: time.Now().UnixNano() / int64(time.Millisecond),
		endTime:   time.Now().UnixNano() / int64(time.Millisecond),
		ds:        d2,
	},
}

var dsListInDB = []dataSetT{
	{
		JobTable:       "tt_jobs_2",
		JobStatusTable: "tt_job_status_2",
		Index:          "2",
	},
	{
		JobTable:       "tt_jobs_3",
		JobStatusTable: "tt_job_status_3",
		Index:          "3",
	},
}

var gwDSListInDB = []dataSetT{
	{
		JobTable:       "gw_jobs_2",
		JobStatusTable: "gw_job_status_2",
		Index:          "2",
	},
	{
		JobTable:       "gw_jobs_3",
		JobStatusTable: "gw_job_status_3",
		Index:          "3",
	},
}

var tablesNamesInDB = []string{
	"tt_jobs_2", "tt_job_status_2", "tt_jobs_3", "tt_job_status_3",
}

var gwTableNamesIndb = []string{
	"gw_jobs_2", "gw_job_status_2", "gw_jobs_3", "gw_job_status_3",
}

var gwmockTables = func() *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{"tablename"})
	for _, row := range gwTableNamesIndb {
		sqlMockRows.AddRow(row)
	}
	return sqlMockRows
}

var mockRows = func() *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{"tablename"})
	for _, row := range tablesNamesInDB {
		sqlMockRows.AddRow(row)
	}
	return sqlMockRows
}

var mockJobs = []*JobT{
	{
		JobID:        1,
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.NewV4(),
		CustomVal:    "MOCKDS",
	},
	{
		JobID:        11,
		Parameters:   []byte(`{"batch_id":2,"source_id":"sourceID","source_job_run_id":"random_sourceJobRunID"}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "dummy_a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.NewV4(),
		CustomVal:    "MOCKDS",
	},
}

func getJobsWithLastState(state string) []*JobT {
	jobs := make([]*JobT, len(mockJobs))
	copier.Copy(&jobs, &mockJobs)

	if state != "" {
		for _, job := range jobs {
			job.LastJobStatus.JobState = state
			job.LastJobStatus.AttemptNum = 1
		}
	}

	return jobs
}

var mockUnprocessedJobs = func(ds dataSetT, count int) *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{fmt.Sprintf("%s.job_id", ds.JobTable),
		fmt.Sprintf("%s.uuid", ds.JobTable),
		fmt.Sprintf("%s.user_id", ds.JobTable),
		fmt.Sprintf("%s.parameters", ds.JobTable),
		fmt.Sprintf("%s.custom_val", ds.JobTable),
		fmt.Sprintf("%s.event_payload", ds.JobTable),
		fmt.Sprintf("%s.created_at", ds.JobTable),
		fmt.Sprintf("%s.expire_at", ds.JobTable),
	})

	for i, job := range mockJobs {
		if i >= count {
			break
		}
		sqlMockRows.AddRow(job.JobID, job.UUID, job.UserID, job.Parameters, job.CustomVal,
			job.EventPayload, job.CreatedAt, job.ExpireAt)
	}
	return sqlMockRows
}

var mockJobsForState = func(ds dataSetT, state string, count int) *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{fmt.Sprintf("%s.job_id", ds.JobTable),
		fmt.Sprintf("%s.uuid", ds.JobTable),
		fmt.Sprintf("%s.user_id", ds.JobTable),
		fmt.Sprintf("%s.parameters", ds.JobTable),
		fmt.Sprintf("%s.custom_val", ds.JobTable),
		fmt.Sprintf("%s.event_payload", ds.JobTable),
		fmt.Sprintf("%s.created_at", ds.JobTable),
		fmt.Sprintf("%s.expire_at", ds.JobTable),
		"job_latest_state.job_state",
		"job_latest_state.attempt",
		"job_latest_state.exec_time",
		"job_latest_state.retry_time",
		"job_latest_state.error_code",
		"job_latest_state.error_response",
		"job_latest_state.parameters",
	})

	for i, job := range getJobsWithLastState(state) {
		if i >= count {
			break
		}
		sqlMockRows.AddRow(job.JobID, job.UUID, job.UserID, job.Parameters, job.CustomVal,
			job.EventPayload, job.CreatedAt, job.ExpireAt, job.LastJobStatus.JobState, job.LastJobStatus.AttemptNum,
			job.LastJobStatus.ExecTime, job.LastJobStatus.RetryTime, job.LastJobStatus.ErrorCode, job.LastJobStatus.ErrorResponse, job.LastJobStatus.Parameters)
	}
	return sqlMockRows
}

var mockedStoreJobs = []*JobT{
	{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.NewV4(),
		CustomVal:    "MOCKDS",
	},
	{
		Parameters:   []byte(`{"batch_id":2,"source_id":"sourceID","source_job_run_id":"random_sourceJobRunID"}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "dummy_a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.NewV4(),
		CustomVal:    "MOCKDEST",
	},
	{
		Parameters:   []byte(`{}`),
		EventPayload: []byte(`{}`),
		UserID:       "",
		UUID:         uuid.NewV4(),
		CustomVal:    "MOCKDEST",
	},
}

var uuidStr = "a362c501-c38e-4aee-ae61-4a3b095ebcab"
var s, _ = uuid.FromString(uuidStr)
var mockedPartiallyStoredJobs = []*JobT{
	{
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.NewV4(),
		CustomVal:    "MOCKDS",
	},
	{
		Parameters:   []byte(`{"batch_id":2,"source_id":"sourceID","source_job_run_id":"random_sourceJobRunID"}`),
		EventPayload: []byte(`{receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "dummy_a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         s,
		CustomVal:    "MOCKDEST",
	},
}

var statusList = []*JobStatusT{
	{
		JobID:         1,
		JobState:      Succeeded.State,
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "200",
		ErrorResponse: []byte(`{}`),
		Parameters:    []byte(`{}`),
	},
	{
		JobID:         11,
		JobState:      Aborted.State,
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "400",
		ErrorResponse: []byte(`{}`),
		Parameters:    []byte(`{}`),
	},
}
