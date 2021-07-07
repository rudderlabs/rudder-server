package jobsdb

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"

	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/services/stats"
)

var _ = Describe("readonly_jobsdb", func() {
	var c *context

	BeforeEach(func() {
		c = &context{}
		c.Setup()
		stats.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("getDSList", func() {
		var jd *ReadonlyHandleT

		BeforeEach(func() {
			jd = &ReadonlyHandleT{}
			jd.DbHandle = c.db
			jd.tablePrefix = `tt`
			jd.logger = pkgLogger.Child("readonly-" + jd.tablePrefix)
		})

		It("gets list of all datasets", func() {
			c.mock.ExpectPrepare(`SELECT tablename
			FROM pg_catalog.pg_tables
			WHERE schemaname != 'pg_catalog' AND
			schemaname != 'information_schema'`).ExpectQuery().WillReturnRows(mockRows())
			Expect(jd.getDSList()).To(Equal(dsListInDB))

			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail(err.Error())
			}
		})
	})

	Context("GetUnprocessedCount", func() {
		var jd *ReadonlyHandleT

		BeforeEach(func() {
			jd = &ReadonlyHandleT{}
			jd.DbHandle = c.db
			jd.tablePrefix = `tt`
			jd.logger = pkgLogger.Child("readonly-" + jd.tablePrefix)
		})

		It("gets count of unprocessed jobs", func() {
			customValFilters := []string{"MOCKDS"}
			parameterFilters := []ParameterFilterT{
				{
					Name:     "source_id",
					Value:    "sourceID",
					Optional: false,
				},
			}

			c.mock.ExpectPrepare(`SELECT tablename
				FROM pg_catalog.pg_tables
				WHERE schemaname != 'pg_catalog' AND
				schemaname != 'information_schema'`).ExpectQuery().WillReturnRows(mockRows())

			ds := dsListInDB[0]
			c.mock.ExpectBegin()
			c.mock.ExpectPrepare(fmt.Sprintf(`LOCK TABLE %s IN ACCESS SHARE MODE;`, ds.JobStatusTable)).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
			c.mock.ExpectPrepare(fmt.Sprintf(`LOCK TABLE %s IN ACCESS SHARE MODE;`, ds.JobTable)).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s LEFT JOIN %[2]s ON %[1]s.job_id=%[2]s.job_id
											 WHERE %[2]s.job_id is NULL AND ((%[1]s.custom_val='MOCKDS')) AND (%[1]s.parameters @> '{"source_id":"sourceID"}' )`, ds.JobTable, ds.JobStatusTable)).WillReturnRows(mockCountRows())
			c.mock.ExpectCommit()

			ds = dsListInDB[1]
			c.mock.ExpectBegin()
			c.mock.ExpectPrepare(fmt.Sprintf(`LOCK TABLE %s IN ACCESS SHARE MODE;`, ds.JobStatusTable)).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
			c.mock.ExpectPrepare(fmt.Sprintf(`LOCK TABLE %s IN ACCESS SHARE MODE;`, ds.JobTable)).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT COUNT(*) FROM %[1]s LEFT JOIN %[2]s ON %[1]s.job_id=%[2]s.job_id
											 WHERE %[2]s.job_id is NULL AND ((%[1]s.custom_val='MOCKDS')) AND (%[1]s.parameters @> '{"source_id":"sourceID"}' )`, ds.JobTable, ds.JobStatusTable)).WillReturnRows(mockCountRows())
			c.mock.ExpectCommit()

			Expect(jd.GetUnprocessedCount(customValFilters, parameterFilters)).To(Equal(int64(2 * len(mockJobs))))

			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail(err.Error())
			}
		})
	})

	Context("getNonSucceededJobsCount", func() {
		var jd *ReadonlyHandleT

		BeforeEach(func() {
			jd = &ReadonlyHandleT{}
			jd.DbHandle = c.db
			jd.tablePrefix = `tt`
			jd.logger = pkgLogger.Child("readonly-" + jd.tablePrefix)
		})

		It("gets pending jobs count", func() {
			customValFilters := []string{"MOCKDS"}
			parameterFilters := []ParameterFilterT{
				{
					Name:     "source_id",
					Value:    "sourceID",
					Optional: false,
				},
			}

			c.mock.ExpectPrepare(`SELECT tablename
				FROM pg_catalog.pg_tables
				WHERE schemaname != 'pg_catalog' AND
				schemaname != 'information_schema'`).ExpectQuery().WillReturnRows(mockRows())

			ds := dsListInDB[0]
			c.mock.ExpectBegin()
			c.mock.ExpectPrepare(fmt.Sprintf(`LOCK TABLE %s IN ACCESS SHARE MODE;`, ds.JobStatusTable)).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
			c.mock.ExpectPrepare(fmt.Sprintf(`LOCK TABLE %s IN ACCESS SHARE MODE;`, ds.JobTable)).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT COUNT(%[1]s.job_id) FROM
			%[1]s,
			(SELECT job_id, retry_time FROM %[2]s WHERE id IN
				(SELECT MAX(id) from %[2]s GROUP BY job_id)  AND ((job_state='failed') OR (job_state='waiting') OR (job_state='throttled') OR (job_state='executing')))
			AS job_latest_state
		 WHERE %[1]s.job_id=job_latest_state.job_id
		   AND ((%[1]s.custom_val='MOCKDS'))  AND (%[1]s.parameters @> '{"source_id":"sourceID"}' )
		  AND job_latest_state.retry_time < $1`, ds.JobTable, ds.JobStatusTable)).WillReturnRows(mockCountRows())

			ds = dsListInDB[1]
			c.mock.ExpectBegin()
			c.mock.ExpectPrepare(fmt.Sprintf(`LOCK TABLE %s IN ACCESS SHARE MODE;`, ds.JobStatusTable)).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
			c.mock.ExpectPrepare(fmt.Sprintf(`LOCK TABLE %s IN ACCESS SHARE MODE;`, ds.JobTable)).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT COUNT(%[1]s.job_id) FROM
			%[1]s,
			(SELECT job_id, retry_time FROM %[2]s WHERE id IN
				(SELECT MAX(id) from %[2]s GROUP BY job_id)  AND ((job_state='failed') OR (job_state='waiting') OR (job_state='throttled') OR (job_state='executing')))
			AS job_latest_state
		 WHERE %[1]s.job_id=job_latest_state.job_id
		   AND ((%[1]s.custom_val='MOCKDS'))  AND (%[1]s.parameters @> '{"source_id":"sourceID"}' )
		  AND job_latest_state.retry_time < $1`, ds.JobTable, ds.JobStatusTable)).WillReturnRows(mockCountRows())
			Expect(jd.getNonSucceededJobsCount(customValFilters, parameterFilters)).To(Equal(int64(2 * len(mockJobs))))

			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail(err.Error())
			}
		})
	})

	Context("GetLatestFailedJobs", func() {
		var jd *ReadonlyHandleT

		BeforeEach(func() {
			jd = &ReadonlyHandleT{}
			jd.DbHandle = c.db
			jd.tablePrefix = `tt`
			jd.logger = pkgLogger.Child("readonly-" + jd.tablePrefix)
		})

		It("gets latest failed jobs; we're giving DS index, so no getDSList call", func() {
			ds := gwDSListInDB[0]
			sqlStatement := fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.user_id, %[1]s.custom_val,
					job_latest_state.exec_time,
					job_latest_state.error_code, job_latest_state.error_response
					FROM %[1]s,
					(SELECT job_id, job_state, attempt, exec_time, retry_time,error_code, error_response FROM %[2]s WHERE id IN
					(SELECT MAX(id) from %[2]s GROUP BY job_id) AND (job_state = 'failed'))
					AS job_latest_state
 					WHERE %[1]s.job_id=job_latest_state.job_id
  					`, ds.JobTable, ds.JobStatusTable)
			sqlStatement = sqlStatement + fmt.Sprintf(`AND %[1]s.custom_val = '%[2]s'`, ds.JobTable, "GW")
			sqlStatement = sqlStatement + fmt.Sprintf(`ORDER BY %[1]s.job_id desc LIMIT 5;`, ds.JobTable)
			c.mock.ExpectQuery(sqlStatement).WillReturnRows(mockReadOnlyJobs(ds, Failed.State, 5))
			failedJobs, err := jd.GetLatestFailedJobs("2:GW", "gw")
			Expect(err).To(BeNil())

			type LatestFailedJob struct {
				JobID         int64           `header:"JobID"`
				UserID        string          `header:"UserID"` //ENUM waiting, executing, succeeded, waiting_retry,  failed, aborted, migrating, migrated, wont_migrate
				CustomVal     string          `header:"CustomVal"`
				ExecTime      time.Time       `header:"ExecTime"`
				ErrorCode     string          `header:"ErrorCode"`
				ErrorResponse json.RawMessage `header:"ErrorResponse"`
			}
			for i, job := range mockJobs {
				failedJob := gjson.Get(failedJobs, "FailedNums."+fmt.Sprint(i)).String()
				var failedJobT LatestFailedJob
				err = json.Unmarshal([]byte(failedJob), &failedJobT)
				Expect(err).To(BeNil())
				Expect(failedJobT.JobID).To(Equal(job.JobID))
				Expect(failedJobT.UserID).To(Equal(job.UserID))
				Expect(failedJobT.CustomVal).To(Equal(job.CustomVal))
			}

			//Assert values in failedJobs equals those in mockJobs
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail(err.Error())
			}
		})
	})

	Context("GetJobIDStatus", func() {
		var jd *ReadonlyHandleT

		BeforeEach(func() {
			jd = &ReadonlyHandleT{}
			jd.DbHandle = c.db
			jd.tablePrefix = `tt`
			jd.logger = pkgLogger.Child("readonly-" + jd.tablePrefix)
		})

		It("goes over DSs and gets the ", func() {
			job_id := "11"
			c.mock.ExpectPrepare(`SELECT tablename
				FROM pg_catalog.pg_tables
				WHERE schemaname != 'pg_catalog' AND
				schemaname != 'information_schema'`).ExpectQuery().WillReturnRows(mockRows())

			ds := dsListInDB[0]
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, ds.JobTable)).WillReturnRows(maxminJobID())

			ds = dsListInDB[1]
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, ds.JobTable)).WillReturnRows(maxminJobID2())

			c.mock.ExpectQuery(fmt.Sprintf(`SELECT job_id, job_state, attempt, exec_time, retry_time,error_code, error_response FROM %[1]s WHERE job_id = %[2]s;`, ds.JobStatusTable, job_id)).WillReturnRows(jobStatusRow())

			jobStatus, err := jd.GetJobIDStatus(fmt.Sprint(job_id), "tt")
			Expect(err).To(BeNil())
			Expect(gjson.Get(jobStatus, "FailedStatusStats.0.JobID").Raw).To(Equal(job_id))
			Expect(gjson.Get(jobStatus, "FailedStatusStats.0.JobState").Str).To(Equal(statusList[1].JobState))
			Expect(gjson.Get(jobStatus, "FailedStatusStats.0.AttemptNum").Raw).To(Equal(fmt.Sprint(statusList[1].AttemptNum)))
			// Expect(gjson.Get(jobStatus, "FailedStatusStats.0.ExecTime")).To(Equal(statusList[1].ExecTime.String()))
			// Expect(gjson.Get(jobStatus, "FailedStatusStats.0.RetryTime").Str).To(Equal(fmt.Sprint(statusList[1].RetryTime)))
			Expect(gjson.Get(jobStatus, "FailedStatusStats.0.ErrorCode").Str).To(Equal(statusList[1].ErrorCode))
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail(err.Error())
			}
		})
	})

	Context("GetJobSummaryCount", func() {
		var jd *ReadonlyHandleT

		BeforeEach(func() {
			jd = &ReadonlyHandleT{}
			jd.DbHandle = c.db
			jd.tablePrefix = `gw`
			jd.logger = pkgLogger.Child("readonly-" + jd.tablePrefix)
		})

		It("gets job summary count", func() {
			// c.mock.ExpectPrepare(`SELECT tablename
			// FROM pg_catalog.pg_tables
			// WHERE schemaname != 'pg_catalog' AND
			// schemaname != 'information_schema'`).ExpectQuery().WillReturnRows(mockRows())

			ds := dsListInDB[0]
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT COUNT(*), 
     					gw_jobs_%[1]s.parameters->'source_id' as source, 
     					gw_jobs_%[1]s.custom_val ,gw_jobs_%[1]s.parameters->'destination_id' as destination, 
     					job_latest_state.job_state
						FROM gw_jobs_%[1]s 
     					LEFT JOIN 
      					(SELECT job_id, job_state, attempt, exec_time, retry_time,error_code, error_response FROM gw_job_status_%[1]s 
						WHERE id IN (SELECT MAX(id) from gw_job_status_%[1]s GROUP BY job_id)) AS job_latest_state
     					ON gw_jobs_%[1]s.job_id=job_latest_state.job_id GROUP BY job_latest_state.job_state,gw_jobs_%[1]s.parameters->'source_id',gw_jobs_%[1]s.parameters->'destination_id', gw_jobs_%[1]s.custom_val;`, ds.Index)).WillReturnRows(summaryRows())
			// fmt.Println(jd.GetJobSummaryCount("2:2", "gw"))
			jobSummary, err := jd.GetJobSummaryCount("2:2", "gw")
			Expect(err).To(BeNil())
			fmt.Println(jobSummary)

			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail(err.Error())
			}
		})

		//incomplete
		It("gets job summary count", func() {
			c.mock.ExpectPrepare(`SELECT tablename
			FROM pg_catalog.pg_tables
			WHERE schemaname != 'pg_catalog' AND
			schemaname != 'information_schema'`).ExpectQuery().WillReturnRows(mockRows())

			// ds := dsListInDB[0]
			// c.mock.ExpectQuery(fmt.Sprintf(`SELECT COUNT(*),
			// 			gw_jobs_%[1]s.parameters->'source_id' as source,
			// 			gw_jobs_%[1]s.custom_val ,gw_jobs_%[1]s.parameters->'destination_id' as destination,
			// 			job_latest_state.job_state
			// 			FROM gw_jobs_%[1]s
			// 			LEFT JOIN
			// 			(SELECT job_id, job_state, attempt, exec_time, retry_time,error_code, error_response FROM gw_job_status_%[1]s
			// 			WHERE id IN (SELECT MAX(id) from gw_job_status_%[1]s GROUP BY job_id)) AS job_latest_state
			// 			ON gw_jobs_%[1]s.job_id=job_latest_state.job_id GROUP BY job_latest_state.job_state,gw_jobs_%[1]s.parameters->'source_id',gw_jobs_%[1]s.parameters->'destination_id', gw_jobs_%[1]s.custom_val;`, ds.Index)).WillReturnRows(summaryRows())

			// ds = dsListInDB[1]
			// c.mock.ExpectQuery(fmt.Sprintf(`SELECT COUNT(*),
			// 			gw_jobs_%[1]s.parameters->'source_id' as source,
			// 			gw_jobs_%[1]s.custom_val ,gw_jobs_%[1]s.parameters->'destination_id' as destination,
			// 			job_latest_state.job_state
			// 			FROM gw_jobs_%[1]s
			// 			LEFT JOIN
			// 			(SELECT job_id, job_state, attempt, exec_time, retry_time,error_code, error_response FROM gw_job_status_%[1]s
			// 			WHERE id IN (SELECT MAX(id) from gw_job_status_%[1]s GROUP BY job_id)) AS job_latest_state
			// 			ON gw_jobs_%[1]s.job_id=job_latest_state.job_id GROUP BY job_latest_state.job_state,gw_jobs_%[1]s.parameters->'source_id',gw_jobs_%[1]s.parameters->'destination_id', gw_jobs_%[1]s.custom_val;`, ds.Index)).WillReturnRows(summaryRows())

			// fmt.Println(jd.GetJobSummaryCount("2:2", "gw"))
			jobSummary, err := jd.GetJobSummaryCount(":2", "gw")
			Expect(err).To(BeNil())
			fmt.Println(jobSummary)

			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail(err.Error())
			}
		})
	})

	Context("GetJobByID", func() {
		var jd *ReadonlyHandleT

		BeforeEach(func() {
			jd = &ReadonlyHandleT{}
			jd.DbHandle = c.db
			jd.tablePrefix = `tt`
			jd.logger = pkgLogger.Child("readonly-" + jd.tablePrefix)
		})

		It("gets the DSList, then searches over them for a job with a particular id", func() {
			job_id := 11
			c.mock.ExpectPrepare(`SELECT tablename
				FROM pg_catalog.pg_tables
				WHERE schemaname != 'pg_catalog' AND
				schemaname != 'information_schema'`).ExpectQuery().WillReturnRows(mockRows())

			ds := dsListInDB[0]
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, ds.JobTable)).WillReturnRows(maxminJobID())
			//Query for job details is skipped here because the id lies outside the min and max job_ids in this table

			ds = dsListInDB[1]
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, ds.JobTable)).WillReturnRows(maxminJobID2())
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT
		%[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload,
		%[1]s.created_at, %[1]s.expire_at,
		job_latest_state.job_state, job_latest_state.attempt,
		job_latest_state.exec_time, job_latest_state.retry_time,
		job_latest_state.error_code, job_latest_state.error_response
	FROM
		%[1]s
	LEFT JOIN 
		(SELECT job_id, job_state, attempt, exec_time, retry_time,
		error_code, error_response FROM %[2]s WHERE id IN
			(SELECT MAX(id) from %[2]s GROUP BY job_id))
		AS job_latest_state
	ON %[1]s.job_id=job_latest_state.job_id
	WHERE %[1]s.job_id = %[3]s;`, ds.JobTable, ds.JobStatusTable, fmt.Sprint(job_id))).WillReturnRows(JobbyIdRow(ds, "failed", 1))

			job, err := jd.GetJobByID(fmt.Sprint(job_id), "tt")
			Expect(err).To(BeNil())
			Expect(gjson.Get(job, "JobID").Raw).To(Equal(fmt.Sprint(mockJobs[1].JobID)))
			Expect(gjson.Get(job, "UUID").Str).To(Equal(fmt.Sprint(mockJobs[1].UUID)))
			Expect(gjson.Get(job, "CustomVal").Str).To(Equal(mockJobs[1].CustomVal))

			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail(err.Error())
			}
		})
	})

	Context("GetJobIDsForUser", func() {
		var jd *ReadonlyHandleT

		BeforeEach(func() {
			jd = &ReadonlyHandleT{}
			jd.DbHandle = c.db
			jd.tablePrefix = `tt`
			jd.logger = pkgLogger.Child("readonly-" + jd.tablePrefix)
		})

		It("get job IDs for a User", func() {
			job_id1 := "1"
			job_id2 := "20"
			userID := "dummy_a-292e-4e79-9880-f8009e0ae4a3"

			c.mock.ExpectPrepare(`SELECT tablename
				FROM pg_catalog.pg_tables
				WHERE schemaname != 'pg_catalog' AND
				schemaname != 'information_schema'`).ExpectQuery().WillReturnRows(mockRows())

			ds := dsListInDB[0]
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, ds.JobTable)).WillReturnRows(maxminJobID())
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT job_id FROM %[1]s WHERE job_id >= %[2]s AND job_id <= %[3]s AND user_id = '%[4]s';`, ds.JobTable, job_id1, job_id2, userID)).WillReturnRows(jobByUser())

			ds = dsListInDB[1]
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, ds.JobTable)).WillReturnRows(maxminJobID2())
			c.mock.ExpectQuery(fmt.Sprintf(`SELECT job_id FROM %[1]s WHERE job_id >= %[2]s AND job_id <= %[3]s AND user_id = '%[4]s';`, ds.JobTable, job_id1, job_id2, userID)).WillReturnRows(jobByUser2())

			expectedUserJobs, err := jd.GetJobIDsForUser([]string{"gw", "Jobs between JobID's of a User", job_id1, job_id2, userID})

			Expect(err).To(BeNil())
			Expect(expectedUserJobs).To(Equal(jobsForUser()))

			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail(err.Error())
			}
		})
	})

	Context("GetFailedStatusErrorCodeCountsByDestination", func() {
		var jd *ReadonlyHandleT

		BeforeEach(func() {
			jd = &ReadonlyHandleT{}
			jd.DbHandle = c.db
			jd.tablePrefix = `tt`
			jd.logger = pkgLogger.Child("readonly-" + jd.tablePrefix)
		})

		It("gets count of failed events per destination", func() {
			ds := dataSetT{
				JobTable:       "rt_jobs_1",
				JobStatusTable: "rt_job_status_1",
			}
			c.mock.ExpectQuery(fmt.Sprintf(`select count(*), a.error_code, a.custom_val, a.d from
			(select count(*), rt.job_id, st.error_code as error_code, rt.custom_val as custom_val,
				rt.parameters -> 'destination_id' as d from %[1]s rt inner join %[2]s st
				on st.job_id=rt.job_id where st.job_state in ('failed', 'aborted')
				group by rt.job_id, st.error_code, rt.custom_val, rt.parameters -> 'destination_id')
			as  a group by a.custom_val, a.error_code, a.d order by a.custom_val;`, ds.JobTable, ds.JobStatusTable)).WillReturnRows(failedStatusErrorCodeCountRows())

			failedCodecounts, err := jd.GetFailedStatusErrorCodeCountsByDestination([]string{"rt", "Error Code Count By Destination", "1"})
			Expect(err).To(BeNil())
			Expect(gjson.Get(failedCodecounts, "ErrorCodeCounts.0.Count").Raw).To(Equal(fmt.Sprint(1)))
			Expect(gjson.Get(failedCodecounts, "ErrorCodeCounts.0.ErrorCode").Str).To(Equal(fmt.Sprint(429)))
			Expect(gjson.Get(failedCodecounts, "ErrorCodeCounts.0.Destination").Str).To(Equal("MOCKDS"))
			Expect(gjson.Get(failedCodecounts, "ErrorCodeCounts.0.DestinationID").Str).To(Equal("destID"))

			// fmt.Println(jd.GetFailedStatusErrorCodeCountsByDestination([]string{"rt", "Error Code Count By Destination", "1"}))
			if err := c.mock.ExpectationsWereMet(); err != nil {
				ginkgo.Fail(err.Error())
			}
		})
	})
})

var failedStatusErrorCodeCountRows = func() *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{
		"count", "errorcode", "destination", "destinationID",
	})
	sqlMockRows.AddRow(
		1, 429, "MOCKDS", "destID",
	)
	return sqlMockRows
}

var userJobs = []*JobT{
	{
		JobID:        1,
		Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "dummy_a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.NewV4(),
		CustomVal:    "MOCKDS",
	},
	{
		JobID:        2,
		Parameters:   []byte(`{"batch_id":2,"source_id":"sourceID","source_job_run_id":"random_sourceJobRunID"}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "dummy_a-292e-4e79-9880-f8009e0ae4a3",
		UUID:         uuid.NewV4(),
		CustomVal:    "MOCKDS",
	},
	{
		JobID:        5,
		Parameters:   []byte(`{"batch_id":2,"source_id":"sourceID","source_job_run_id":"random_sourceJobRunID"}`),
		EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
		UserID:       "dummy_a-292e-4e79-9880-f8009e0ae4a3",
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

var jobsForUser = func() string {
	var userJobList string
	for _, userjob := range userJobs {
		userJobList += fmt.Sprint(userjob.JobID) + "\n"
	}
	return userJobList
}

var jobByUser2 = func() *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{
		"job_id",
	})
	sqlMockRows.AddRow(userJobs[len(userJobs)-1].JobID)
	return sqlMockRows
}

var jobByUser = func() *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{
		"job_id",
	})
	for _, job := range userJobs {
		if job.JobID <= 10 {
			sqlMockRows.AddRow(job.JobID)
		}
	}
	return sqlMockRows
}

var maxminJobID = func() *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{"min", "max"})
	sqlMockRows.AddRow(int32(1), int32(10))
	return sqlMockRows
}

var maxminJobID2 = func() *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{"min", "max"})
	sqlMockRows.AddRow(int32(11), int32(20))
	return sqlMockRows
}

var mockCountRows = func() *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{"jobcounts"})
	sqlMockRows.AddRow(len(mockJobs))
	return sqlMockRows
}

var mockReadOnlyJobs = func(ds dataSetT, state string, count int) *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{fmt.Sprintf("%s.job_id", ds.JobTable),
		fmt.Sprintf("%s.user_id", ds.JobTable),
		fmt.Sprintf("%s.custom_val", ds.JobTable),
		"job_latest_state.exec_time",
		"job_latest_state.error_code",
		"job_latest_state.error_response",
	})

	for i, job := range getJobsWithLastState(state) {
		if i >= count {
			break
		}
		sqlMockRows.AddRow(job.JobID, job.UserID, job.CustomVal,
			job.LastJobStatus.ExecTime, job.LastJobStatus.ErrorCode, job.LastJobStatus.ErrorResponse)
	}
	return sqlMockRows
}

var JobbyIdRow = func(ds dataSetT, state string, count int) *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{
		fmt.Sprintf("%s.job_id", ds.JobTable),
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
	})

	job := getJobsWithLastState(state)[1]
	sqlMockRows.AddRow(job.JobID, job.UUID, job.UserID, job.Parameters, job.CustomVal, job.EventPayload, job.CreatedAt, job.ExpireAt,
		job.LastJobStatus.JobState, job.LastJobStatus.AttemptNum, job.LastJobStatus.ExecTime, job.LastJobStatus.RetryTime, job.LastJobStatus.ErrorCode, job.LastJobStatus.ErrorResponse)
	return sqlMockRows
}

var jobStatusRow = func() *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{
		"job_id", "job_state", "attempt", "exec_time", "retry_time", "error_code", "error_response",
	})

	sqlMockRows.AddRow(
		statusList[1].JobID, statusList[1].JobState, statusList[1].AttemptNum, statusList[1].ExecTime,
		statusList[1].RetryTime, statusList[1].ErrorCode, statusList[1].ErrorResponse,
	)
	return sqlMockRows
}
var summaryRows = func() *sqlmock.Rows {
	sqlmockRows := sqlmock.NewRows([]string{
		"count", "source", "custom_val", "destination", "job_latest_state.job_state",
	})
	sqlmockRows.AddRow(
		3, "sourceID1", "GW", "destID1", Failed.State,
	)
	sqlmockRows.AddRow(
		590, "sourceID1", "GW", "destID1", Succeeded.State,
	)
	return sqlmockRows
}
