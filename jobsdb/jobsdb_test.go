package jobsdb

import (
	"database/sql"
	"fmt"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
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
	Parameters:   []byte(`{"batch_id":1,"source_id":"1rNMpysD4lTuzglyfmPzsmihAbK","source_job_run_id":""}`),
	EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"1rNMpxFxVdoaAdItcXTbVVWdonD","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
	UserID:       "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
	UUID:         uuid.NewV4(),
	CustomVal:    "GW",
}

type context struct{}

var _ = Describe("testing generic functions in jobsdb", func() {
	stats.Setup()
	var jd *HandleT
	var db *sql.DB
	var mock sqlmock.Sqlmock
	var err error
	BeforeEach(func() {
		jd = &HandleT{}
		jd.statTableCount = stats.NewStat(fmt.Sprintf("jobsdb.%s_tables_count", jd.tablePrefix), stats.GaugeType)
		db, mock, err = sqlmock.New()
		Expect(err).ShouldNot(HaveOccurred())
		jd.dbHandle = db
		jd.tablePrefix = "tt"
		jd.logger = pkgLogger.Child("tt")
	})
	AfterEach(func() {
		db.Close()
	})
	Context("getDSList unit test", func() {
		BeforeEach(func() {
			jd.datasetList = dsListInMemory
		})
		AfterEach(func() {
		})
		It("doesn't make db calls if !refreshFromDB", func() {
			mock.ExpectationsWereMet() //Not necessary. There's no work with db here.
			Expect(jd.getDSList(false)).To(Equal(dsListInMemory))
		})
		It("makes some db calls if refreshFromDB", func() {
			//Setting Expectations for the DB
			//Prepare and execute. Note that tables in DB is different from that in memory.
			mockgetAllTableNames(mock)
			Expect(jd.getDSList(true)).To(Equal(dsListInDB))
		})
	})
	Context("storeJobsDS", func() {
		var ds dataSetT
		BeforeEach(func() {
			ds = dataSetT{
				JobTable:       "tt_jobs_1",
				JobStatusTable: "tt_job_status_1",
				Index:          "1",
			}
		})
		It("should store to db with JobID and all", func() { //copyID = true
			mock.ExpectBegin()
			//storeJobsDSInTxn
			mock.ExpectPrepare(pq.CopyIn(ds.JobTable, "job_id", "uuid", "user_id", "custom_val", "parameters",
				"event_payload", "created_at", "expire_at")).ExpectExec().WithArgs(sqlmock.AnyArg())
			mock.ExpectCommit()
		})
	})
})

var dsListInMemory = []dataSetT{
	{
		JobTable:       "tt_jobs_1",
		JobStatusTable: "tt_job_status_1",
	},
	{
		JobTable:       "tt_jobs_2",
		JobStatusTable: "tt_job_status_2",
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

var tablesNamesInDB = []string{
	"tt_jobs_2", "tt_job_status_2", "tt_jobs_3", "tt_job_status_3",
}

var mockRows = func() *sqlmock.Rows {
	sqlMockRows := sqlmock.NewRows([]string{"tablename"})
	for _, row := range tablesNamesInDB {
		sqlMockRows.AddRow(row)
	}
	return sqlMockRows
}()

func mockgetAllTableNames(mock sqlmock.Sqlmock) {
	mock.ExpectPrepare(`SELECT tablename
			FROM pg_catalog.pg_tables
			WHERE schemaname != 'pg_catalog' AND
			schemaname != 'information_schema'`).ExpectQuery().WillReturnRows(mockRows)
}

type storeTest struct {
	testJob   JobT
	testError error
}

var properStoreJobs = []storeTest{
	{
		testJob: JobT{
			Parameters:   []byte(`{"batch_id":1,"source_id":"1rNMpysD4lTuzglyfmPzsmihAbK","source_job_run_id":""}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"1rNMpxFxVdoaAdItcXTbVVWdonD","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.NewV4(),
			CustomVal:    "GW",
		},
		testError: nil,
	},
	{
		testJob: JobT{
			Parameters:   []byte(`{"batch_id":2,"source_id":"1rNMpysD4lTuzglyfmPzsmihAbK","source_job_run_id":"random_sourceJobRunID"}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"1rNMpxFxVdoaAdItcXTbVVWdonD","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"90ca6da0-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "dummy_90ca6da0-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.NewV4(),
			CustomVal:    "WEBHOOK",
		},
		testError: nil,
	},
	{
		//this example was while trying for a job that would fail
		testJob: JobT{
			Parameters:   []byte(`{}`),
			EventPayload: []byte(`{}`),
			UserID:       "",
			// UUID:         uuid.NewV4(),
			UUID:      uuid.UUID{},
			CustomVal: "WEBHOOK",
		},
		testError: nil,
	},
}

var properStoreJobsQueryList = []interface{}{}

// var errorStoreJobs = []storeTest{}
