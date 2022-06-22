package jobsdb

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	uuid "github.com/gofrs/uuid"
	"github.com/minio/minio-go"
	"github.com/stretchr/testify/require"

	// . "github.com/onsi/ginkgo/v2"
	// . "github.com/onsi/gomega"
	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	hold            bool
	DB_DSN          = "root@tcp(127.0.0.1:3306)/service"
	minioEndpoint   string
	bucket          = "backup-test"
	region          = "us-east-1"
	accessKeyId     = "MYACCESSKEY"
	secretAccessKey = "MYSECRETKEY"

	// db     *sql.DB
)

// var _ = Describe("Calculate newDSIdx for internal migrations", func() {
// 	initJobsDB()

// 	var _ = DescribeTable("newDSIdx tests",
// 		func(before, after, expected string) {
// 			computedIdx, err := computeInsertIdx(before, after)
// 			Expect(computedIdx).To(Equal(expected))
// 			Expect(err).To(BeNil())
// 		},
// 		//dList => 1 2 3 4 5
// 		Entry("Internal Migration for regular tables 1 Test 1 : ", "1", "2", "1_1"),
// 		Entry("Internal Migration for regular tables 1 Test 2 : ", "2", "3", "2_1"),

// 		//dList => 1_1 2 3 4 5
// 		Entry("Internal Migration for regular tables 2 Test 1 : ", "1_1", "2", "1_2"),
// 		Entry("Internal Migration for regular tables 2 Test 2 : ", "2", "3", "2_1"),

// 		//dList => 1 2_1 3 4 5
// 		Entry("Internal Migration for regular tables 3 Test 1 : ", "1", "2_1", "1_1"),
// 		Entry("Internal Migration for regular tables 3 Test 2 : ", "2_1", "3", "2_2"),
// 		Entry("Internal Migration for regular tables 3 Test 3 : ", "3", "4", "3_1"),

// 		//dList => 1_1 2_1 3 4 5
// 		Entry("Internal Migration for regular tables 4 Test 1 : ", "1_1", "2_1", "1_2"),

// 		//dList => 0_1 1 2 3 4 5
// 		Entry("Internal Migration for import tables Case 1 Test 1 : ", "0_1", "1", "0_1_1"),
// 		Entry("Internal Migration for import tables Case 1 Test 2 : ", "1", "2", "1_1"),

// 		//dList => 0_1 0_2 1 2 3 4 5
// 		Entry("Internal Migration for import tables Case 2 Test 1 : ", "0_1", "0_2", "0_1_1"),
// 		Entry("Internal Migration for import tables Case 2 Test 2 : ", "0_2", "1", "0_2_1"),
// 		Entry("Internal Migration for import tables Case 2 Test 3 : ", "1", "2", "1_1"),

// 		//dList => 0_1_1 0_2 1 2 3 4 5
// 		Entry("Internal Migration for import tables Case 3 Test 1 : ", "0_1_1", "0_2", "0_1_2"),
// 		Entry("Internal Migration for import tables Case 3 Test 2 : ", "0_2", "1", "0_2_1"),

// 		//dList => 0_1_1 0_2_1 1 2 3 4 5
// 		Entry("Internal Migration for import tables Case 4 Test 1 : ", "0_2_1", "1", "0_2_2"),
// 		Entry("Internal Migration for import tables Case 4 Test 2 : ", "0_1_1", "0_2_1", "0_1_2"),

// 		//dList => 0_1 0_2_1 1 2 3
// 		Entry("Internal Migration for import tables Case 5 Test 1 : ", "0_1", "0_2_1", "0_1_1"),

// 		Entry("OrderTest Case 1 Test 1 : ", "9", "10", "9_1"),

// 		Entry("Internal Migration for tables : ", "10_1", "11_3", "10_2"),
// 		Entry("Internal Migration for tables : ", "0_1", "1", "0_1_1"),
// 		Entry("Internal Migration for tables : ", "0_1", "20", "0_1_1"),
// 		Entry("Internal Migration for tables : ", "0_1", "0_2", "0_1_1"),
// 	)

// 	Context("computeInsertIdx - bad input tests", func() {
// 		It("Should throw error for input 1, 1_1", func() {
// 			_, err := computeInsertIdx("1", "1_1")
// 			Expect(err).To(HaveOccurred())
// 		})
// 		It("Should throw error for input 10_1, 10_2", func() {
// 			_, err := computeInsertIdx("10_1", "10_2")
// 			Expect(err).To(HaveOccurred())
// 		})
// 		It("Should throw error for input 10_1, 10_1", func() {
// 			_, err := computeInsertIdx("10_1", "10_1")
// 			Expect(err).To(HaveOccurred())
// 		})
// 		It("Should throw error for input 10, 9", func() {
// 			_, err := computeInsertIdx("10", "9")
// 			Expect(err).To(HaveOccurred())
// 		})
// 		It("Should throw error for input 10_1_2, 11_3", func() {
// 			_, err := computeInsertIdx("10_1_2", "11_3")
// 			Expect(err).To(HaveOccurred())
// 		})
// 		It("Should throw error for input 0, 1", func() {
// 			_, err := computeInsertIdx("0", "1")
// 			Expect(err).To(HaveOccurred())
// 		})
// 		It("Should throw error for input 0_1, 0", func() {
// 			_, err := computeInsertIdx("0_1", "0")
// 			Expect(err).To(HaveOccurred())
// 		})
// 	})

// 	Context("computeInsertVals - good input tests", func() {
// 		It("Should not throw error for input 0_1, 0_2", func() {
// 			calculatedIdx, err := computeInsertVals([]string{"0", "1"}, []string{"0", "2"})
// 			Expect(err).To(BeNil())
// 			Expect(calculatedIdx).To(Equal([]string{"0", "1", "1"}))
// 		})
// 	})

// 	Context("computeInsertVals - bad input tests", func() {
// 		It("Should throw error for input 1, 1_1", func() {
// 			_, err := computeInsertVals([]string{"1"}, []string{"1", "1"})
// 			Expect(err).To(HaveOccurred())
// 		})
// 		It("Should throw error for input 10_1, 10_2", func() {
// 			_, err := computeInsertVals([]string{"10", "1"}, []string{"10", "2"})
// 			Expect(err).To(HaveOccurred())
// 		})
// 		It("Should throw error for input 10_1, 10_1", func() {
// 			_, err := computeInsertVals([]string{"10", "1"}, []string{"10", "1"})
// 			Expect(err).To(HaveOccurred())
// 		})
// 		It("Should throw error for input 10, 9", func() {
// 			_, err := computeInsertVals([]string{"10"}, []string{"9"})
// 			Expect(err).To(HaveOccurred())
// 		})
// 	})
// })

// var _ = Describe("Calculate newDSIdx for cluster migrations", func() {
// 	initJobsDB()

// 	var _ = DescribeTable("newDSIdx tests",
// 		func(dList []dataSetT, after dataSetT, expected string) {
// 			computedIdx, err := computeIdxForClusterMigration("table_prefix", dList, after)
// 			Expect(computedIdx).To(Equal(expected))
// 			Expect(err).To(BeNil())
// 		},

// 		Entry("ClusterMigration Case 1",
// 			[]dataSetT{
// 				{
// 					JobTable:       "",
// 					JobStatusTable: "",
// 					Index:          "1",
// 				},
// 			},
// 			dataSetT{
// 				JobTable:       "",
// 				JobStatusTable: "",
// 				Index:          "1",
// 			}, "0_1"),

// 		Entry("ClusterMigration Case 2",
// 			[]dataSetT{
// 				{
// 					JobTable:       "",
// 					JobStatusTable: "",
// 					Index:          "0_1",
// 				},
// 				{
// 					JobTable:       "",
// 					JobStatusTable: "",
// 					Index:          "1",
// 				},
// 				{
// 					JobTable:       "",
// 					JobStatusTable: "",
// 					Index:          "2",
// 				},
// 			},
// 			dataSetT{
// 				JobTable:       "",
// 				JobStatusTable: "",
// 				Index:          "1",
// 			}, "0_2"),
// 	)

// 	var _ = DescribeTable("Error cases",
// 		func(dList []dataSetT, after dataSetT) {
// 			_, err := computeIdxForClusterMigration("table_prefix", dList, after)
// 			Expect(err != nil).Should(BeTrue())
// 		},

// 		Entry("ClusterMigration Case 1",
// 			[]dataSetT{
// 				{
// 					JobTable:       "",
// 					JobStatusTable: "",
// 					Index:          "1_1",
// 				},
// 			},
// 			dataSetT{
// 				JobTable:       "",
// 				JobStatusTable: "",
// 				Index:          "1_1",
// 			},
// 		),

// 		Entry("ClusterMigration Case 2",
// 			[]dataSetT{
// 				{
// 					JobTable:       "",
// 					JobStatusTable: "",
// 					Index:          "1",
// 				},
// 				{
// 					JobTable:       "",
// 					JobStatusTable: "",
// 					Index:          "1_1",
// 				},
// 			},
// 			dataSetT{
// 				JobTable:       "",
// 				JobStatusTable: "",
// 				Index:          "1_1",
// 			},
// 		),

// 		Entry("ClusterMigration Case 4",
// 			[]dataSetT{},
// 			dataSetT{
// 				JobTable:       "",
// 				JobStatusTable: "",
// 				Index:          "1_1",
// 			},
// 		),

// 		Entry("ClusterMigration Case 5",
// 			[]dataSetT{},
// 			dataSetT{
// 				JobTable:       "",
// 				JobStatusTable: "",
// 				Index:          "1_1_1_1",
// 			},
// 		),

// 		Entry("ClusterMigration Case 6",
// 			[]dataSetT{},
// 			dataSetT{
// 				JobTable:       "",
// 				JobStatusTable: "",
// 				Index:          "1_1_!_1",
// 			},
// 		),
// 	)
// })

// var sampleTestJob = JobT{
// 	Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
// 	EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
// 	UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
// 	UUID:         uuid.Must(uuid.NewV4()),
// 	CustomVal:    "MOCKDS",
// }

func initJobsDB() {
	config.Load()
	logger.Init()
	admin.Init()
	Init()
	Init2()
	Init3()
}

// var _ = Describe("jobsdb", func() {
// 	initJobsDB()

// 	BeforeEach(func() {
// 		// setup static requirements of dependencies
// 		stats.Setup()
// 	})

// 	Context("getDSList", func() {
// 		var jd *HandleT

// 		BeforeEach(func() {
// 			jd = &HandleT{}

// 			jd.skipSetupDBSetup = true
// 			jd.Setup(ReadWrite, false, "tt", 0*time.Hour, "", false, QueryFiltersT{}, []prebackup.Handler{})
// 		})

// 		AfterEach(func() {
// 			jd.TearDown()
// 		})

// 		It("doesn't make db calls if !refreshFromDB", func() {
// 			jd.datasetList = dsListInMemory

// 			Expect(jd.getDSList(false)).To(Equal(dsListInMemory))
// 		})
// 	})
// })

// var d1 = dataSetT{JobTable: "tt_jobs_1",
// 	JobStatusTable: "tt_job_status_1"}

// var d2 = dataSetT{
// 	JobTable:       "tt_jobs_2",
// 	JobStatusTable: "tt_job_status_2",
// }

// var dsListInMemory = []dataSetT{
// 	d1,
// 	d2,
// }

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()

	// hack to make defer work, without being affected by the os.Exit in TestMain
	os.Exit(run(m))
}

func run(m *testing.M) int {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	database := "jobsdb"
	// pulls an image, creates a container based on it and runs it
	resourcePostgres, err := pool.Run("postgres", "11-alpine", []string{
		"POSTGRES_PASSWORD=password",
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=rudder",
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(resourcePostgres); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resourcePostgres.GetPort("5432/tcp"), database)
	fmt.Println("DB_DSN:", DB_DSN)
	os.Setenv("JOBS_DB_DB_NAME", database)
	os.Setenv("JOBS_DB_HOST", "localhost")
	os.Setenv("JOBS_DB_NAME", "jobsdb")
	os.Setenv("JOBS_DB_USER", "rudder")
	os.Setenv("JOBS_DB_PASSWORD", "password")
	os.Setenv("JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err := sql.Open("postgres", DB_DSN)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// running minio container on docker
	minioResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		Env: []string{
			fmt.Sprintf("MINIO_ACCESS_KEY=%s", accessKeyId),
			fmt.Sprintf("MINIO_SECRET_KEY=%s", secretAccessKey),
			fmt.Sprintf("MINIO_SITE_REGION=%s", region),
		},
	})
	if err != nil {
		panic(fmt.Errorf("Could not start resource: %s", err))
	}
	defer func() {
		if err := pool.Purge(minioResource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	minioEndpoint = fmt.Sprintf("localhost:%s", minioResource.GetPort("9000/tcp"))

	//check if minio server is up & running.
	if err := pool.Retry(func() error {
		url := fmt.Sprintf("http://%s/minio/health/live", minioEndpoint)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	fmt.Println("minio is up & running properly")

	useSSL := false
	minioClient, err := minio.New(minioEndpoint, accessKeyId, secretAccessKey, useSSL)
	if err != nil {
		panic(err)
	}
	fmt.Println("minioClient created successfully")

	//creating bucket inside minio where testing will happen.
	err = minioClient.MakeBucket(bucket, "us-east-1")
	if err != nil {
		panic(err)
	}
	fmt.Println("bucket created successfully")

	code := m.Run()
	blockOnHold()

	return code
}

func blockOnHold() {
	if !hold {
		return
	}

	fmt.Println("Test on hold, before cleanup")
	fmt.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
}

// func initJobsDB() {
// 	config.Load()
// 	logger.Init()
// 	admin.Init()
// 	jobsdb.Init()
// 	jobsdb.Init2()
// 	jobsdb.Init3()

// 	archiver.Init()
// }

func genJobs(workspaceId, customVal string, jobCount, eventsPerJob int) []*JobT {
	js := make([]*JobT, jobCount)
	for i := range js {
		js[i] = &JobT{
			Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","source_job_run_id":""}`),
			EventPayload: []byte(`{"receivedAt":"2021-06-06T20:26:39.598+05:30","writeKey":"writeKey","requestIP":"[::1]",  "batch": [{"anonymousId":"anon_id","channel":"android-sdk","context":{"app":{"build":"1","name":"RudderAndroidClient","namespace":"com.rudderlabs.android.sdk","version":"1.0"},"device":{"id":"49e4bdd1c280bc00","manufacturer":"Google","model":"Android SDK built for x86","name":"generic_x86"},"library":{"name":"com.rudderstack.android.sdk.core"},"locale":"en-US","network":{"carrier":"Android"},"screen":{"density":420,"height":1794,"width":1080},"traits":{"anonymousId":"49e4bdd1c280bc00"},"user_agent":"Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"},"event":"Demo Track","integrations":{"All":true},"messageId":"b96f3d8a-7c26-4329-9671-4e3202f42f15","originalTimestamp":"2019-08-12T05:08:30.909Z","properties":{"category":"Demo Category","floatVal":4.501,"label":"Demo Label","testArray":[{"id":"elem1","value":"e1"},{"id":"elem2","value":"e2"}],"testMap":{"t1":"a","t2":4},"value":5},"rudderId":"a-292e-4e79-9880-f8009e0ae4a3","sentAt":"2019-08-12T05:08:30.909Z","type":"track"}]}`),
			UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
			UUID:         uuid.UUID{},
			CustomVal:    customVal,
			EventCount:   eventsPerJob,
			WorkspaceId:  workspaceId,
			CreatedAt:    time.Date(2022, 06, 07, 13, 13, 13, 13, time.UTC),
			ExpireAt:     time.Date(2022, 06, 07, 13, 13, 13, 13, time.UTC),
		}
	}
	return js
}

func genJobStatuses(jobs []*JobT, state string) []*JobStatusT {
	statuses := []*JobStatusT{}
	for i := range jobs {
		job := jobs[i]
		statuses = append(statuses, &JobStatusT{
			JobID:         job.JobID,
			JobState:      state,
			AttemptNum:    1,
			ExecTime:      time.Date(2022, 06, 07, 13, 13, 13, 13, time.UTC),
			RetryTime:     time.Date(2022, 06, 07, 13, 13, 13, 13, time.UTC),
			ErrorCode:     "404",
			ErrorResponse: []byte(`{}`),
			Parameters:    []byte(`{}`),
			WorkspaceId:   job.WorkspaceId,
		})
	}
	return statuses
}
func setTestEnvs(t *testing.T) {
	t.Setenv("RSERVER_JOBS_DB_BACKUP_ENABLED", "true")
	t.Setenv("RSERVER_JOBS_DB_BACKUP_BATCH_RT_ENABLED", "true")
	t.Setenv("JOBS_BACKUP_BUCKET", "backup-test")
	t.Setenv("RUDDER_TMPDIR", "/tmp")
	t.Setenv(config.TransformKey("JobsDB.maxDSSize"), "10")
	t.Setenv(config.TransformKey("JobsDB.migrateDSLoopSleepDuration"), "3")
	t.Setenv("JOBS_BACKUP_BUCKET", bucket)
	t.Setenv("JOBS_BACKUP_PREFIX", "some-prefix")
	t.Setenv("AWS_ACCESS_KEY_ID", accessKeyId)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secretAccessKey)
	t.Setenv("AWS_ENABLE_SSE", "false")
	t.Setenv("AWS_ENDPOINT", minioEndpoint)
	t.Setenv("AWS_S3_FORCE_PATH_STYLE", "true")
	t.Setenv("AWS_DISABLE_SSL", "true")
	t.Setenv("AWS_REGION", region)
}

func insertMockData() {

}
func TestBackupTable(t *testing.T) {
	setTestEnvs(t)

	initJobsDB()
	stats.Setup()

	dbRetention := time.Second
	migrationMode := ""

	triggerAddNewDS := make(chan time.Time, 0)
	maxDSSize := 10
	jobDB := HandleT{
		MaxDSSize: &maxDSSize,
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	queryFilters := QueryFiltersT{
		CustomVal: true,
	}

	jobDB.Setup(ReadWrite, false, "batch_rt", dbRetention, migrationMode, true, queryFilters, []prebackup.Handler{})
	defer jobDB.TearDown()

	customVal := "MOCKDS"

	insertMockData()
	jobs := genJobs("defaultWorkspaceID-1", customVal, 30, 2)
	require.NoError(t, jobDB.Store(jobs))
	jobs = jobDB.GetUnprocessed(GetQueryParamsT{JobsLimit: 100})
	require.Equal(t, 30, len(jobs), "should get all 30 jobs")
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(jobs[0:30], Aborted.State), []string{customVal}, []ParameterFilterT{}))

	triggerAddNewDS <- time.Now()
	jobs2 := genJobs("defaultWorkspaceID-2", customVal, 30, 2)
	require.NoError(t, jobDB.Store(jobs2))
	jobs2 = jobDB.GetUnprocessed(GetQueryParamsT{JobsLimit: 100})
	require.Equal(t, 30, len(jobs2), "should get all 30 jobs")
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(jobs2[0:30], Aborted.State), []string{customVal}, []ParameterFilterT{}))

	triggerAddNewDS <- time.Now()
	jobs3 := genJobs("defaultWorkspaceID-3", customVal, 30, 2)
	require.NoError(t, jobDB.Store(jobs3))
	jobs3 = jobDB.GetUnprocessed(GetQueryParamsT{JobsLimit: 100})
	require.Equal(t, 30, len(jobs3), "should get all 30 jobs")
	require.NoError(t, jobDB.UpdateJobStatus(genJobStatuses(jobs3[0:30], Aborted.State), []string{customVal}, []ParameterFilterT{}))

	fmFactory := filemanager.FileManagerFactoryT{}
	fm, err := fmFactory.New(&filemanager.SettingsT{
		Provider: "S3",
		Config: map[string]interface{}{
			"bucketName":       bucket,
			"accessKeyID":      accessKeyId,
			"accessKey":        secretAccessKey,
			"enableSSE":        false,
			"prefix":           "some-prefix",
			"endPoint":         minioEndpoint,
			"s3ForcePathStyle": true,
			"disableSSL":       true,
			"region":           region,
			"useGlue":          true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var file []*filemanager.FileObject
	require.Eventually(t, func() bool {
		file, err = fm.ListFilesWithPrefix(context.TODO(), "some-prefix", 5)
		require.NoError(t, err, "expected no error while listing files")

		if len(file) > 0 {
			return true
		} else {
			return false
		}
	}, time.Second*10, time.Second*5, "no files found in backup store")

	var backedupStatusFileName, backedupJobsFileName string
	for i := 0; i < len(file); i++ {
		if strings.Contains(file[i].Key, "status") {
			backedupStatusFileName = file[i].Key
		} else {
			backedupJobsFileName = file[i].Key
		}
	}

	//reading golden files
	originalJobsFile, err := readGzipFile("goldenDirectory/backupJobs.json.gz")
	require.NoError(t, err, "expected no error while reading golden jobs file")
	originalJobsFile = bytes.Replace(originalJobsFile, []byte("}\n{"), []byte("}, \n {"), -1) //replacing ", \n " with "\n"
	originalJobsFile = []byte("[" + string(originalJobsFile) + "]")

	var originalJobs []*JobT
	err = json.Unmarshal(originalJobsFile, &originalJobs)
	require.NoError(t, err, "expected no error while unmarshalling downloaded file")

	originalStatusFile, err := readGzipFile("goldenDirectory/backupStatus.json.gz")
	require.NoError(t, err, "expected no error while reading golden jobs file")

	//downloading backed-up files
	DownloadedStausFileName := "downloadedStatus.json.gz"
	err = downloadBackedupFiles(fm, backedupStatusFileName, DownloadedStausFileName)
	require.NoError(t, err, "expected no error")
	defer os.Remove(DownloadedStausFileName)

	DownloadedJobsFileName := "downloadedJobs.json.gz"
	err = downloadBackedupFiles(fm, backedupJobsFileName, DownloadedJobsFileName)
	require.NoError(t, err, "expected no error")
	defer os.Remove(DownloadedJobsFileName)

	//reading downloaded backedup file to verify
	downloadedJobsFileContent, err := readGzipFile(DownloadedJobsFileName)
	require.NoError(t, err, "expected no error while reading golden jobs file")

	downloadedStatusFileContent, err := readGzipFile(DownloadedStausFileName)
	require.NoError(t, err, "expected no error while reading golden jobs file")

	ans := strings.Compare(string(originalStatusFile), string(downloadedStatusFileContent))
	require.Equal(t, 0, ans, "downloaded status file different than actual file")
	downloadedJobsFileContent = bytes.Replace(downloadedJobsFileContent, []byte("}\n{"), []byte("}, \n {"), -1) //replacing ", \n " with "\n"
	downloadedJobsFileContent = []byte("[" + string(downloadedJobsFileContent) + "]")

	var backedupJobs []*JobT
	err = json.Unmarshal(downloadedJobsFileContent, &backedupJobs)
	require.NoError(t, err, "expected no error while unmarshalling downloaded file")

	ok := checkJobsEqual(originalJobs[0], backedupJobs[0])
	require.Equal(t, true, ok, "original jobs in not equal to backedup jobs")
}

func checkJobsEqual(originalJobs, backedupJobs *JobT) bool {

	if strings.Compare(originalJobs.UUID.String(), backedupJobs.UUID.String()) != 0 {
		return false
	}
	if originalJobs.JobID != backedupJobs.JobID {
		return false
	}
	if strings.Compare(originalJobs.UserID, backedupJobs.UserID) != 0 {
		return false
	}
	if strings.Compare(originalJobs.CustomVal, backedupJobs.CustomVal) != 0 {
		return false
	}
	if originalJobs.EventCount != backedupJobs.EventCount {
		return false
	}

	originalEventPayload, err := json.Marshal(originalJobs.EventPayload)
	if err != nil {
		return false
	}
	backedupEventPayload, err := json.Marshal(backedupJobs.EventPayload)
	if err != nil {
		return false
	}
	if strings.Compare(string(originalEventPayload), string(backedupEventPayload)) != 0 {
		return false
	}

	if originalJobs.PayloadSize != backedupJobs.PayloadSize {
		return false
	}

	originalParameters, err := json.Marshal(originalJobs.Parameters)
	if err != nil {
		return false
	}
	backedupParameters, err := json.Marshal(backedupJobs.Parameters)
	if err != nil {
		return false
	}
	if strings.Compare(string(originalParameters), string(backedupParameters)) != 0 {
		return false
	}

	if strings.Compare(originalJobs.WorkspaceId, backedupJobs.WorkspaceId) != 0 {
		return false
	}
	return true
}

func downloadBackedupFiles(fm filemanager.FileManager, fileToDownload string, downloadedFileName string) error {
	filePtr, err := os.OpenFile(downloadedFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("error while Creating file to download data: ", err)
	}

	err = fm.Download(context.Background(), filePtr, fileToDownload)

	filePtr.Close()
	return err
}

func readGzipFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return []byte{}, err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return []byte{}, err
	}
	defer gz.Close()

	return io.ReadAll(gz)
}
