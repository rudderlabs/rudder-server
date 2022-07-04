package jobsdb

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	uuid "github.com/gofrs/uuid"
	"github.com/minio/minio-go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	hold   bool
	DB_DSN = "root@tcp(127.0.0.1:3306)/service"

	minioEndpoint   string
	bucket          = "backup-test"
	prefix          = "some-prefix"
	region          = "us-east-1"
	accessKeyId     = "MYACCESSKEY"
	secretAccessKey = "MYSECRETKEY"
)

var _ = Describe("Calculate newDSIdx for internal migrations", func() {
	initJobsDB()

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

		Entry("Internal Migration for tables : ", "10_1", "11_3", "10_2"),
		Entry("Internal Migration for tables : ", "0_1", "1", "0_1_1"),
		Entry("Internal Migration for tables : ", "0_1", "20", "0_1_1"),
		Entry("Internal Migration for tables : ", "0_1", "0_2", "0_1_1"),
	)

	Context("computeInsertIdx - bad input tests", func() {
		It("Should throw error for input 1, 1_1", func() {
			_, err := computeInsertIdx("1", "1_1")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10_1, 10_2", func() {
			_, err := computeInsertIdx("10_1", "10_2")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10_1, 10_1", func() {
			_, err := computeInsertIdx("10_1", "10_1")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10, 9", func() {
			_, err := computeInsertIdx("10", "9")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10_1_2, 11_3", func() {
			_, err := computeInsertIdx("10_1_2", "11_3")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 0, 1", func() {
			_, err := computeInsertIdx("0", "1")
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 0_1, 0", func() {
			_, err := computeInsertIdx("0_1", "0")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("computeInsertVals - good input tests", func() {
		It("Should not throw error for input 0_1, 0_2", func() {
			calculatedIdx, err := computeInsertVals([]string{"0", "1"}, []string{"0", "2"})
			Expect(err).To(BeNil())
			Expect(calculatedIdx).To(Equal([]string{"0", "1", "1"}))
		})
	})

	Context("computeInsertVals - bad input tests", func() {
		It("Should throw error for input 1, 1_1", func() {
			_, err := computeInsertVals([]string{"1"}, []string{"1", "1"})
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10_1, 10_2", func() {
			_, err := computeInsertVals([]string{"10", "1"}, []string{"10", "2"})
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10_1, 10_1", func() {
			_, err := computeInsertVals([]string{"10", "1"}, []string{"10", "1"})
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for input 10, 9", func() {
			_, err := computeInsertVals([]string{"10"}, []string{"9"})
			Expect(err).To(HaveOccurred())
		})
	})
})

var _ = Describe("Calculate newDSIdx for cluster migrations", func() {
	initJobsDB()

	var _ = DescribeTable("newDSIdx tests",
		func(dList []dataSetT, after dataSetT, expected string) {
			computedIdx, err := computeIdxForClusterMigration("table_prefix", dList, after)
			Expect(computedIdx).To(Equal(expected))
			Expect(err).To(BeNil())
		},

		Entry("ClusterMigration Case 1",
			[]dataSetT{
				{
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
				{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "0_1",
				},
				{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1",
				},
				{
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
				{
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
				{
					JobTable:       "",
					JobStatusTable: "",
					Index:          "1",
				},
				{
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
	UUID:         uuid.Must(uuid.NewV4()),
	CustomVal:    "MOCKDS",
}

func initJobsDB() {
	config.Load()
	logger.Init()
	admin.Init()
	Init()
	Init2()
	Init3()
}

var _ = Describe("jobsdb", func() {
	initJobsDB()

	BeforeEach(func() {
		// setup static requirements of dependencies
		stats.Setup()
	})

	Context("getDSList", func() {
		var jd *HandleT

		BeforeEach(func() {
			jd = &HandleT{}

			jd.skipSetupDBSetup = true
			jd.Setup(ReadWrite, false, "tt", 0*time.Hour, "", false, QueryFiltersT{}, []prebackup.Handler{})
		})

		AfterEach(func() {
			jd.TearDown()
		})

		It("doesn't make db calls if !refreshFromDB", func() {
			jd.datasetList = dsListInMemory

			Expect(jd.getDSList(false)).To(Equal(dsListInMemory))
		})
	})
})

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

func setTestEnvs(t *testing.T) {
	t.Setenv("RSERVER_JOBS_DB_BACKUP_ENABLED", "true")
	t.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
	t.Setenv("RSERVER_JOBS_DB_BACKUP_BATCH_RT_ENABLED", "true")
	t.Setenv("JOBS_BACKUP_BUCKET", "backup-test")
	t.Setenv("RUDDER_TMPDIR", "/tmp")
	t.Setenv(config.TransformKey("JobsDB.maxDSSize"), "10")
	t.Setenv(config.TransformKey("JobsDB.migrateDSLoopSleepDuration"), "3")
	t.Setenv("JOBS_BACKUP_BUCKET", bucket)
	t.Setenv("JOBS_BACKUP_PREFIX", prefix)
	t.Setenv("MINIO_ACCESS_KEY_ID", accessKeyId)
	t.Setenv("MINIO_SECRET_ACCESS_KEY", secretAccessKey)
	t.Setenv("MINIO_ENDPOINT", minioEndpoint)
	t.Setenv("MINIO_SSL", "false")
}

func TestBackupTable(t *testing.T) {

	// running minio container on docker
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

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
	goldenFileJobsFileName := "goldenDirectory/backupJobs.json.gz"
	goldenFileStatusFileName := "goldenDirectory/backupStatus.json.gz"

	jobDB.Setup(ReadWrite, false, "batch_rt", dbRetention, migrationMode, true, queryFilters, []prebackup.Handler{})
	defer jobDB.TearDown()
	fmt.Println("reading jobs")
	jobs, err := readGzipJobFile(goldenFileJobsFileName)
	require.NoError(t, err, "expected no error while reading golden jobs file")

	statusList, err := readGzipStatusFile(goldenFileStatusFileName)
	require.NoError(t, err, "expected no error while reading golden status file")

	ds := newDataSet("batch_rt", "1")
	err = jobDB.WithTx(func(tx *sql.Tx) error {
		if err := jobDB.copyJobsDS(tx, ds, jobs); err != nil {
			fmt.Println("error while copying jobs to ds")
			return err
		}

		return jobDB.copyJobStatusDS(tx, ds, statusList, []string{}, nil)
	})
	require.NoError(t, err, "expected no error while coping jobs & status to DS")

	ds2 := newDataSet("batch_rt", "2")
	jobDB.addNewDS(ds2)

	err = jobDB.WithTx(func(tx *sql.Tx) error {
		if err := jobDB.copyJobsDS(tx, ds2, jobs); err != nil {
			fmt.Println("error while copying jobs to ds")
			return err
		}

		return jobDB.copyJobStatusDS(tx, ds2, statusList, []string{}, nil)
	})
	require.NoError(t, err, "expected no error while coping jobs & status to DS")

	fmFactory := filemanager.FileManagerFactoryT{}
	fm, err := fmFactory.New(&filemanager.SettingsT{
		Provider: "MINIO",
		Config: map[string]interface{}{
			"bucketName":      bucket,
			"prefix":          prefix,
			"endPoint":        minioEndpoint,
			"accessKeyID":     accessKeyId,
			"secretAccessKey": secretAccessKey,
			"useSSL":          false,
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

	//downloading backed-up files
	DownloadedStausFileName := "downloadedStatus.json.gz"
	err = downloadBackedupFiles(fm, backedupStatusFileName, DownloadedStausFileName)
	require.NoError(t, err, "expected no error")
	downloadedStatusFile, err := readGzipFile(DownloadedStausFileName)
	require.NoError(t, err, "expected no error")
	backupStatusFile, err := readGzipFile(goldenFileStatusFileName)
	require.NoError(t, err, "expected no error")
	require.Equal(t, backupStatusFile, downloadedStatusFile, "expected status files to be same")
	defer os.Remove(DownloadedStausFileName)

	DownloadedJobsFileName := "downloadedJobs.json.gz"
	err = downloadBackedupFiles(fm, backedupJobsFileName, DownloadedJobsFileName)
	require.NoError(t, err, "expected no error")
	downloadedJobsFile, err := readGzipFile(DownloadedJobsFileName)
	require.NoError(t, err, "expected no error")
	backupJobsFile, err := readGzipFile(goldenFileJobsFileName)
	require.NoError(t, err, "expected no error")
	require.Equal(t, backupJobsFile, downloadedJobsFile, "expected jobs files to be same")
	defer os.Remove(DownloadedJobsFileName)

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
		return nil, err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	return io.ReadAll(gz)
}

func readGzipJobFile(filename string) ([]*JobT, error) {
	file, err := os.Open(filename)
	if err != nil {
		return []*JobT{}, err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return []*JobT{}, err
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := 10240 * 1024 // 10MB
	buf := make([]byte, maxCapacity)
	sc.Buffer(buf, maxCapacity)

	jobs := []*JobT{}
	for sc.Scan() {
		lineByte := sc.Bytes()
		uuid, err := uuid.FromString("69359037-9599-48e7-b8f2-48393c019135")
		if err != nil {
			return []*JobT{}, err
		}
		job := &JobT{
			UUID:         uuid,
			JobID:        gjson.GetBytes(lineByte, "job_id").Int(),
			UserID:       gjson.GetBytes(lineByte, "user_id").String(),
			CustomVal:    gjson.GetBytes(lineByte, "custom_val").String(),
			Parameters:   []byte(gjson.GetBytes(lineByte, "parameters").String()),
			EventCount:   int(gjson.GetBytes(lineByte, "event_count").Int()),
			WorkspaceId:  gjson.GetBytes(lineByte, "workspace_id").String(),
			EventPayload: []byte(gjson.GetBytes(lineByte, "event_payload").String()),
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func readGzipStatusFile(fileName string) ([]*JobStatusT, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return []*JobStatusT{}, err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return []*JobStatusT{}, err
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := 10240 * 1024 // 10MB
	buf := make([]byte, maxCapacity)
	sc.Buffer(buf, maxCapacity)

	statusList := []*JobStatusT{}
	for sc.Scan() {
		lineByte := sc.Bytes()
		jobStatus := &JobStatusT{
			JobID:         gjson.GetBytes(lineByte, "job_id").Int(),
			JobState:      gjson.GetBytes(lineByte, "job_state").String(),
			AttemptNum:    int(gjson.GetBytes(lineByte, "attempt").Int()),
			ErrorCode:     gjson.GetBytes(lineByte, "error_code").String(),
			ErrorResponse: []byte(gjson.GetBytes(lineByte, "error_response").String()),
			Parameters:    []byte(gjson.GetBytes(lineByte, "parameters").String()),
		}
		statusList = append(statusList, jobStatus)
	}
	return statusList, nil
}
