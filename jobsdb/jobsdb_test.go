package jobsdb

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	uuid "github.com/gofrs/uuid"
	"github.com/minio/minio-go"
	"github.com/ory/dockertest/v3"
	"github.com/tidwall/gjson"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	hold            bool
	database        = "jobsdb"
	DB_DSN          = "root@tcp(127.0.0.1:3306)/service"
	minioEndpoint   string
	bucket          = "backup-test"
	prefix          = "some-prefix"
	region          = "us-east-1"
	accessKeyId     = "MYACCESSKEY"
	secretAccessKey = "MYSECRETKEY"
)

var _ = Describe("Calculate newDSIdx for internal migrations", func() {
	initJobsDB()

	_ = DescribeTable("newDSIdx tests",
		func(before, after, expected string) {
			computedIdx, err := computeInsertIdx(before, after)
			Expect(computedIdx).To(Equal(expected))
			Expect(err).To(BeNil())
		},
		// dList => 1 2 3 4 5
		Entry("Internal Migration for regular tables 1 Test 1 : ", "1", "2", "1_1"),
		Entry("Internal Migration for regular tables 1 Test 2 : ", "2", "3", "2_1"),

		// dList => 1_1 2 3 4 5
		Entry("Internal Migration for regular tables 2 Test 1 : ", "1_1", "2", "1_2"),
		Entry("Internal Migration for regular tables 2 Test 2 : ", "2", "3", "2_1"),

		// dList => 1 2_1 3 4 5
		Entry("Internal Migration for regular tables 3 Test 1 : ", "1", "2_1", "1_1"),
		Entry("Internal Migration for regular tables 3 Test 2 : ", "2_1", "3", "2_2"),
		Entry("Internal Migration for regular tables 3 Test 3 : ", "3", "4", "3_1"),

		// dList => 1_1 2_1 3 4 5
		Entry("Internal Migration for regular tables 4 Test 1 : ", "1_1", "2_1", "1_2"),

		// dList => 0_1 1 2 3 4 5
		Entry("Internal Migration for import tables Case 1 Test 1 : ", "0_1", "1", "0_1_1"),
		Entry("Internal Migration for import tables Case 1 Test 2 : ", "1", "2", "1_1"),

		// dList => 0_1 0_2 1 2 3 4 5
		Entry("Internal Migration for import tables Case 2 Test 1 : ", "0_1", "0_2", "0_1_1"),
		Entry("Internal Migration for import tables Case 2 Test 2 : ", "0_2", "1", "0_2_1"),
		Entry("Internal Migration for import tables Case 2 Test 3 : ", "1", "2", "1_1"),

		// dList => 0_1_1 0_2 1 2 3 4 5
		Entry("Internal Migration for import tables Case 3 Test 1 : ", "0_1_1", "0_2", "0_1_2"),
		Entry("Internal Migration for import tables Case 3 Test 2 : ", "0_2", "1", "0_2_1"),

		// dList => 0_1_1 0_2_1 1 2 3 4 5
		Entry("Internal Migration for import tables Case 4 Test 1 : ", "0_2_1", "1", "0_2_2"),
		Entry("Internal Migration for import tables Case 4 Test 2 : ", "0_1_1", "0_2_1", "0_1_2"),

		// dList => 0_1 0_2_1 1 2 3
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

	_ = DescribeTable("newDSIdx tests with skipZeroAssertionForMultitenant",
		func(before, after, expected string) {
			setSkipZeroAssertionForMultitenant(true)
			computedIdx, err := computeInsertIdx(before, after)
			Expect(computedIdx).To(Equal(expected))
			Expect(err).To(BeNil())
			setSkipZeroAssertionForMultitenant(false)
		},
		// dList => 1 2 3 4 5
		Entry("Internal Migration for regular tables 1 Test 1 with skipZeroAssertionForMultitenant: ", "1", "2", "1_1"),
		Entry("Internal Migration for regular tables 1 Test 2 with skipZeroAssertionForMultitenant: ", "2", "3", "2_1"),

		// dList => 1_1 2 3 4 5
		Entry("Internal Migration for regular tables 2 Test 1 with skipZeroAssertionForMultitenant: ", "1_1", "2", "1_2"),
		Entry("Internal Migration for regular tables 2 Test 2 with skipZeroAssertionForMultitenant: ", "2", "3", "2_1"),

		// dList => 1 2_1 3 4 5
		Entry("Internal Migration for regular tables 3 Test 1 with skipZeroAssertionForMultitenant: ", "1", "2_1", "1_1"),
		Entry("Internal Migration for regular tables 3 Test 2 with skipZeroAssertionForMultitenant: ", "2_1", "3", "2_2"),
		Entry("Internal Migration for regular tables 3 Test 3 with skipZeroAssertionForMultitenant: ", "3", "4", "3_1"),

		// dList => 1_1 2_1 3 4 5
		Entry("Internal Migration for regular tables 4 Test 1 with skipZeroAssertionForMultitenant: ", "1_1", "2_1", "1_2"),

		// dList => 0_1 1 2 3 4 5
		Entry("Internal Migration for import tables Case 1 Test 2 with skipZeroAssertionForMultitenant: ", "1", "2", "1_1"),

		Entry("Internal Migration for import tables Case 2 Test 3 with skipZeroAssertionForMultitenant: ", "1", "2", "1_1"),

		Entry("OrderTest Case 1 Test 1 with skipZeroAssertionForMultitenant: ", "9", "10", "9_1"),

		Entry("Internal Migration for tables with skipZeroAssertionForMultitenant: ", "10_1", "11_3", "10_2"),
		Entry("Internal Migration for tables with skipZeroAssertionForMultitenant: ", "0_1", "1", "0_2"),
		Entry("Internal Migration for tables with skipZeroAssertionForMultitenant: ", "0_1", "20", "0_2"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "-1", "0", "-1_1"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "0", "1", "0_1"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "-2_1", "-1_1", "-2_2"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "-2_1", "-1", "-2_2"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "-2_1", "0", "-2_2"),
		Entry("Internal Migration for tables with Negative Indexes and skipZeroAssertionForMultitenant: ", "-2_1", "20", "-2_2"),
	)

	Context("computeInsertVals - good input tests", func() {
		It("Should not throw error for input 0_1, 0_2", func() {
			calculatedIdx, err := computeInsertVals([]string{"0", "1"}, []string{"0", "2"})
			Expect(err).To(BeNil())
			Expect(calculatedIdx).To(Equal([]string{"0", "1", "1"}))
		})
	})

	Context("computeInsertVals - bad input tests", func() {
		It("Should throw error for nil inputs", func() {
			_, err := computeInsertVals(nil, nil)
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for nil before input", func() {
			_, err := computeInsertVals(nil, []string{"1"})
			Expect(err).To(HaveOccurred())
		})
		It("Should throw error for nil after input", func() {
			_, err := computeInsertVals([]string{"1"}, nil)
			Expect(err).To(HaveOccurred())
		})
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

	_ = DescribeTable("newDSIdx tests",
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

	_ = DescribeTable("Error cases",
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

			Expect(jd.getDSList()).To(Equal(dsListInMemory))
		})
	})

	Context("Start & Stop", Ordered, func() {
		var jd *HandleT

		BeforeEach(func() {
			jd = &HandleT{}
			jd.skipSetupDBSetup = true
			jd.Setup(ReadWrite, false, "tt", 0*time.Hour, "", false, QueryFiltersT{}, []prebackup.Handler{})
		})

		AfterEach(func() {
			jd.TearDown()
		})

		It("can call Stop before Start without side-effects", func() {
			jd.Stop()
			jd.Start()
			Expect(jd.lifecycle.started).To(Equal(true))
		})

		It("can call Start twice without side-effects", func() {
			jd.Start()
			group1 := jd.backgroundGroup
			jd.Start()
			group2 := jd.backgroundGroup
			Expect(group1).To(Equal(group2))
			Expect(jd.lifecycle.started).To(Equal(true))
		})

		It("can call Start in parallel without side-effects", func() {
			var wg sync.WaitGroup
			var bgGroups []*errgroup.Group
			wg.Add(10)
			for i := 0; i < 10; i++ {
				go func() {
					jd.Start()
					bgGroups = append(bgGroups, jd.backgroundGroup)
					wg.Done()
				}()
			}
			wg.Wait()
			for i := 1; i < 10; i++ {
				Expect(bgGroups[i-1]).To(Equal(bgGroups[i]))
			}
			Expect(jd.lifecycle.started).To(Equal(true))
		})

		It("can call Stop twice without side-effects", func() {
			jd.Start()
			Expect(jd.lifecycle.started).To(Equal(true))
			Expect(jd.backgroundGroup).ToNot(BeNil())
			jd.Stop()
			Expect(jd.backgroundGroup).ToNot(BeNil())
			Expect(jd.lifecycle.started).To(Equal(false))
			Expect(jd.backgroundGroup.Wait()).To(BeNil())
			jd.Stop()
			Expect(jd.backgroundGroup).ToNot(BeNil())
			Expect(jd.lifecycle.started).To(Equal(false))
			Expect(jd.backgroundGroup.Wait()).To(BeNil())
		})

		It("can call Stop in parallel without side-effects", func() {
			jd.Start()

			var wg sync.WaitGroup
			wg.Add(10)
			for i := 0; i < 10; i++ {
				go func() {
					jd.Stop()
					Expect(jd.backgroundGroup.Wait()).To(BeNil())
					wg.Done()
				}()
			}
			wg.Wait()
		})

		It("can call Start & Stop in parallel without problems", func() {
			jd.Start()

			var wg sync.WaitGroup
			wg.Add(10)
			for i := 0; i < 10; i++ {
				go func() {
					jd.Start()
					jd.Stop()
					wg.Done()
				}()
			}
			wg.Wait()
			Expect(jd.lifecycle.started).To(Equal(false))
		})
	})
})

var d1 = dataSetT{
	JobTable:       "tt_jobs_1",
	JobStatusTable: "tt_job_status_1",
}

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
	t.Setenv("RSERVER_JOBS_DB_BACKUP_RT_FAILED_ONLY", "true")
	t.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
	t.Setenv("RSERVER_JOBS_DB_BACKUP_BATCH_RT_ENABLED", "true")
	t.Setenv("RSERVER_JOBS_DB_BACKUP_RT_ENABLED", "true")

	t.Setenv("JOBS_BACKUP_BUCKET", "backup-test")
	t.Setenv("RUDDER_TMPDIR", "/tmp")
	t.Setenv(config.TransformKey("JobsDB.maxDSSize"), "10")
	t.Setenv(config.TransformKey("JobsDB.migrateDSLoopSleepDuration"), "3")
	t.Setenv("JOBS_BACKUP_BUCKET", bucket)
	t.Setenv("JOBS_BACKUP_PREFIX", prefix)
	t.Setenv("MINIO_ACCESS_KEY_ID", accessKeyId)
	t.Setenv("MINIO_SECRET_ACCESS_KEY", secretAccessKey)
	t.Setenv("MINIO_SSL", "false")
	t.Setenv("JOBS_DB_DB_NAME", database)
	t.Setenv("JOBS_DB_HOST", "localhost")
	t.Setenv("JOBS_DB_NAME", "jobsdb")
	t.Setenv("JOBS_DB_USER", "rudder")
	t.Setenv("JOBS_DB_PASSWORD", "password")
}

func TestBackupTable(t *testing.T) {
	// running minio container on docker
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

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
	t.Setenv("JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))

	// NOTE: DB initilization should happen after we have all the env vars set
	setTestEnvs(t)
	initJobsDB()
	stats.Setup()

	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resourcePostgres.GetPort("5432/tcp"), database)
	t.Log("DB_DSN:", DB_DSN)
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
	t.Log("postgres setup successful")

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
		t.Fatal(fmt.Errorf("Could not start resource: %s", err))
	}
	defer func() {
		if err := pool.Purge(minioResource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	minioEndpoint = fmt.Sprintf("localhost:%s", minioResource.GetPort("9000/tcp"))
	t.Setenv("MINIO_ENDPOINT", minioEndpoint)

	// check if minio server is up & running.
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
	t.Log("minio is up & running properly")

	useSSL := false
	minioClient, err := minio.New(minioEndpoint, accessKeyId, secretAccessKey, useSSL)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("minioClient created successfully")

	// creating bucket inside minio where testing will happen.
	err = minioClient.MakeBucket(bucket, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("bucket created successfully")

	goldenFileJobsFileName := "goldenDirectory/backupJobs.json.gz"
	goldenFileStatusFileName := "goldenDirectory/backupStatus.json.gz"

	t.Log("reading jobs")
	jobs, err := readGzipJobFile(goldenFileJobsFileName)
	require.NoError(t, err, "expected no error while reading golden jobs file")

	statusList, err := readGzipStatusFile(goldenFileStatusFileName)
	require.NoError(t, err, "expected no error while reading golden status file")

	maxDSSize = 10
	batchRTJobsDB, err := insertBatchRTData(t, jobs, statusList)
	require.NoError(t, err, "expected no error while inserting batch data")
	defer batchRTJobsDB.TearDown()

	jobDB_rt, err := insertRTData(t, jobs, statusList)
	require.NoError(t, err, "expected no error while inserting rt data")
	defer jobDB_rt.TearDown()

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
		file, err = fm.ListFilesWithPrefix(context.Background(), prefix, 5)
		require.NoError(t, err, "expected no error while listing files")
		if len(file) == 3 {
			return true
		} else {
			return false
		}
	}, time.Second*10, time.Second*3, "no files found in backup store")

	var backedupStatusFileName, backedupJobsFileName, backedupAbortedOnlyRTJobs string
	for i := 0; i < len(file); i++ {
		if strings.Contains(file[i].Key, "batch_rt") {
			if strings.Contains(file[i].Key, "status") {
				backedupStatusFileName = file[i].Key
			} else {
				backedupJobsFileName = file[i].Key
			}
		} else if strings.Contains(file[i].Key, "aborted") {
			backedupAbortedOnlyRTJobs = file[i].Key
		}
	}

	// downloading backed-up files
	DownloadedAbortedFileName := "downloadedAbortedJobs.json.gz"
	err = downloadBackedupFiles(t, fm, backedupAbortedOnlyRTJobs, DownloadedAbortedFileName)
	require.NoError(t, err, "expected no error")
	defer os.Remove(DownloadedAbortedFileName)
	abortedJobs, abortedStatus, err := getJobsFromAbortedJobs(t, DownloadedAbortedFileName)
	require.NoError(t, err, "expected no error while getting jobs & status from aborted jobs file")
	require.Equal(t, jobs, abortedJobs, "expected jobs to be same in case of only aborted backup")
	require.Equal(t, statusList, abortedStatus, "expected status to be same in case of only aborted backup")

	DownloadedStausFileName := "downloadedStatus.json.gz"
	err = downloadBackedupFiles(t, fm, backedupStatusFileName, DownloadedStausFileName)
	require.NoError(t, err, "expected no error")
	defer os.Remove(DownloadedStausFileName)
	downloadedStatusFile, err := readGzipFile(DownloadedStausFileName)
	require.NoError(t, err, "expected no error")
	backupStatusFile, err := readGzipFile(goldenFileStatusFileName)
	require.NoError(t, err, "expected no error")
	require.Equal(t, backupStatusFile, downloadedStatusFile, "expected status files to be same")

	DownloadedJobsFileName := "downloadedJobs.json.gz"
	err = downloadBackedupFiles(t, fm, backedupJobsFileName, DownloadedJobsFileName)
	require.NoError(t, err, "expected no error")
	defer os.Remove(DownloadedJobsFileName)
	downloadedJobsFile, err := readGzipFile(DownloadedJobsFileName)
	require.NoError(t, err, "expected no error")
	backupJobsFile, err := readGzipFile(goldenFileJobsFileName)
	require.NoError(t, err, "expected no error")
	require.Equal(t, backupJobsFile, downloadedJobsFile, "expected jobs files to be same")
}

func insertRTData(t *testing.T, jobs []*JobT, statusList []*JobStatusT) (HandleT, error) {
	dbRetention := time.Second
	migrationMode := ""
	queryFilters := QueryFiltersT{
		CustomVal: true,
	}
	triggerAddNewDS := make(chan time.Time, 0)

	jobDB_rt := HandleT{
		MaxDSSize: &maxDSSize,
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	jobDB_rt.Setup(ReadWrite, false, "rt", dbRetention, migrationMode, true, queryFilters, []prebackup.Handler{})
	// defer jobDB.TearDown()

	rtDS := newDataSet("rt", "1")
	err := jobDB_rt.WithTx(func(tx *sql.Tx) error {
		if err := jobDB_rt.copyJobsDS(tx, rtDS, jobs); err != nil {
			return err
		}

		return jobDB_rt.copyJobStatusDS(tx, rtDS, statusList, []string{}, nil)
	})
	if err != nil {
		return jobDB_rt, err
	}

	rtDS2 := newDataSet("rt", "2")
	jobDB_rt.dsListLock.WithLock(func(l lock.DSListLockToken) {
		jobDB_rt.addNewDS(l, rtDS2)
	})
	err = jobDB_rt.WithTx(func(tx *sql.Tx) error {
		if err := jobDB_rt.copyJobsDS(tx, rtDS2, jobs); err != nil {
			return err
		}

		return jobDB_rt.copyJobStatusDS(tx, rtDS2, statusList, []string{}, nil)
	})
	if err != nil {
		t.Log("error while coping RT jobs & status to DS: ", err)
		return jobDB_rt, err
	}
	return jobDB_rt, nil
}

func insertBatchRTData(t *testing.T, jobs []*JobT, statusList []*JobStatusT) (HandleT, error) {
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

	ds := newDataSet("batch_rt", "1")
	err := jobDB.WithTx(func(tx *sql.Tx) error {
		if err := jobDB.copyJobsDS(tx, ds, jobs); err != nil {
			t.Log("error while copying jobs to ds: ", err)
			return err
		}

		return jobDB.copyJobStatusDS(tx, ds, statusList, []string{}, nil)
	})
	if err != nil {
		t.Log("error while coping jobs & status to DS: ", err)
		return jobDB, err
	}

	ds2 := newDataSet("batch_rt", "2")
	jobDB.dsListLock.WithLock(func(l lock.DSListLockToken) {
		jobDB.addNewDS(l, ds2)
	})
	err = jobDB.WithTx(func(tx *sql.Tx) error {
		if err := jobDB.copyJobsDS(tx, ds2, jobs); err != nil {
			t.Log("error while copying jobs to ds: ", err)
			return err
		}

		return jobDB.copyJobStatusDS(tx, ds2, statusList, []string{}, nil)
	})
	if err != nil {
		t.Log("error while coping jobs & status to DS: ", err)
		return jobDB, err
	}
	return jobDB, nil
}

func getJobsFromAbortedJobs(t *testing.T, fileName string) ([]*JobT, []*JobStatusT, error) {
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		t.Log("error while opening file: ", err)
		return nil, nil, err
	}

	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Log("error while creating gzip reader: ", err)
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := 10240 * 1024 // 10MB
	buf := make([]byte, maxCapacity)
	sc.Buffer(buf, maxCapacity)

	jobs := []*JobT{}
	statusList := []*JobStatusT{}

	for sc.Scan() {
		lineByte := sc.Bytes()
		uuid, err := uuid.FromString("69359037-9599-48e7-b8f2-48393c019135")
		if err != nil {
			return []*JobT{}, []*JobStatusT{}, err
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
	return jobs, statusList, nil
}

func downloadBackedupFiles(t *testing.T, fm filemanager.FileManager, fileToDownload, downloadedFileName string) error {
	filePtr, err := os.OpenFile(downloadedFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Log("error while Creating file to download data: ", err)
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

func BenchmarkSanitizeJson(b *testing.B) {
	size := 4_000
	nulls := 100

	// string with nulls
	inputWithoutNulls := randomString(size - nulls*len(`\u0000`))
	inputWithNulls := insertStringInString(inputWithoutNulls, `\u0000`, nulls)
	require.Equal(b, json.RawMessage(inputWithoutNulls), sanitizedJsonUsingStrings(json.RawMessage(inputWithNulls)))
	require.Equal(b, json.RawMessage(inputWithoutNulls), sanitizedJsonUsingBytes(json.RawMessage(inputWithNulls)))
	require.Equal(b, json.RawMessage(inputWithoutNulls), sanitizedJsonUsingRegexp(json.RawMessage(inputWithNulls)))
	b.Run(fmt.Sprintf("SanitizeUsingStrings string of size %d with null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingStrings(json.RawMessage(inputWithNulls))
		}
	})
	b.Run(fmt.Sprintf("SanitizeUsingBytes string of size %d with null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingBytes(json.RawMessage(inputWithNulls))
		}
	})
	b.Run(fmt.Sprintf("SanitizeUsingRegexp string of size %d with null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingRegexp(json.RawMessage(inputWithNulls))
		}
	})

	// string without null characters
	input := randomString(size)
	b.Run(fmt.Sprintf("SanitizeUsingStrings string of size %d without null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingStrings(json.RawMessage(input))
		}
	})
	b.Run(fmt.Sprintf("SanitizeUsingBytes string of size %d without null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingBytes(json.RawMessage(input))
		}
	})
	b.Run(fmt.Sprintf("SanitizeUsingRegexp string of size %d without null characters", size), func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			sanitizedJsonUsingRegexp(json.RawMessage(input))
		}
	})
}

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func insertStringInString(input, c string, times int) string {
	if times == 0 {
		return input
	}
	pos := map[int]struct{}{}
	for len(pos) < times {
		newPos := rand.Intn(len(input))
		pos[newPos] = struct{}{}
	}
	keys := make([]int, 0, len(pos))
	for k := range pos {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	offset := len(c)
	for i, idx := range keys {
		oidx := idx + i*offset
		input = input[:oidx] + c + input[oidx:]
	}
	return input
}

func sanitizedJsonUsingStrings(input json.RawMessage) json.RawMessage {
	return json.RawMessage(strings.Replace(string(input), `\u0000`, "", -1))
}

func sanitizedJsonUsingBytes(input json.RawMessage) json.RawMessage {
	return bytes.ReplaceAll(input, []byte(`\u0000`), []byte(""))
}

var sanitizeRegexp = regexp.MustCompile(`\\u0000`)

func sanitizedJsonUsingRegexp(input json.RawMessage) json.RawMessage {
	return json.RawMessage(sanitizeRegexp.ReplaceAllString(string(input), ""))
}

func setSkipZeroAssertionForMultitenant(b bool) {
	skipZeroAssertionForMultitenant = b
}
