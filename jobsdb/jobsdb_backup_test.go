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
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestBackupTable(t *testing.T) {

	var (
		tc               backupTestCase
		database         = "jobsdb"
		DB_DSN           = "root@tcp(127.0.0.1:3306)/service"
		minioEndpoint    string
		bucket           = "backup-test"
		prefix           = "some-prefix"
		region           = "us-east-1"
		accessKeyId      = "MYACCESSKEY"
		secretAccessKey  = "MYSECRETKEY"
		resourcePostgres *dockertest.Resource
	)

	setTestEnvs := func(t *testing.T) {
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
		t.Setenv("JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))
		t.Setenv("JOBS_DB_NAME", "jobsdb")
		t.Setenv("JOBS_DB_USER", "rudder")
		t.Setenv("JOBS_DB_PASSWORD", "password")
	}

	// running minio container on docker
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resourcePostgres, err = pool.Run("postgres", "11-alpine", []string{
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

	goldenFileJobsFileName := "testdata/backupJobs.json.gz"
	goldenFileStatusFileName := "testdata/backupStatus.json.gz"

	t.Log("reading jobs")
	jobs, err := tc.readGzipJobFile(goldenFileJobsFileName)
	require.NoError(t, err, "expected no error while reading golden jobs file")

	statusList, err := tc.readGzipStatusFile(goldenFileStatusFileName)
	require.NoError(t, err, "expected no error while reading golden status file")

	maxDSSize = 10
	batchRTJobsDB, err := tc.insertBatchRTData(t, jobs, statusList)
	require.NoError(t, err, "expected no error while inserting batch data")
	defer batchRTJobsDB.TearDown()

	jobDB_rt, err := tc.insertRTData(t, jobs, statusList)
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
	err = tc.downloadBackedupFiles(t, fm, backedupAbortedOnlyRTJobs, DownloadedAbortedFileName)
	require.NoError(t, err, "expected no error")
	defer os.Remove(DownloadedAbortedFileName)
	abortedJobs, abortedStatus, err := tc.getJobsFromAbortedJobs(t, DownloadedAbortedFileName)
	require.NoError(t, err, "expected no error while getting jobs & status from aborted jobs file")
	require.Equal(t, jobs, abortedJobs, "expected jobs to be same in case of only aborted backup")
	require.Equal(t, statusList, abortedStatus, "expected status to be same in case of only aborted backup")

	DownloadedStausFileName := "downloadedStatus.json.gz"
	err = tc.downloadBackedupFiles(t, fm, backedupStatusFileName, DownloadedStausFileName)
	require.NoError(t, err, "expected no error")
	defer os.Remove(DownloadedStausFileName)
	downloadedStatusFile, err := tc.readGzipFile(DownloadedStausFileName)
	require.NoError(t, err, "expected no error")
	backupStatusFile, err := tc.readGzipFile(goldenFileStatusFileName)
	require.NoError(t, err, "expected no error")
	require.Equal(t, backupStatusFile, downloadedStatusFile, "expected status files to be same")

	DownloadedJobsFileName := "downloadedJobs.json.gz"
	err = tc.downloadBackedupFiles(t, fm, backedupJobsFileName, DownloadedJobsFileName)
	require.NoError(t, err, "expected no error")
	defer os.Remove(DownloadedJobsFileName)
	downloadedJobsFile, err := tc.readGzipFile(DownloadedJobsFileName)
	require.NoError(t, err, "expected no error")
	backupJobsFile, err := tc.readGzipFile(goldenFileJobsFileName)
	require.NoError(t, err, "expected no error")
	require.Equal(t, backupJobsFile, downloadedJobsFile, "expected jobs files to be same")
}

type backupTestCase struct {
}

func (*backupTestCase) insertRTData(t *testing.T, jobs []*JobT, statusList []*JobStatusT) (HandleT, error) {
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

func (*backupTestCase) insertBatchRTData(t *testing.T, jobs []*JobT, statusList []*JobStatusT) (HandleT, error) {
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

func (*backupTestCase) getJobsFromAbortedJobs(t *testing.T, fileName string) ([]*JobT, []*JobStatusT, error) {
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

func (*backupTestCase) downloadBackedupFiles(t *testing.T, fm filemanager.FileManager, fileToDownload, downloadedFileName string) error {
	filePtr, err := os.OpenFile(downloadedFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		t.Log("error while Creating file to download data: ", err)
	}

	err = fm.Download(context.Background(), filePtr, fileToDownload)

	filePtr.Close()
	return err
}

func (*backupTestCase) readGzipFile(filename string) ([]byte, error) {
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

func (*backupTestCase) readGzipJobFile(filename string) ([]*JobT, error) {
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

func (*backupTestCase) readGzipStatusFile(fileName string) ([]*JobStatusT, error) {
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
