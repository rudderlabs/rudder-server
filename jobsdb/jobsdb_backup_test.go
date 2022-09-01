package jobsdb

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
)

func TestBackupTable(t *testing.T) {
	var (
		tc                       backupTestCase
		prefix                   = "some-prefix"
		postgresResource         *destination.PostgresResource
		minioResource            *destination.MINIOResource
		goldenFileJobsFileName   = "testdata/backupJobs.json.gz"
		goldenFileStatusFileName = "testdata/backupStatus.json.gz"
	)

	// running minio container on docker
	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")
	cleanup := &testhelper.Cleanup{}
	defer cleanup.Run()

	postgresResource, err = destination.SetupPostgres(pool, cleanup)
	require.NoError(t, err)

	minioResource, err = destination.SetupMINIO(pool, cleanup)
	require.NoError(t, err)

	// create a unique temporary directory to allow for parallel test execution
	tmpDir := t.TempDir()

	{ // skipcq: CRT-A0008
		t.Setenv("RSERVER_JOBS_DB_BACKUP_ENABLED", "true")
		t.Setenv("RSERVER_JOBS_DB_BACKUP_RT_FAILED_ONLY", "true")
		t.Setenv("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
		t.Setenv("RSERVER_JOBS_DB_BACKUP_BATCH_RT_ENABLED", "true")
		t.Setenv("RSERVER_JOBS_DB_BACKUP_RT_ENABLED", "true")
		t.Setenv("JOBS_BACKUP_BUCKET", "backup-test")
		t.Setenv("RUDDER_TMPDIR", tmpDir)
		t.Setenv(config.TransformKey("JobsDB.maxDSSize"), "10")
		t.Setenv(config.TransformKey("JobsDB.migrateDSLoopSleepDuration"), "3")
		t.Setenv("JOBS_BACKUP_BUCKET", minioResource.BucketName)
		t.Setenv("JOBS_BACKUP_PREFIX", prefix)

		t.Setenv("MINIO_ENDPOINT", minioResource.Endpoint)
		t.Setenv("MINIO_ACCESS_KEY_ID", minioResource.AccessKey)
		t.Setenv("MINIO_SECRET_ACCESS_KEY", minioResource.SecretKey)
		t.Setenv("MINIO_SSL", "false")

		t.Setenv("JOBS_DB_DB_NAME", postgresResource.Database)
		t.Setenv("JOBS_DB_NAME", postgresResource.Database)
		t.Setenv("JOBS_DB_HOST", postgresResource.Host)
		t.Setenv("JOBS_DB_PORT", postgresResource.Port)
		t.Setenv("JOBS_DB_USER", postgresResource.User)
		t.Setenv("JOBS_DB_PASSWORD", postgresResource.Password)
		initJobsDB()
		stats.Setup()
	}

	t.Log("reading jobs")
	jobs, err := tc.readGzipJobFile(goldenFileJobsFileName)
	require.NoError(t, err, "expected no error while reading golden jobs file")

	statusList, err := tc.readGzipStatusFile(goldenFileStatusFileName)
	require.NoError(t, err, "expected no error while reading golden status file")

	// batch_rt jobsdb is taking a full backup
	tc.insertBatchRTData(t, jobs, statusList, cleanup)

	// rt jobsdb is taking a backup of failed jobs only
	tc.insertRTData(t, jobs, statusList, cleanup)
	require.NoError(t, err, "expected no error while inserting rt data")

	// create a filemanager instance
	fmFactory := filemanager.FileManagerFactoryT{}
	fm, err := fmFactory.New(&filemanager.SettingsT{
		Provider: "MINIO",
		Config: map[string]interface{}{
			"bucketName":      minioResource.BucketName,
			"prefix":          prefix,
			"endPoint":        minioResource.Endpoint,
			"accessKeyID":     minioResource.AccessKey,
			"secretAccessKey": minioResource.SecretKey,
			"useSSL":          false,
		},
	})
	require.NoError(t, err, "expected no error while creating file manager")

	// wait for the backup to finish
	var file []*filemanager.FileObject
	require.Eventually(t, func() bool {
		file, err = fm.ListFilesWithPrefix(context.Background(), "", prefix, 5)

		if len(file) != 3 {
			t.Log("file list: ", file, " err: ", err)
			return false
		}
		return true
	}, 20*time.Second, 1*time.Second, "less than 3 backup files found in backup store: ", err)

	var jobStatusBackupFilename, jobsBackupFilename, abortedJobsBackupFilename string
	for i := 0; i < len(file); i++ {
		filename := file[i].Key
		if strings.Contains(filename, "batch_rt") {
			if strings.Contains(filename, "status") {
				jobStatusBackupFilename = filename
			} else {
				jobsBackupFilename = filename
			}
		} else if strings.Contains(filename, "aborted") {
			abortedJobsBackupFilename = filename
		}
	}

	// Verify aborted jobs backup
	f := tc.downloadFile(t, fm, abortedJobsBackupFilename, cleanup)
	abortedJobs, abortedStatus := tc.getJobsFromAbortedJobs(t, f)
	require.Equal(t, jobs, abortedJobs, "expected jobs to be same in case of only aborted backup")
	require.Equal(t, statusList, abortedStatus, "expected status to be same in case of only aborted backup")

	// Verify full backup of job statuses
	f = tc.downloadFile(t, fm, jobStatusBackupFilename, cleanup)
	jobStatusBackupFile := tc.readGzipFile(t, f)
	goldenFile := tc.openFile(t, goldenFileStatusFileName, cleanup)
	goldenStatusFile := tc.readGzipFile(t, goldenFile)
	require.Equal(t, goldenStatusFile, jobStatusBackupFile, "expected status files to be same")

	// Verify full backup of jobs
	f = tc.downloadFile(t, fm, jobsBackupFilename, cleanup)
	downloadedJobsFile := tc.readGzipFile(t, f)
	goldenFile = tc.openFile(t, goldenFileJobsFileName, cleanup)
	goldenJobsFile := tc.readGzipFile(t, goldenFile)
	require.Equal(t, goldenJobsFile, downloadedJobsFile, "expected jobs files to be same")
}

type backupTestCase struct{}

func (*backupTestCase) insertRTData(t *testing.T, jobs []*JobT, statusList []*JobStatusT, cleanup *testhelper.Cleanup) {
	migrationMode := ""
	queryFilters := QueryFiltersT{
		CustomVal: true,
	}
	triggerAddNewDS := make(chan time.Time)

	jobsDB := &HandleT{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	err := jobsDB.Setup(ReadWrite, false, "rt", migrationMode, true, queryFilters, []prebackup.Handler{})
	require.NoError(t, err)

	rtDS := newDataSet("rt", "1")
	err = jobsDB.WithTx(func(tx *sql.Tx) error {
		if err := jobsDB.copyJobsDS(tx, rtDS, jobs); err != nil {
			return err
		}

		return jobsDB.copyJobStatusDS(context.Background(), tx, rtDS, statusList, []string{})
	})
	require.NoError(t, err)

	rtDS2 := newDataSet("rt", "2")
	jobsDB.dsListLock.WithLock(func(l lock.DSListLockToken) {
		jobsDB.addNewDS(l, rtDS2)
	})
	err = jobsDB.WithTx(func(tx *sql.Tx) error {
		if err := jobsDB.copyJobsDS(tx, rtDS2, jobs); err != nil {
			return err
		}
		return jobsDB.copyJobStatusDS(context.Background(), tx, rtDS2, statusList, []string{})
	})
	require.NoError(t, err)
	cleanup.Cleanup(func() {
		jobsDB.TearDown()
	})
}

func (*backupTestCase) insertBatchRTData(t *testing.T, jobs []*JobT, statusList []*JobStatusT, cleanup *testhelper.Cleanup) {
	migrationMode := ""

	triggerAddNewDS := make(chan time.Time)
	jobsDB := &HandleT{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	queryFilters := QueryFiltersT{
		CustomVal: true,
	}

	err := jobsDB.Setup(ReadWrite, false, "batch_rt", migrationMode, true, queryFilters, []prebackup.Handler{})
	require.NoError(t, err)

	ds := newDataSet("batch_rt", "1")
	err = jobsDB.WithTx(func(tx *sql.Tx) error {
		if err := jobsDB.copyJobsDS(tx, ds, jobs); err != nil {
			t.Log("error while copying jobs to ds: ", err)
			return err
		}
		return jobsDB.copyJobStatusDS(context.Background(), tx, ds, statusList, []string{})
	})
	require.NoError(t, err)

	ds2 := newDataSet("batch_rt", "2")
	jobsDB.dsListLock.WithLock(func(l lock.DSListLockToken) {
		jobsDB.addNewDS(l, ds2)
	})
	err = jobsDB.WithTx(func(tx *sql.Tx) error {
		if err := jobsDB.copyJobsDS(tx, ds2, jobs); err != nil {
			t.Log("error while copying jobs to ds: ", err)
			return err
		}

		return jobsDB.copyJobStatusDS(context.Background(), tx, ds2, statusList, []string{})
	})
	require.NoError(t, err)
	cleanup.Cleanup(func() {
		jobsDB.TearDown()
	})
}

func (*backupTestCase) getJobsFromAbortedJobs(t *testing.T, file *os.File) ([]*JobT, []*JobStatusT) {
	t.Log(file.Name())
	gz, err := gzip.NewReader(file)
	require.NoError(t, err, "expected no error")
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
		require.NoError(t, err, "expected no error")
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
	return jobs, statusList
}

func (*backupTestCase) downloadFile(t *testing.T, fm filemanager.FileManager, fileToDownload string, cleanup *testhelper.Cleanup) *os.File {
	file, err := os.CreateTemp("", "backedupfile")
	require.NoError(t, err, "expected no error while creating temporary file")

	err = fm.Download(context.Background(), file, fileToDownload)
	require.NoError(t, err)

	// reopening the file so to reset the pointer
	// since file.Seek(0, io.SeekStart) doesn't work
	file.Close()
	file, err = os.Open(file.Name())
	require.NoError(t, err, "expected no error while reopening downloaded file")

	require.NoError(t, err)
	cleanup.Cleanup(func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	})
	return file
}

func (*backupTestCase) openFile(t *testing.T, filename string, cleanup *testhelper.Cleanup) *os.File {
	f, err := os.Open(filename)
	require.NoError(t, err, "expected no error")
	cleanup.Cleanup(func() {
		f.Close()
	})
	return f
}

func (*backupTestCase) readGzipFile(t *testing.T, file *os.File) []byte {
	gz, err := gzip.NewReader(file)
	require.NoError(t, err)
	defer gz.Close()

	r, err := io.ReadAll(gz)
	require.NoError(t, err)
	return r
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
