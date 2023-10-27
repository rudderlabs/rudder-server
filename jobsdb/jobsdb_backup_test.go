package jobsdb

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/testhelper"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

func TestBackupTable(t *testing.T) {
	var (
		tc                       backupTestCase
		prefix                   = "some-prefix"
		minioResource            *resource.MinioResource
		goldenFileJobsFileName   = "testdata/backupJobs.json.gz"
		goldenFileStatusFileName = "testdata/backupStatus.json.gz"
	)

	// running minio container on docker
	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")
	cleanup := &testhelper.Cleanup{}
	defer cleanup.Run()

	postgresResource, err := resource.SetupPostgres(pool, cleanup)
	require.NoError(t, err)

	minioResource, err = resource.SetupMinio(pool, cleanup)
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
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.maxDSSize"), "10")
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.migrateDSLoopSleepDuration"), "3")
		t.Setenv("JOBS_BACKUP_BUCKET", minioResource.BucketName)
		t.Setenv("JOBS_BACKUP_PREFIX", prefix)

		t.Setenv("MINIO_ENDPOINT", minioResource.Endpoint)
		t.Setenv("MINIO_ACCESS_KEY_ID", minioResource.AccessKeyID)
		t.Setenv("MINIO_SECRET_ACCESS_KEY", minioResource.AccessKeySecret)
		t.Setenv("MINIO_SSL", "false")

		t.Setenv("JOBS_DB_DB_NAME", postgresResource.Database)
		t.Setenv("JOBS_DB_NAME", postgresResource.Database)
		t.Setenv("JOBS_DB_HOST", postgresResource.Host)
		t.Setenv("JOBS_DB_PORT", postgresResource.Port)
		t.Setenv("JOBS_DB_USER", postgresResource.User)
		t.Setenv("JOBS_DB_PASSWORD", postgresResource.Password)

		initJobsDB()
	}

	t.Log("reading jobs")
	jobs, err := tc.readGzipJobFile(goldenFileJobsFileName)
	require.NoError(t, err, "expected no error while reading golden jobs file")

	statusList, err := tc.readGzipStatusFile(goldenFileStatusFileName)
	require.NoError(t, err, "expected no error while reading golden status file")

	// insert duplicates in status table to verify that only latest 2 status of each job is backed up
	duplicateStatusList := duplicateStatuses(statusList, 3)

	fileUploaderProvider := fileuploader.NewDefaultProvider()

	// batch_rt jobsdb is taking a full backup
	tc.insertBatchRTData(t, jobs, duplicateStatusList, cleanup, fileUploaderProvider)

	// rt jobsdb is taking a backup of failed jobs only
	tc.insertRTData(t, jobs, statusList, cleanup, fileUploaderProvider)
	require.NoError(t, err, "expected no error while inserting rt data")

	// create a filemanager instance
	fm, err := filemanager.New(&filemanager.Settings{
		Provider: "MINIO",
		Config: map[string]interface{}{
			"bucketName":      minioResource.BucketName,
			"prefix":          prefix,
			"endPoint":        minioResource.Endpoint,
			"accessKeyID":     minioResource.AccessKeyID,
			"secretAccessKey": minioResource.AccessKeySecret,
			"useSSL":          false,
		},
	})
	require.NoError(t, err, "expected no error while creating file manager")

	// wait for the backup to finish
	var file []*filemanager.FileInfo
	require.Eventually(t, func() bool {
		file, err = fm.ListFilesWithPrefix(context.Background(), "", prefix, 5).Next()

		if len(file) != 3 {
			t.Log("file list: ", file, " err: ", err)
			fm, _ = filemanager.New(&filemanager.Settings{
				Logger:   logger.NOP,
				Provider: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource.BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource.Endpoint,
					"accessKeyID":     minioResource.AccessKeyID,
					"secretAccessKey": minioResource.AccessKeySecret,
					"useSSL":          false,
				},
			})
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
		require.Contains(t, filename, "defaultWorkspaceID-1")
	}

	// Verify aborted jobs backup
	f := tc.downloadFile(t, fm, abortedJobsBackupFilename, cleanup)
	abortedJobs, abortedStatus := tc.getJobsFromAbortedJobs(t, f)
	require.Equal(t, jobs, abortedJobs, "expected jobs to be same in case of only aborted backup")
	require.Equal(t, statusList, abortedStatus, "expected status to be same in case of only aborted backup")

	// Verify full backup of job statuses
	f = tc.downloadFile(t, fm, jobStatusBackupFilename, cleanup)
	backedupStatus, err := tc.readGzipStatusFile(f.Name())
	require.NoError(t, err, "expected no error while reading backedup status file")
	goldenStatusFile, err := tc.readGzipStatusFile(goldenFileStatusFileName)
	require.NoError(t, err, "expected no error while reading golden status file")
	verifyStatus(t, backedupStatus, goldenStatusFile)
	require.Equal(t, len(goldenStatusFile), len(backedupStatus), "expected status files to be same")

	// Verify full backup of jobs
	f = tc.downloadFile(t, fm, jobsBackupFilename, cleanup)
	backedupJobs, err := tc.readGzipJobFile(f.Name())
	require.NoError(t, err, "expected no error while reading backedup status file")
	goldenFileJobs, err := tc.readGzipJobFile(goldenFileJobsFileName)
	require.NoError(t, err, "expected no error while reading golden status file")
	require.Equal(t, goldenFileJobs, backedupJobs, "expected jobs files to be same")
}

func TestMultipleWorkspacesBackupTable(t *testing.T) {
	var (
		tc                       backupTestCase
		prefix                   = "some-prefix"
		minioResource            []*resource.MinioResource
		goldenFileJobsFileName   = "testdata/MultiWorkspaceBackupJobs.json.gz"
		goldenFileStatusFileName = "testdata/MultiWorkspaceBackupStatus.json.gz"
		uniqueWorkspaces         = 3
	)

	// running minio container on docker
	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")
	cleanup := &testhelper.Cleanup{}
	defer cleanup.Run()

	postgresResource, err := resource.SetupPostgres(pool, cleanup)
	require.NoError(t, err)

	minioResource = make([]*resource.MinioResource, uniqueWorkspaces)
	for i := 0; i < uniqueWorkspaces; i++ {
		minioResource[i], err = resource.SetupMinio(pool, cleanup)
		require.NoError(t, err)
	}

	// create a unique temporary directory to allow for parallel test execution
	tmpDir := t.TempDir()

	{ // skipcq: CRT-A0008
		t.Setenv("RSERVER_JOBS_DB_BACKUP_ENABLED", "true")
		t.Setenv("RSERVER_JOBS_DB_BACKUP_RT_FAILED_ONLY", "true")
		t.Setenv("RSERVER_JOBS_DB_BACKUP_BATCH_RT_ENABLED", "true")
		t.Setenv("RSERVER_JOBS_DB_BACKUP_RT_ENABLED", "true")
		t.Setenv("JOBS_BACKUP_BUCKET", "backup-test")
		t.Setenv("RUDDER_TMPDIR", tmpDir)
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.maxDSSize"), "10")
		t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.migrateDSLoopSleepDuration"), "3")
		t.Setenv("MINIO_SSL", "false")

		t.Setenv("JOBS_DB_DB_NAME", postgresResource.Database)
		t.Setenv("JOBS_DB_NAME", postgresResource.Database)
		t.Setenv("JOBS_DB_HOST", postgresResource.Host)
		t.Setenv("JOBS_DB_PORT", postgresResource.Port)
		t.Setenv("JOBS_DB_USER", postgresResource.User)
		t.Setenv("JOBS_DB_PASSWORD", postgresResource.Password)

		initJobsDB()
	}

	t.Log("reading jobs")
	jobs, err := tc.readGzipJobFile(goldenFileJobsFileName)
	require.NoError(t, err, "expected no error while reading golden jobs file")

	jobsByWorkspace := getJobsByWorkspace(jobs)

	statusList, err := tc.readGzipStatusFile(goldenFileStatusFileName)
	require.NoError(t, err, "expected no error while reading golden status file")

	jobStatusByWorkspace := getStatusByWorkspace(statusList, jobsByWorkspace)

	// insert duplicates in status table to verify that only latest 2 status of each job is backed up
	duplicateStatusList := duplicateStatuses(statusList, 3)

	storageSettings := map[string]fileuploader.StorageSettings{
		"defaultWorkspaceID-1": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[0].BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource[0].Endpoint,
					"accessKeyID":     minioResource[0].AccessKeyID,
					"secretAccessKey": minioResource[0].AccessKeySecret,
				},
			},
			Preferences: backendconfig.StoragePreferences{
				GatewayDumps:     false,
				BatchRouterDumps: true,
				RouterDumps:      true,
				ProcErrorDumps:   true,
			},
		},
		"defaultWorkspaceID-2": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[1].BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource[1].Endpoint,
					"accessKeyID":     minioResource[1].AccessKeyID,
					"secretAccessKey": minioResource[1].AccessKeySecret,
				},
			},
			Preferences: backendconfig.StoragePreferences{
				GatewayDumps:     true,
				BatchRouterDumps: true,
				RouterDumps:      true,
				ProcErrorDumps:   false,
			},
		},
		"defaultWorkspaceID-3": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[2].BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource[2].Endpoint,
					"accessKeyID":     minioResource[2].AccessKeyID,
					"secretAccessKey": minioResource[2].AccessKeySecret,
				},
			},
			Preferences: backendconfig.StoragePreferences{
				GatewayDumps:     true,
				BatchRouterDumps: true,
				RouterDumps:      true,
				ProcErrorDumps:   true,
			},
		},
	}

	fileuploaderProvider := fileuploader.NewStaticProvider(storageSettings)

	// batch_rt jobsdb is taking a full backup
	tc.insertBatchRTData(t, jobs, duplicateStatusList, cleanup, fileuploaderProvider)

	// rt jobsdb is taking a backup of failed jobs only
	tc.insertRTData(t, jobs, statusList, cleanup, fileuploaderProvider)
	require.NoError(t, err, "expected no error while inserting rt data")

	// wait for the backup to finish
	for i := 0; i < uniqueWorkspaces; i++ {
		workspace := "defaultWorkspaceID-" + strconv.Itoa(i+1)
		fm, err := fileuploaderProvider.GetFileManager(workspace)
		require.NoError(t, err)
		var file []*filemanager.FileInfo
		require.Eventually(t, func() bool {
			file, err = fm.ListFilesWithPrefix(context.Background(), "", prefix, 10).Next()

			if len(file) != 3 {
				t.Log("file list: ", file, " err: ", err, "len: ", len(file))
				fm, err = fileuploaderProvider.GetFileManager(workspace)
				require.NoError(t, err)
				return false
			}
			return true
		}, 30*time.Second, 1*time.Second, fmt.Errorf("less than 3 backup files found in backup store for workspace:%s. Error: %w ", workspace, err))
		t.Log("file list: ", file, " err: ", err)

		var jobStatusBackupFilename, jobsBackupFilename, abortedJobsBackupFilename string
		for j := 0; j < len(file); j++ {
			filename := file[j].Key
			if strings.Contains(filename, "batch_rt") {
				if strings.Contains(filename, "status") {
					jobStatusBackupFilename = filename
				} else {
					jobsBackupFilename = filename
				}
			} else if strings.Contains(filename, "aborted") {
				abortedJobsBackupFilename = filename
			}
			require.Contains(t, filename, "defaultWorkspaceID-"+strconv.Itoa(i+1))
		}
		require.NotEmpty(t, jobStatusBackupFilename)
		require.NotEmpty(t, jobsBackupFilename)
		require.NotEmpty(t, abortedJobsBackupFilename)

		// Verify aborted jobs backup
		f := tc.downloadFile(t, fm, abortedJobsBackupFilename, cleanup)
		abortedJobs, abortedStatus := tc.getJobsFromAbortedJobs(t, f)
		require.NotZero(t, len(abortedJobs))
		require.NotZero(t, len(abortedStatus))
		require.Equal(t, jobsByWorkspace[workspace], abortedJobs, "expected jobs to be same in case of only aborted backup")
		require.Equal(t, jobStatusByWorkspace[workspace], abortedStatus, "expected status to be same in case of only aborted backup")

		// Verify full backup of job statuses
		f = tc.downloadFile(t, fm, jobStatusBackupFilename, cleanup)
		backedupStatus, err := tc.readGzipStatusFile(f.Name())
		require.NotZero(t, len(backedupStatus))
		require.NoError(t, err, "expected no error while reading backedup status file")
		verifyStatus(t, backedupStatus, jobStatusByWorkspace[workspace])
		require.Equal(t, len(jobStatusByWorkspace[workspace]), len(backedupStatus), "expected status files to be same")

		// Verify full backup of jobs
		f = tc.downloadFile(t, fm, jobsBackupFilename, cleanup)
		backedupJobs, err := tc.readGzipJobFile(f.Name())
		require.NotZero(t, len(backedupJobs))
		require.NoError(t, err, "expected no error while reading backedup status file")
		require.Equal(t, jobsByWorkspace[workspace], backedupJobs, "expected jobs files to be same")
	}
}

func verifyStatus(t *testing.T, backedupStatus, goldenStatusFile []*JobStatusT) {
	// verify that the backed up status is same as the golden status
	for i := 0; i < len(goldenStatusFile); i++ {
		require.Equal(t, goldenStatusFile[i], backedupStatus[i], "expected job state to be same")
	}
}

type backupTestCase struct{}

func duplicateStatuses(statusList []*JobStatusT, duplicateCount int) []*JobStatusT {
	var res []*JobStatusT
	res = append(res, statusList...)
	now := time.Now()
	for i := 0; i < duplicateCount; i++ {
		dup := make([]*JobStatusT, len(statusList))
		for j := range dup {
			tmp := *statusList[j]
			dup[j] = &tmp
			newExecTime := now
			dup[j].ExecTime = newExecTime.Add(time.Duration(i) * time.Second)
		}
		res = append(res, dup...)
	}
	return res
}

func (*backupTestCase) insertRTData(t *testing.T, jobs []*JobT, statusList []*JobStatusT, cleanup *testhelper.Cleanup, fileuploader fileuploader.Provider) {
	triggerAddNewDS := make(chan time.Time)

	jobsDB := &Handle{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}
	err := jobsDB.Setup(ReadWrite, false, "rt", []prebackup.Handler{}, fileuploader)
	require.NoError(t, err)

	rtDS := newDataSet("rt", "1")
	err = jobsDB.WithTx(func(tx *Tx) error {
		if err := jobsDB.copyJobsDS(tx, rtDS, jobs); err != nil {
			return err
		}

		return jobsDB.copyJobStatusDS(context.Background(), tx, rtDS, statusList, []string{})
	})
	require.NoError(t, err)

	rtDS2 := newDataSet("rt", "2")
	jobsDB.dsListLock.WithLock(func(l lock.LockToken) {
		jobsDB.addNewDS(l, rtDS2)
	})
	err = jobsDB.WithTx(func(tx *Tx) error {
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

func (*backupTestCase) insertBatchRTData(t *testing.T, jobs []*JobT, statusList []*JobStatusT, cleanup *testhelper.Cleanup, fileUploaderProvider fileuploader.Provider) {
	triggerAddNewDS := make(chan time.Time)
	jobsDB := &Handle{
		TriggerAddNewDS: func() <-chan time.Time {
			return triggerAddNewDS
		},
	}

	err := jobsDB.Setup(ReadWrite, false, "batch_rt", []prebackup.Handler{}, fileUploaderProvider)
	require.NoError(t, err)

	ds := newDataSet("batch_rt", "1")
	err = jobsDB.WithTx(func(tx *Tx) error {
		if err := jobsDB.copyJobsDS(tx, ds, jobs); err != nil {
			t.Log("error while copying jobs to ds: ", err)
			return err
		}
		return jobsDB.copyJobStatusDS(context.Background(), tx, ds, statusList, []string{})
	})
	require.NoError(t, err)

	ds2 := newDataSet("batch_rt", "2")
	jobsDB.dsListLock.WithLock(func(l lock.LockToken) {
		jobsDB.addNewDS(l, ds2)
	})
	err = jobsDB.WithTx(func(tx *Tx) error {
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

	var jobs []*JobT
	var statusList []*JobStatusT

	for sc.Scan() {
		lineByte := sc.Bytes()
		uuid := uuid.MustParse("69359037-9599-48e7-b8f2-48393c019135")
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

func (*backupTestCase) readGzipJobFile(filename string) ([]*JobT, error) {
	file, err := os.Open(filename)
	if err != nil {
		return []*JobT{}, err
	}
	defer func() { _ = file.Close() }()

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

	var jobs []*JobT
	for sc.Scan() {
		lineByte := sc.Bytes()
		uuid := uuid.MustParse("69359037-9599-48e7-b8f2-48393c019135")
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
	defer func() { _ = file.Close() }()

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

	var statusList []*JobStatusT
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

func getJobsByWorkspace(jobs []*JobT) map[string][]*JobT {
	jobsByWorkspace := make(map[string][]*JobT)
	for _, job := range jobs {
		jobsByWorkspace[job.WorkspaceId] = append(jobsByWorkspace[job.WorkspaceId], job)
	}
	return jobsByWorkspace
}

func getStatusByWorkspace(jobStatus []*JobStatusT, jobsByWorkspace map[string][]*JobT) map[string][]*JobStatusT {
	jobStatusByWorkspace := make(map[string][]*JobStatusT)
	jobStatusByJobID := make(map[int64]*JobStatusT)
	for _, status := range jobStatus {
		jobStatusByJobID[status.JobID] = status
	}
	for workspaceId, job := range jobsByWorkspace {
		for _, job := range job {
			if _, ok := jobStatusByJobID[job.JobID]; ok {
				jobStatusByWorkspace[workspaceId] = append(jobStatusByWorkspace[workspaceId], jobStatusByJobID[job.JobID])
			}
		}
	}
	return jobStatusByWorkspace
}

func (jd *Handle) copyJobStatusDS(ctx context.Context, tx *Tx, ds dataSetT, statusList []*JobStatusT, customValFilters []string) (err error) {
	if len(statusList) == 0 {
		return nil
	}
	tags := statTags{CustomValFilters: customValFilters, ParameterFilters: nil}
	_, err = jd.updateJobStatusDSInTx(ctx, tx, ds, statusList, tags)
	if err != nil {
		return err
	}
	// We are manually triggering ANALYZE to help with query planning since a large
	// amount of rows are being copied in the table in a very short time and
	// AUTOVACUUM might not have a chance to do its work before we start querying
	// this table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`ANALYZE %q`, ds.JobStatusTable))
	if err != nil {
		return err
	}

	return nil
}

func (jd *Handle) copyJobsDS(tx *Tx, ds dataSetT, jobList []*JobT) error { // When fixing callers make sure error is handled with assertError
	defer jd.getTimerStat(
		"copy_jobs",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()

	tx.AddSuccessListener(func() {
		jd.invalidateCacheForJobs(ds, jobList)
	})
	return jd.copyJobsDSInTx(tx, ds, jobList)
}

func (*Handle) copyJobsDSInTx(txHandler transactionHandler, ds dataSetT, jobList []*JobT) error {
	var stmt *sql.Stmt
	var err error

	stmt, err = txHandler.Prepare(pq.CopyIn(ds.JobTable, "job_id", "uuid", "user_id", "custom_val", "parameters",
		"event_payload", "event_count", "created_at", "expire_at", "workspace_id"))

	if err != nil {
		return err
	}

	defer func() { _ = stmt.Close() }()

	for _, job := range jobList {
		eventCount := 1
		if job.EventCount > 1 {
			eventCount = job.EventCount
		}

		_, err = stmt.Exec(job.JobID, job.UUID, job.UserID, job.CustomVal, string(job.Parameters),
			string(job.EventPayload), eventCount, job.CreatedAt, job.ExpireAt, job.WorkspaceId)

		if err != nil {
			return err
		}
	}
	if _, err = stmt.Exec(); err != nil {
		return err
	}

	// We are manually triggering ANALYZE to help with query planning since a large
	// amount of rows are being copied in the table in a very short time and
	// AUTOVACUUM might not have a chance to do its work before we start querying
	// this table
	_, err = txHandler.Exec(fmt.Sprintf(`ANALYZE %q`, ds.JobTable))
	return err
}
