package stash

import (
	"bufio"
	"compress/gzip"
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
)

var prefix = "proc_error_jobs_"

func TestStoreErrorsToObjectStorage(t *testing.T) {
	tmpDir := t.TempDir()
	uniqueWorkspaces := 4

	t.Setenv("RUDDER_TMPDIR", tmpDir)

	// running minio container on docker
	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")
	cleanup := &testhelper.Cleanup{}
	defer cleanup.Run()

	minioResource := make([]*destination.MINIOResource, uniqueWorkspaces)
	for i := 0; i < uniqueWorkspaces; i++ {
		minioResource[i], err = destination.SetupMINIO(pool, cleanup)
		require.NoError(t, err)
	}

	storageSettings := map[string]fileuploader.StorageSettings{
		"defaultWorkspaceID-1": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[0].BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource[0].Endpoint,
					"accessKeyID":     minioResource[0].AccessKey,
					"secretAccessKey": minioResource[0].SecretKey,
				},
			},
			Preferences: backendconfig.StoragePreferences{
				ProcErrors: true,
			},
		},
		"defaultWorkspaceID-2": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[1].BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource[1].Endpoint,
					"accessKeyID":     minioResource[1].AccessKey,
					"secretAccessKey": minioResource[1].SecretKey,
				},
			},
			Preferences: backendconfig.StoragePreferences{
				ProcErrors: true,
			},
		},
		"defaultWorkspaceID-3": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[2].BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource[2].Endpoint,
					"accessKeyID":     minioResource[2].AccessKey,
					"secretAccessKey": minioResource[2].SecretKey,
				},
			},
			Preferences: backendconfig.StoragePreferences{
				ProcErrors: true,
			},
		},
		"defaultWorkspaceID-4": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[3].BucketName,
					"prefix":          prefix,
					"endPoint":        minioResource[3].Endpoint,
					"accessKeyID":     minioResource[3].AccessKey,
					"secretAccessKey": minioResource[3].SecretKey,
				},
			},
			Preferences: backendconfig.StoragePreferences{
				ProcErrors: false,
			},
		},
	}

	fileuploaderProvider := fileuploader.NewStaticProvider(storageSettings)

	jobs := []*jobsdb.JobT{
		{
			WorkspaceId: "defaultWorkspaceID-1",
		},
		{
			WorkspaceId: "defaultWorkspaceID-1",
		},
		{
			WorkspaceId: "defaultWorkspaceID-2",
		},
		{
			WorkspaceId: "defaultWorkspaceID-2",
		},
		{
			WorkspaceId: "defaultWorkspaceID-2",
		},
		{
			WorkspaceId: "defaultWorkspaceID-3",
		},
		{
			WorkspaceId: "defaultWorkspaceID-4",
		},
		{
			WorkspaceId: "defaultWorkspaceID-4",
		},
	}

	st := New()
	st.fileuploader = fileuploaderProvider
	st.logger = logger.NOP

	jobsCount := countJobsByWorkspace(jobs)

	errJobs := st.storeErrorsToObjectStorage(jobs)
	require.Equal(t, uniqueWorkspaces, len(errJobs))

	for i := 0; i < uniqueWorkspaces; i++ {
		workspace := "defaultWorkspaceID-" + strconv.Itoa(i+1)
		fm, err := st.fileuploader.GetFileManager(workspace)
		require.NoError(t, err)
		var file []*filemanager.FileInfo
		require.Eventually(t, func() bool {
			file, err = fm.ListFilesWithPrefix(context.Background(), "", "", 5).Next()
			if !storageSettings[workspace].Preferences.ProcErrors {
				return true
			}
			if len(file) != 1 {
				t.Log("file list: ", file, " err: ", err, "len: ", len(file))
				fm, err = fileuploaderProvider.GetFileManager(workspace)
				require.NoError(t, err)
				return false
			}
			return true
		}, 20*time.Second, 1*time.Second, "no backup files found in backup store: ", err)

		if storageSettings[workspace].Preferences.ProcErrors {
			f := downloadFile(t, fm, file[0].Key, cleanup)
			jobsFromFile, err := readGzipJobFile(f.Name())
			require.NoError(t, err)
			require.NotZero(t, jobsCount[workspace], "jobsCount for workspace: ", workspace, " is zero")
			require.Equal(t, jobsCount[workspace], len(jobsFromFile))
		}
	}

	jobsToFail := []*jobsdb.JobT{
		{
			WorkspaceId: "defaultWorkspaceID-5",
		},
	}

	errJobs = st.storeErrorsToObjectStorage(jobsToFail)
	require.Equal(t, 1, len(errJobs))
	require.Equal(t, errJobs[0].errorOutput.Error, fileuploader.NoStorageForWorkspaceError)
}

func countJobsByWorkspace(jobs []*jobsdb.JobT) map[string]int {
	count := make(map[string]int)
	for _, job := range jobs {
		count[job.WorkspaceId]++
	}
	return count
}

func readGzipJobFile(filename string) ([]*jobsdb.JobT, error) {
	file, err := os.Open(filename)
	if err != nil {
		return []*jobsdb.JobT{}, err
	}
	defer func() { _ = file.Close() }()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return []*jobsdb.JobT{}, err
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := 10240 * 1024 // 10MB
	buf := make([]byte, maxCapacity)
	sc.Buffer(buf, maxCapacity)

	jobs := []*jobsdb.JobT{}
	for sc.Scan() {
		lineByte := sc.Bytes()
		uuid := uuid.MustParse("69359037-9599-48e7-b8f2-48393c019135")
		job := &jobsdb.JobT{
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

func downloadFile(t *testing.T, fm filemanager.FileManager, fileToDownload string, cleanup *testhelper.Cleanup) *os.File {
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
