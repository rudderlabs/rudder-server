package archiver

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	c "github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/utils/misc"

	arch "github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
)

func TestJobsArchival(t *testing.T) {
	var (
		prefix        = "some-prefix"
		minioResource []*destination.MINIOResource

		// test data - contains jobs from 3 workspaces(1 - 1 source, 2 & 3 - 2 sources each)
		goldenFileJobsFileName = "testdata/MultiWorkspaceBackupJobs.json.gz"
		uniqueWorkspaces       = 3
		sourcesPerWorkspace    = []int{1, 2, 2}
		ctx, cancel            = context.WithCancel(context.Background())
	)
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")
	cleanup := &testhelper.Cleanup{}

	postgresResource, err := resource.SetupPostgres(pool, cleanup)
	require.NoError(t, err, "failed to setup postgres resource")

	minioResource = make([]*destination.MINIOResource, uniqueWorkspaces)
	for i := 0; i < uniqueWorkspaces; i++ {
		minioResource[i], err = destination.SetupMINIO(pool, cleanup)
		require.NoError(t, err, "failed to setup minio resource")
	}

	jobs, err := readGzipJobFile(goldenFileJobsFileName)
	require.NoError(t, err, "failed to read jobs file")

	{
		t.Setenv("MINIO_SSL", "false")
		t.Setenv("JOBS_DB_DB_NAME", postgresResource.Database)
		t.Setenv("JOBS_DB_NAME", postgresResource.Database)
		t.Setenv("JOBS_DB_HOST", postgresResource.Host)
		t.Setenv("JOBS_DB_PORT", postgresResource.Port)
		t.Setenv("JOBS_DB_USER", postgresResource.User)
		t.Setenv("JOBS_DB_PASSWORD", postgresResource.Password)
	}

	arch.Init()
	jobsdb.Init2()
	misc.Init()
	jd := &jobsdb.HandleT{
		TriggerAddNewDS: func() <-chan time.Time {
			return make(chan time.Time)
		},
	}
	require.NoError(t, jd.Setup(
		jobsdb.ReadWrite,
		false,
		"gw",
		[]prebackup.Handler{},
		nil,
	))
	require.NoError(t, jd.Start())

	require.NoError(t, jd.Store(ctx, jobs))

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
				GatewayDumps:     true,
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
					"accessKeyID":     minioResource[1].AccessKey,
					"secretAccessKey": minioResource[1].SecretKey,
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
					"accessKeyID":     minioResource[2].AccessKey,
					"secretAccessKey": minioResource[2].SecretKey,
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

	fileUploaderProvider := fileuploader.NewStaticProvider(storageSettings)
	trigger := make(chan time.Time)
	archiver := New(
		jd,
		fileUploaderProvider,
		c.New(),
		stats.Default,
		WithArchiveTrigger(
			func() <-chan time.Time {
				return trigger
			},
		),
	)

	require.NoError(t, archiver.Start())

	trigger <- time.Now()

	require.Eventually(
		t,
		func() bool {
			succeeded, err := jd.GetProcessed(
				ctx,
				jobsdb.GetQueryParamsT{
					IgnoreCustomValFiltersInQuery: true,
					StateFilters:                  []string{jobsdb.Succeeded.State},
					JobsLimit:                     1000,
					EventsLimit:                   1000,
					PayloadSizeLimit:              bytesize.GB,
				},
			)
			require.NoError(t, err)
			return len(jobs) == len(succeeded.Jobs)
		},
		30*time.Second,
		1*time.Second,
	)

	downloadedJobs := make([]*jobsdb.JobT, 0)
	for i := 0; i < uniqueWorkspaces; i++ {
		workspace := "defaultWorkspaceID-" + strconv.Itoa(i+1)
		fm, err := fileUploaderProvider.GetFileManager(workspace)
		require.NoError(t, err)
		fileIter := fm.ListFilesWithPrefix(context.Background(), "", prefix, 20)
		files, err := getAllFileNames(fileIter)
		require.NoError(t, err)
		require.Equal(t, sourcesPerWorkspace[i], len(files))

		for j, file := range files {
			downloadFile, err := os.CreateTemp("", fmt.Sprintf("backedupfile%d%d", i, j))
			require.NoError(t, err)
			err = fm.Download(context.Background(), downloadFile, file)
			require.NoError(t, err, file)
			err = downloadFile.Close()
			require.NoError(t, err)
			dJobs, err := readGzipJobFile(downloadFile.Name())
			require.NoError(t, err)
			cleanup.Cleanup(func() {
				_ = os.Remove(downloadFile.Name())
			})
			downloadedJobs = append(downloadedJobs, dJobs...)
		}
	}
	require.Equal(t, len(jobs), len(downloadedJobs))
	archiver.Stop()
	jd.Stop()
	jd.Close()
	cleanup.Run()
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

	var jobs []*jobsdb.JobT
	for sc.Scan() {
		lineByte := sc.Bytes()
		uuid := uuid.MustParse("69359037-9599-48e7-b8f2-48393c019135")
		j := &jobsdb.JobT{
			UUID:         uuid,
			JobID:        gjson.GetBytes(lineByte, "JobID").Int(),
			UserID:       gjson.GetBytes(lineByte, "UserID").String(),
			CustomVal:    gjson.GetBytes(lineByte, "CustomVal").String(),
			Parameters:   []byte(gjson.GetBytes(lineByte, "Parameters").String()),
			EventCount:   int(gjson.GetBytes(lineByte, "EventCount").Int()),
			WorkspaceId:  gjson.GetBytes(lineByte, "WorkspaceId").String(),
			EventPayload: []byte(gjson.GetBytes(lineByte, "EventPayload").String()),
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

func getAllFileNames(fileIter filemanager.ListSession) ([]string, error) {
	files := make([]string, 0)
	for {
		fileInfo, err := fileIter.Next()
		if err != nil {
			return files, err
		}
		if len(fileInfo) == 0 {
			break
		}
		for _, file := range fileInfo {
			files = append(files, file.Key)
		}
	}
	return files, nil
}
