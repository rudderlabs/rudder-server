package archiver

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	trand "github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func TestJobsArchival(t *testing.T) {
	var (
		prefixByWorkspace = map[int]string{0: trand.String(10), 1: trand.String(10), 2: trand.String(10)}
		minioResource     []*minio.Resource

		// test data - contains jobs from 3 workspaces(1 - 1 source, 2 & 3 - 2 sources each)
		seedJobsFileName    = "testdata/MultiWorkspaceBackupJobs.json.gz"
		uniqueWorkspaces    = 3
		sourcesPerWorkspace = []int{1, 2, 2}
		ctx, cancel         = context.WithCancel(context.Background())
	)
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err, "Failed to create docker pool")

	postgresResource, err := postgres.Setup(pool, t, postgres.WithShmSize(256*bytesize.MB))
	require.NoError(t, err, "failed to setup postgres resource")
	c := config.New()
	c.Set("DB.name", postgresResource.Database)
	c.Set("DB.host", postgresResource.Host)
	c.Set("DB.port", postgresResource.Port)
	c.Set("DB.user", postgresResource.User)
	c.Set("DB.password", postgresResource.Password)
	misc.Init()

	jd := jobsdb.NewForReadWrite("archiver", jobsdb.WithClearDB(false), jobsdb.WithConfig(c), jobsdb.WithStats(stats.NOP))
	require.NoError(t, jd.Start())

	minioResource = make([]*minio.Resource, uniqueWorkspaces)
	for i := 0; i < uniqueWorkspaces; i++ {
		minioResource[i], err = minio.Setup(pool, t)
		require.NoError(t, err, "failed to setup minio resource")
	}

	jobs, err := readGzipJobFile(seedJobsFileName)
	require.NoError(t, err, "failed to read jobs file")

	require.NoError(t, jd.Store(ctx, jobs))

	storageSettings := map[string]fileuploader.StorageSettings{
		"defaultWorkspaceID-1": {
			Bucket: backendconfig.StorageBucket{
				Type: "MINIO",
				Config: map[string]interface{}{
					"bucketName":      minioResource[0].BucketName,
					"prefix":          prefixByWorkspace[0],
					"endPoint":        minioResource[0].Endpoint,
					"accessKeyID":     minioResource[0].AccessKeyID,
					"secretAccessKey": minioResource[0].AccessKeySecret,
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
					"prefix":          prefixByWorkspace[1],
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
					"prefix":          prefixByWorkspace[2],
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

	fileUploaderProvider := fileuploader.NewStaticProvider(storageSettings)
	trigger := make(chan time.Time)
	archiver := New(
		jd,
		fileUploaderProvider,
		c,
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
			succeeded, err := jd.GetSucceeded(
				ctx,
				jobsdb.GetQueryParams{
					JobsLimit:        1000,
					EventsLimit:      1000,
					PayloadSizeLimit: bytesize.GB,
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
		fm, err := fileUploaderProvider.GetFileManager(ctx, workspace)
		require.NoError(t, err)
		fileIter := fm.ListFilesWithPrefix(context.Background(), "", prefixByWorkspace[i], 20)
		files, err := getAllFileNames(fileIter)
		require.NoError(t, err)
		require.Equal(t, sourcesPerWorkspace[i], len(files),
			fmt.Sprintf("found files: %s for workspace: %s", strings.Join(files, ", "), workspace),
		)

		for j, file := range files {
			downloadFile, err := os.CreateTemp(t.TempDir(), fmt.Sprintf("backedupfile%d%d", i, j))
			require.NoError(t, err)
			err = fm.Download(context.Background(), downloadFile, file)
			require.NoError(t, err, file)
			err = downloadFile.Close()
			require.NoError(t, err)
			dJobs, err := readGzipJobFile(downloadFile.Name())
			require.NoError(t, err)
			downloadedJobs = append(downloadedJobs, dJobs...)
		}
	}
	require.Equal(t, len(jobs), len(downloadedJobs))
	archiver.Stop()
	jd.Stop()
	jd.Close()
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
	defer func() { _ = gz.Close() }()

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

type jdWrapper struct {
	jobsdb.JobsDB
	queries *int32
}

func (jd jdWrapper) GetDistinctParameterValues(context.Context, string) ([]string, error) {
	atomic.AddInt32(jd.queries, 1)
	return []string{}, nil
}

func (jd jdWrapper) GetUnprocessed(
	context.Context,
	jobsdb.GetQueryParams,
) (jobsdb.JobsResult, error) {
	atomic.AddInt32(jd.queries, 1)
	return jobsdb.JobsResult{}, nil
}

func (jd jdWrapper) UpdateJobStatus(
	context.Context,
	[]*jobsdb.JobStatusT,
	[]string,
	[]jobsdb.ParameterFilterT,
) error {
	atomic.AddInt32(jd.queries, 1)
	return nil
}

func TestNoQueriesIfDisabled(t *testing.T) {
	queryCount := int32(0)
	jd := jdWrapper{
		&jobsdb.Handle{},
		&queryCount,
	}
	c := config.New()
	c.Set("archival.Enabled", false)
	c.Set("archival.ArchiveSleepDuration", "1ms")

	arc := New(
		jd,
		fileuploader.NewStaticProvider(map[string]fileuploader.StorageSettings{}),
		c,
		stats.Default,
	)

	require.NoError(t, arc.Start())
	defer arc.Stop()
	time.Sleep(100 * time.Millisecond)
	require.True(t, atomic.LoadInt32(&queryCount) == 0)

	c.Set("archival.Enabled", true)
	time.Sleep(100 * time.Millisecond)
	require.True(t, atomic.LoadInt32(&queryCount) > 0)
	// queries should've happened(only getDistinct) -> since empty list is returned
}
