package batchrouter

import (
	"context"
	jsonstd "encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
)

func Benchmark_GetStorageDateFormat(b *testing.B) {
	config.Reset()

	destination := &Connection{
		Source:      backendconfig.SourceT{},
		Destination: backendconfig.DestinationT{},
	}
	folderName := ""
	dfProvider := &storageDateFormatProvider{dateFormatsCache: map[string]string{}}
	b.SetParallelism(2)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			destination.Destination.ID = randomString()
			destination.Source.ID = randomString()

			_, _ = dfProvider.GetFormat(logger.NOP, mockFileManager{}, destination, folderName)
		}
	})
}

func randomString() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

// Benchmark_JSONUnmarshal tries to reproduce a panic encountered with jsoniter
func Benchmark_JSONUnmarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var jobs []*jobsdb.JobT
		for i := 0; i < 100; i++ {
			params := routerutils.JobParameters{
				EventName:  "test",
				EventType:  "track",
				MessageID:  uuid.New().String(),
				ReceivedAt: "2021-01-01T00:00:00Z",
			}

			p, _ := json.Marshal(params)
			jobs = append(jobs, &jobsdb.JobT{
				UUID:       uuid.New(),
				JobID:      int64(i),
				Parameters: p,
			})
		}

		g := errgroup.Group{}
		g.Go(func() error {
			params := routerutils.JobParameters{
				EventName: "test",
				EventType: "track",
				MessageID: uuid.New().String(),
			}

			p, _ := json.Marshal(params)
			for i := range jobs {
				err := jsonstd.Unmarshal(p, &jobs[i].Parameters)
				if err != nil {
					b.Fatal(err)
				}

				jobs[i].Parameters = jobs[i].Parameters[:len(p):len(p)]
			}

			return nil
		})
		g.Go(func() error {
			for i := range jobs {
				var params routerutils.JobParameters
				_ = json.Unmarshal(jobs[i].Parameters, &params)
			}

			return nil
		})
		_ = g.Wait()
	}
}

type mockFileManager struct{}

func (m mockFileManager) ListFilesWithPrefix(ctx context.Context, startAfter, prefix string, maxItems int64) filemanager.ListSession {
	return m
}

func (mockFileManager) Next() (fileObjects []*filemanager.FileInfo, err error) {
	return nil, nil
}

func (mockFileManager) Upload(context.Context, *os.File, ...string) (filemanager.UploadedFile, error) {
	return filemanager.UploadedFile{}, nil
}

func (mockFileManager) Download(context.Context, *os.File, string) error {
	return nil
}

func (mockFileManager) Delete(ctx context.Context, keys []string) error {
	return nil
}

func (mockFileManager) Prefix() string {
	return ""
}

func (mockFileManager) SetTimeout(timeout time.Duration) {
	// no-op
}

func (mockFileManager) GetObjectNameFromLocation(string) (string, error) {
	return "", nil
}

func (mockFileManager) GetDownloadKeyFromFileLocation(string) string {
	return ""
}
