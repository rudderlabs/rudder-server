package batchrouter

import (
	jsonstd "encoding/json"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksFileManager "github.com/rudderlabs/rudder-server/mocks/services/filemanager"
)

func Benchmark_GetStorageDateFormat(b *testing.B) {
	config.Reset()
	Init()

	mockCtrl := gomock.NewController(b)
	mockFileManager := mocksFileManager.NewMockFileManager(mockCtrl)
	destination := &DestinationT{
		Source:      backendconfig.SourceT{},
		Destination: backendconfig.DestinationT{},
	}
	folderName := ""

	b.SetParallelism(2)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			destination.Destination.ID = randomString()
			destination.Source.ID = randomString()

			mockFileManager.EXPECT().GetConfiguredPrefix().AnyTimes()
			mockFileManager.EXPECT().ListFilesWithPrefix(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			_, _ = GetStorageDateFormat(mockFileManager, destination, folderName)
		}
	})
}

func randomString() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

// Benchmark_JSONUnmarshal tries to reproduce a panic encountered with jsoniter
func Benchmark_JSONUnmarshal(b *testing.B) {
	Init()

	for i := 0; i < b.N; i++ {
		var jobs []*jobsdb.JobT
		for i := 0; i < 100; i++ {
			params := JobParametersT{
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
			params := JobParametersT{
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
				var params JobParametersT
				_ = json.Unmarshal(jobs[i].Parameters, &params)
			}

			return nil
		})
		_ = g.Wait()
	}
}
