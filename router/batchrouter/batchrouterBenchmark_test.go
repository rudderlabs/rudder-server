package batchrouter

import (
	"strings"
	"testing"

	jsonstd "encoding/json"

	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksFileManager "github.com/rudderlabs/rudder-server/mocks/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"golang.org/x/sync/errgroup"
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
	return strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
}

func Benchmark_JSONUmarshal(b *testing.B) {
	config.Load()
	logger.Init()
	Init()

	for i := 0; i < b.N; i++ {

		jobs := []*jobsdb.JobT{}
		for i := 0; i < 100; i++ {
			params := JobParametersT{
				EventName: "test",
				EventType: "track",
				MessageID: uuid.Must(uuid.NewV4()).String(),

				ReceivedAt: "2021-01-01T00:00:00Z",
			}

			p, _ := json.Marshal(params)

			jobs = append(jobs, &jobsdb.JobT{
				UUID:       uuid.Must(uuid.NewV4()),
				JobID:      int64(i),
				Parameters: p,
			})
		}

		g := errgroup.Group{}
		g.Go(func() error {
			params := JobParametersT{
				EventName: "test",
				EventType: "track",
				MessageID: string(uuid.Must(uuid.NewV4()).String()),
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
				_ = TryUnmarshalJSON(jobs[i].Parameters, &params)
				// if err != nil {
				// 	b.Fatal(err)
				// }

			}

			return nil
		})

		_ = g.Wait()
	}
}
