package batchrouter

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksFileManager "github.com/rudderlabs/rudder-server/mocks/services/filemanager"
	"github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func Benchmark_GetStorageDateFormat(b *testing.B) {
	config.Load()
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

func BenchmarkReproduce(b *testing.B) {
	// SETUP
	initBatchRouter()
	stats.Setup()
	whutils.WarehouseDestinations = []string{whutils.POSTGRES}
	readPerDestination = false
	warehouseServiceMaxRetryTime = 0
	start := time.Now()
	defer func() { b.Logf("Benchmark took %v", time.Since(start)) }()

	// CREATION OF JOBS
	var jobs []*jobsdb.JobT
	for i := 0; i < 1000; i++ {
		params := JobParametersT{
			SourceID:   "my-source-id",
			EventName:  "test",
			EventType:  "track",
			MessageID:  uuid.Must(uuid.NewV4()).String(),
			ReceivedAt: "2021-01-01T00:00:00Z",
		}

		p, _ := json.Marshal(params)
		jobs = append(jobs, &jobsdb.JobT{
			UUID:       uuid.Must(uuid.NewV4()),
			JobID:      int64(i),
			Parameters: p,
			EventPayload: []byte(`{
				"metadata":{
					"table":"test",
					"columns": {}
				}
			}`),
		})
	}

	delta := 100000
	var wg sync.WaitGroup
	wg.Add(delta)

	ctrl := gomock.NewController(b)
	jobsDB := mocksJobsDB.NewMockJobsDB(ctrl)
	jobsDB.EXPECT().GetToRetry(gomock.Any(), gomock.Any()).Return(jobsdb.JobsResult{
		Jobs:          jobs,
		LimitsReached: false,
		EventsCount:   len(jobs),
		PayloadSize:   rand.Int63n(1000000),
	}, nil).AnyTimes()
	jobsDB.EXPECT().WithUpdateSafeTx(gomock.Any()).Do(func(_ func(_ jobsdb.UpdateSafeTx) error) {
		wg.Done()
	}).AnyTimes()
	jobsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	jobsDB.EXPECT().UpdateJobStatus(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mockFileManager := mocksFileManager.NewMockFileManager(ctrl)
	mockFileManager.EXPECT().Upload(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(
		filemanager.UploadOutput{
			Location:   "some-location",
			ObjectName: "some-object-name",
		},
		nil,
	).AnyTimes()
	mockFileManager.EXPECT().GetConfiguredPrefix().AnyTimes()
	mockFileManager.EXPECT().ListFilesWithPrefix(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	fileManagerFactory := mocksFileManager.NewMockFileManagerFactory(ctrl)
	fileManagerFactory.EXPECT().New(gomock.Any()).AnyTimes().Return(mockFileManager, nil)

	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return nil, fmt.Errorf("some error")
			},
			DialTLSContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return nil, fmt.Errorf("some error")
			},
		},
	}

	noOfWorkers := 10
	processQ := make(chan *BatchDestinationDataT, 100)
	br := &HandleT{
		processQ:           processQ,
		jobsDB:             jobsDB,
		errorDB:            jobsDB,
		logger:             logger.NOP{},
		jobdDBMaxRetries:   10,
		noOfWorkers:        noOfWorkers,
		workers:            make([]*workerT, noOfWorkers),
		destType:           whutils.POSTGRES,
		fileManagerFactory: fileManagerFactory,
		netHandle:          httpClient,
	}

	for i := 0; i < noOfWorkers; i++ {
		worker := &workerT{workerID: i, brt: br}
		br.workers[i] = worker
		go worker.workerProcess()
	}

	go func() {
		for i := 0; i < delta; i++ {
			processQ <- &BatchDestinationDataT{
				batchDestination: utils.BatchDestinationT{
					Sources: []backendconfig.SourceT{{ID: "my-source-id"}},
				},
				jobs: jobs,
			}
		}
	}()

	wg.Wait()
}

func TestJsoniter(t *testing.T) {
	buf, err := os.ReadFile("sample.json")
	require.NoError(t, err)

	var (
		wg       sync.WaitGroup
		routines = 1000
		m        = make(map[string]interface{})
		json     = jsoniter.ConfigCompatibleWithStandardLibrary
	)

	wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func() {
			defer wg.Done()
			require.NoError(t, json.Unmarshal(buf, &m))
		}()
	}

	wg.Wait()
}
