package batchrouter

import (
	"github.com/gofrs/uuid"
	"github.com/golang/mock/gomock"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mocksFileManager "github.com/rudderlabs/rudder-server/mocks/services/filemanager"
	"strings"
	"testing"
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
			mockFileManager.EXPECT().ListFilesWithPrefix(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
			_, _ = GetStorageDateFormat(mockFileManager, destination, folderName)
		}
	})
}

func randomString() string {
	return strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
}
