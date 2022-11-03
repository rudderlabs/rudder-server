package fileuploader

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/gomega"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mock_backendconfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func TestFileUploaderUpdatingWithConfigBackend(t *testing.T) {
	RegisterTestingT(t)
	ctrl := gomock.NewController(t)
	config := mock_backendconfig.NewMockBackendConfig(ctrl)

	configCh := make(chan pubsub.DataEvent)

	var ready sync.WaitGroup
	ready.Add(2)

	var storageSettings sync.WaitGroup
	storageSettings.Add(1)

	config.EXPECT().Subscribe(
		gomock.Any(),
		gomock.Eq(backendconfig.TopicBackendConfig),
	).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
		ready.Done()
		go func() {
			<-ctx.Done()
			close(configCh)
		}()

		return configCh
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Given I have a service reading from the backend
	service := NewService(ctx, config)
	var err error
	var preferences backendconfig.StoragePreferences

	go func() {
		ready.Done()
		preferences, err = service.GetStoragePreferences("testWorkspaceId-1")
		storageSettings.Done()
	}()

	// When the config backend has not published any event yet
	ready.Wait()
	Expect(preferences).To(BeEquivalentTo(backendconfig.StoragePreferences{}))

	// When user has not configured any storage
	configCh <- pubsub.DataEvent{
		Data: map[string]backendconfig.ConfigT{
			"testWorkspaceId-1": {
				WorkspaceID: "testWorkspaceId-1",
				Settings: backendconfig.Settings{
					DataRetention: backendconfig.DataRetention{
						UseSelfStorage: false,
						StorageBucket:  backendconfig.StorageBucket{},
						StoragePreferences: backendconfig.StoragePreferences{
							ProcErrors:   true,
							GatewayDumps: false,
						},
					},
				},
			},
			"testWorkspaceId-2": {
				WorkspaceID: "testWorkspaceId-2",
				Settings: backendconfig.Settings{
					DataRetention: backendconfig.DataRetention{
						UseSelfStorage: true,
						StorageBucket: backendconfig.StorageBucket{
							Type: "some-type",
							Config: map[string]interface{}{
								"some-key": "some-value",
							},
						},
						StoragePreferences: backendconfig.StoragePreferences{
							ProcErrors:   true,
							GatewayDumps: true,
						},
					},
				},
			},
		},
		Topic: string(backendconfig.TopicBackendConfig),
	}

	storageSettings.Wait()
	Expect(preferences).To(Equal(
		backendconfig.StoragePreferences{
			ProcErrors:   true,
			GatewayDumps: false,
		},
	))

	preferences, err = service.GetStoragePreferences("testWorkspaceId-0")
	Expect(err).To(HaveOccurred())
	Expect(preferences).To(BeEquivalentTo(backendconfig.StoragePreferences{}))
}
