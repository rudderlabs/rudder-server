package reporting

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func TestConfigSubscriber(t *testing.T) {
	workspaceID := "test-workspace-id"
	sourceID := "test-source-id"
	destinationID := "test-destination-id"
	destinationDefinitionID := "test-destination-definition-id"
	destType := "test-dest-type"

	unknownWokspaceID := "unknown-workspace-id"
	unknownSourceID := "unknown-source-id"
	unknownDestinationID := "unknown-destination-id"

	otherWorkspaceID := "other-workspace-id"
	otherSourceID := "other-source-id"
	otherDestinationID := "other-destination-id"
	otherDestinationDefinitionID := "other-destination-definition-id"

	t.Run("single workspace", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
		mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{
				Data: map[string]backendconfig.ConfigT{
					workspaceID: {
						WorkspaceID: workspaceID,
						Sources: []backendconfig.SourceT{
							{
								ID:      sourceID,
								Enabled: true,
								Destinations: []backendconfig.DestinationT{
									{
										ID:      destinationID,
										Enabled: true,
										DestinationDefinition: backendconfig.DestinationDefinitionT{
											ID:   destinationDefinitionID,
											Name: destType,
										},
									},
								},
							},
						},
						Settings: backendconfig.Settings{
							DataRetention: backendconfig.DataRetention{
								DisableReportingPII: true,
							},
						},
					},
				},
				Topic: string(backendconfig.TopicBackendConfig),
			}
			close(ch)
			return ch
		}).AnyTimes()

		cs := newConfigSubscriber(logger.NOP)

		ctx, cancel := context.WithCancel(context.Background())

		subscribeDone := make(chan struct{})
		go func() {
			defer close(subscribeDone)

			cs.Subscribe(ctx, mockBackendConfig)
		}()

		cs.Wait()

		require.Equal(t, workspaceID, cs.WorkspaceID())
		require.Equal(t, workspaceID, cs.WorkspaceIDFromSource(sourceID))
		require.Equal(t,
			destDetail{
				destinationDefinitionID: destinationDefinitionID,
				destType:                destType,
			},
			cs.GetDestDetail(destinationID),
		)
		require.True(t, cs.IsPIIReportingDisabled(workspaceID))

		require.Empty(t, cs.GetDestDetail(unknownDestinationID))
		require.Empty(t, cs.WorkspaceIDFromSource(unknownSourceID))
		require.False(t, cs.IsPIIReportingDisabled(unknownWokspaceID))

		cancel()

		<-subscribeDone
	})
	t.Run("multiple workspaces", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
		mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{
				Data: map[string]backendconfig.ConfigT{
					workspaceID: {
						WorkspaceID: workspaceID,
						Sources: []backendconfig.SourceT{
							{
								ID:      sourceID,
								Enabled: true,
								Destinations: []backendconfig.DestinationT{
									{
										ID:      destinationID,
										Enabled: true,
										DestinationDefinition: backendconfig.DestinationDefinitionT{
											ID:   destinationDefinitionID,
											Name: destType,
										},
									},
								},
							},
						},
						Settings: backendconfig.Settings{
							DataRetention: backendconfig.DataRetention{
								DisableReportingPII: true,
							},
						},
					},
					otherWorkspaceID: {
						WorkspaceID: otherWorkspaceID,
						Sources: []backendconfig.SourceT{
							{
								ID:      otherSourceID,
								Enabled: true,
								Destinations: []backendconfig.DestinationT{
									{
										ID:      otherDestinationID,
										Enabled: true,
										DestinationDefinition: backendconfig.DestinationDefinitionT{
											ID:   otherDestinationDefinitionID,
											Name: destType,
										},
									},
								},
							},
						},
						Settings: backendconfig.Settings{
							DataRetention: backendconfig.DataRetention{
								DisableReportingPII: false,
							},
						},
					},
				},
				Topic: string(backendconfig.TopicBackendConfig),
			}
			close(ch)
			return ch
		}).AnyTimes()

		cs := newConfigSubscriber(logger.NOP)

		ctx, cancel := context.WithCancel(context.Background())

		subscribeDone := make(chan struct{})
		go func() {
			defer close(subscribeDone)

			cs.Subscribe(ctx, mockBackendConfig)
		}()

		cs.Wait()

		require.Empty(t, cs.WorkspaceID())
		require.Equal(t, otherWorkspaceID, cs.WorkspaceIDFromSource(otherSourceID))
		require.Equal(t,
			destDetail{
				destinationDefinitionID: otherDestinationDefinitionID,
				destType:                destType,
			},
			cs.GetDestDetail(otherDestinationID),
		)
		require.False(t, cs.IsPIIReportingDisabled(otherWorkspaceID))

		cancel()

		<-subscribeDone
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(ctrl)
		mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{
				Data:  map[string]backendconfig.ConfigT{},
				Topic: string(backendconfig.TopicBackendConfig),
			}
			close(ch)
			return ch
		}).AnyTimes()

		cs := newConfigSubscriber(logger.NOP)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		subscribeDone := make(chan struct{})
		go func() {
			defer close(subscribeDone)

			cs.Subscribe(ctx, mockBackendConfig)
		}()

		cs.Wait()

		require.Empty(t, cs.WorkspaceID())
		require.Empty(t, cs.WorkspaceIDFromSource(sourceID))
		require.Empty(t, cs.GetDestDetail(destinationID))
		require.False(t, cs.IsPIIReportingDisabled(workspaceID))

		<-subscribeDone
	})
}
