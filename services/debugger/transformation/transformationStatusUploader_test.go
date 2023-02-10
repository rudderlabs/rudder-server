package transformationdebugger_test

import (
	"testing"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/stretchr/testify/require"
)

const (
	WriteKeyEnabled       = "enabled-write-key"
	WriteKeyEnabledNoUT   = "enabled-write-key-no-ut"
	WriteKeyEnabledOnlyUT = "enabled-write-key-only-ut"
	WorkspaceID           = "some-workspace-id"
	SourceIDEnabled       = "enabled-source"
	SourceIDDisabled      = "disabled-source"
	DestinationIDEnabledA = "enabled-destination-a" // test destination router
	DestinationIDEnabledB = "enabled-destination-b" // test destination batch router
	DestinationIDDisabled = "disabled-destination"
)

var sampleBackendConfig = backendconfig.ConfigT{
	WorkspaceID: WorkspaceID,
	Sources: []backendconfig.SourceT{
		{
			ID:       SourceIDDisabled,
			WriteKey: WriteKeyEnabled,
			Enabled:  false,
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabled,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledA,
					Name:               "A",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"eventDelivery": true,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
					},
				},
				{
					ID:                 DestinationIDEnabledB,
					Name:               "B",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-b-definition-id",
						Name:        "MINIO",
						DisplayName: "enabled-destination-b-definition-display-name",
						Config:      map[string]interface{}{},
					},
					Transformations: []backendconfig.TransformationT{
						{
							VersionID: "transformation-version-id",
							Config:    map[string]interface{}{"eventDelivery": true},
							ID:        "enabled-id",
						},
					},
				},
				// This destination should receive no events
				{
					ID:                 DestinationIDDisabled,
					Name:               "C",
					Enabled:            false,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-disabled",
						Name:        "destination-definition-name-disabled",
						DisplayName: "destination-definition-display-name-disabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabledNoUT,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledA,
					Name:               "A",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
						Config:      map[string]interface{}{},
					},
				},
				// This destination should receive no events
				{
					ID:                 DestinationIDDisabled,
					Name:               "C",
					Enabled:            false,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-disabled",
						Name:        "destination-definition-name-disabled",
						DisplayName: "destination-definition-display-name-disabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabledOnlyUT,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledB,
					Name:               "B",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-b-definition-id",
						Name:        "MINIO",
						DisplayName: "enabled-destination-b-definition-display-name",
						Config:      map[string]interface{}{},
					},
					Transformations: []backendconfig.TransformationT{
						{
							VersionID: "transformation-version-id",
							Config:    map[string]interface{}{"eventDelivery": true},
							ID:        "enabled-id",
						},
					},
				},
			},
		},
	},
}

func TestLimit(t *testing.T) {
	var (
		singularEvent1 = types.SingularEventT{"payload": "event-1"}
		metadata1      = transformer.MetadataT{MessageID: "message-id-1"}
		singularEvent2 = types.SingularEventT{"payload": "event-2"}
		metadata2      = transformer.MetadataT{MessageID: "message-id-2"}
		singularEvent3 = types.SingularEventT{"payload": "event-3"}
		metadata3      = transformer.MetadataT{MessageID: "message-id-3"}
		singularEvent4 = types.SingularEventT{"payload": "event-4"}
		metadata4      = transformer.MetadataT{MessageID: "message-id-4"}
		now            = time.Now()
		limit          = 1
	)
	t.Run("should limit the received transformation status", func(t *testing.T) {
		tStatus := &transformationdebugger.TransformationStatusT{
			Destination: &sampleBackendConfig.Sources[1].Destinations[1],
			DestID:      sampleBackendConfig.Sources[1].Destinations[1].ID,
			SourceID:    sampleBackendConfig.Sources[1].ID,
			UserTransformedEvents: []transformer.TransformerEventT{
				{
					Message:  singularEvent1,
					Metadata: metadata1,
				},
				{
					Message:  singularEvent2,
					Metadata: metadata2,
				},
			},
			EventsByMessageID: map[string]types.SingularEventWithReceivedAt{
				metadata1.MessageID: {
					SingularEvent: singularEvent1,
					ReceivedAt:    now,
				},
				metadata2.MessageID: {
					SingularEvent: singularEvent2,
					ReceivedAt:    now,
				},
				metadata3.MessageID: {
					SingularEvent: singularEvent3,
					ReceivedAt:    now,
				},
				metadata4.MessageID: {
					SingularEvent: singularEvent4,
					ReceivedAt:    now,
				},
			},
			FailedEvents: []transformer.TransformerResponseT{
				{
					Output:   singularEvent1,
					Metadata: metadata3,
					Error:    "some_error1",
				},
				{
					Output:   singularEvent2,
					Metadata: metadata4,
					Error:    "some_error2",
				},
			},
			UniqueMessageIds: map[string]struct{}{
				metadata1.MessageID: {},
				metadata2.MessageID: {},
				metadata3.MessageID: {},
				metadata4.MessageID: {},
			},
		}
		limitedTStatus := tStatus.Limit(
			1,
			sampleBackendConfig.Sources[1].Destinations[1].Transformations[0],
		)
		require.Equal(t, limit, len(limitedTStatus.UserTransformedEvents))
		require.Equal(t, limit, len(limitedTStatus.FailedEvents))
		require.Equal(t, limit*2, len(limitedTStatus.UniqueMessageIds))
		require.Equal(t, limit*2, len(limitedTStatus.EventsByMessageID))
		require.Equal(
			t,
			[]backendconfig.TransformationT{sampleBackendConfig.Sources[1].Destinations[1].Transformations[0]},
			limitedTStatus.Destination.Transformations,
		)
		require.Equal(
			t,
			[]transformer.TransformerEventT{
				{
					Message:  singularEvent1,
					Metadata: metadata1,
				},
			},
			limitedTStatus.UserTransformedEvents,
		)
		require.Equal(
			t,
			[]transformer.TransformerResponseT{
				{
					Output:   singularEvent1,
					Metadata: metadata3,
					Error:    "some_error1",
				},
			},
			limitedTStatus.FailedEvents,
		)
		require.Equal(
			t,
			map[string]struct{}{
				metadata1.MessageID: {},
				metadata3.MessageID: {},
			},
			limitedTStatus.UniqueMessageIds,
		)
		require.Equal(
			t,
			map[string]types.SingularEventWithReceivedAt{
				metadata1.MessageID: {
					SingularEvent: singularEvent1,
					ReceivedAt:    now,
				},
				metadata3.MessageID: {
					SingularEvent: singularEvent3,
					ReceivedAt:    now,
				},
			},
			limitedTStatus.EventsByMessageID,
		)
	})
}
