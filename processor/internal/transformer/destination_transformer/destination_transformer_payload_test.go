package destination_transformer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestGetRequestPayloadCarriesDestinationVersioningMetadata(t *testing.T) {
	client := &Client{}
	destination := backendconfig.DestinationT{
		ID:      "customerio-prod",
		Name:    "Customer IO Prod",
		Version: 1,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "customerio-definition",
			Name:        "CUSTOMERIO",
			DisplayName: "Customer.io",
			Config: map[string]any{
				"apiVersion": "v2",
			},
			Version: "2.0",
			Versions: map[string]backendconfig.DestinationDefinitionVersionT{
				"1": {
					Version: "1.0",
					Status:  "supported",
					Config: map[string]any{
						"apiVersion": "v1",
					},
					ConfigSchema: map[string]any{
						"type": "object",
					},
					UIConfig: map[string]any{
						"legacy": true,
					},
				},
			},
		},
	}
	events := []types.TransformerEvent{
		{
			Message: map[string]any{"type": "identify"},
			Metadata: types.Metadata{
				SourceID:      "source-1",
				DestinationID: destination.ID,
			},
			Destination: destination,
			Connection: backendconfig.Connection{
				SourceID:      "source-1",
				DestinationID: destination.ID,
			},
		},
	}

	for _, compacted := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "compacted"}[compacted], func(t *testing.T) {
			payload, err := client.getRequestPayload(events, compacted)
			require.NoError(t, err)

			var got []types.TransformerEvent
			if compacted {
				var ctr types.CompactedTransformRequest
				require.NoError(t, jsonrs.Unmarshal(payload, &ctr))
				got = ctr.ToTransformerEvents()
			} else {
				require.NoError(t, jsonrs.Unmarshal(payload, &got))
			}

			require.Len(t, got, 1)
			gotDestination := got[0].Destination
			require.Equal(t, 1, gotDestination.Version, "destination instance version must be sent to transformer")
			require.Equal(t, "2.0", gotDestination.DestinationDefinition.Version)
			require.Equal(t, "1.0", gotDestination.DestinationDefinition.Versions["1"].Version)
			require.Equal(t, map[string]any{"apiVersion": "v1"}, gotDestination.DestinationDefinition.Versions["1"].Config)
		})
	}
}
