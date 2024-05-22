package transformer

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

var transformerEvent = TransformerEvent{
	Message: map[string]interface{}{
		"key": "value",
		"nested": map[string]interface{}{
			"key": "value",
		},
	},
	Metadata: Metadata{
		SourceID:                "source-id",
		SourceName:              "source-name",
		WorkspaceID:             "279L3V7FSpx43LaNJ0nIs9KRaNC",
		Namespace:               "namespace",
		InstanceID:              "instance-id",
		SourceType:              "source-type",
		SourceCategory:          "source-category",
		TrackingPlanId:          "tracking-plan-id",
		TrackingPlanVersion:     1,
		SourceTpConfig:          map[string]map[string]interface{}{"key": {"key": "value"}},
		MergedTpConfig:          map[string]interface{}{"key": "value"},
		DestinationID:           "destination-id",
		JobID:                   1,
		SourceJobID:             "source-job-id",
		SourceJobRunID:          "source-job-run-id",
		SourceTaskRunID:         "source-task-run-id",
		RecordID:                "record-id",
		DestinationType:         "destination-type",
		MessageID:               "message-id",
		OAuthAccessToken:        "oauth-access-token",
		MessageIDs:              []string{"message-id"},
		RudderID:                "rudder-id",
		ReceivedAt:              "2021-09-01T00:00:00Z",
		EventName:               "event-name",
		EventType:               "event-type",
		SourceDefinitionID:      "source-definition-id",
		DestinationDefinitionID: "destination-definition-id",
		TransformationID:        "2gaewjk29B7YCbo4ZdJkpjAVRfX",
		TransformationVersionID: "2gaewirKXdcwxtfjIcPjtBQPAKH",
	},
	Destination: backendconfig.DestinationT{
		ID:   "destination-id",
		Name: "destination-name",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "destination-definition-id",
			Name:        "destination-definition-name",
			DisplayName: "destination-definition-display-name",
			Config:      map[string]interface{}{"key": "value"},
			ResponseRules: map[string]interface{}{
				"key": "value",
			},
		},
		Config:      map[string]interface{}{"key": "value"},
		Enabled:     true,
		WorkspaceID: "279L3V7FSpx43LaNJ0nIs9KRaNC",
		Transformations: []backendconfig.TransformationT{
			{
				VersionID: "2gaz0prkKJfOFo2osPw7PIKyDaY",
				ID:        "2gaewjk29B7YCbo4ZdJkpjAVRfX",
				Config:    map[string]interface{}{"key": "value"},
			},
		},
		IsProcessorEnabled: true,
		RevisionID:         "revision-id",
	},
	Libraries: []backendconfig.LibraryT{
		{
			VersionID: "2gaxuceuCyUbhSOChSbFbOwF8QX",
		},
	},
}

func TestGrpcRequest(t *testing.T) {
	ctx, stopTest := context.WithCancel(context.Background())
	defer stopTest()

	conf := config.New()
	conf.Set("TRANSFORM_GRPC_URL", fmt.Sprintf("localhost:%d", 50051))
	conf.Set("Processor.Transformer.useGrpcClient", true)

	trans := NewTransformer(conf, logger.NOP, stats.NOP)
	response := trans.UserTransform(ctx, []TransformerEvent{transformerEvent}, 10)

	transformerEvent.Metadata.SourceName = ""
	transformerEvent.Metadata.TransformationVersionID = ""
	require.EqualValues(t,
		Response{
			Events: []TransformerResponse{
				{
					Output:           transformerEvent.Message,
					Metadata:         transformerEvent.Metadata,
					StatusCode:       http.StatusOK,
					Error:            "",
					ValidationErrors: []ValidationError{},
				},
			},
		},
		response)
}
