package transformer

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	transformerpb "github.com/rudderlabs/rudder-server/proto/transformer"
)

var transformerEvent = TransformerEvent{
	Message: map[string]interface{}{"key": "value"},
	Metadata: Metadata{
		SourceID:                "source-id",
		SourceName:              "source-name",
		WorkspaceID:             "workspace-id",
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
		TraceParent:             "trace-parent",
		MessageIDs:              []string{"message-id"},
		RudderID:                "rudder-id",
		ReceivedAt:              "2021-09-01T00:00:00Z",
		EventName:               "event-name",
		EventType:               "event-type",
		SourceDefinitionID:      "source-definition-id",
		DestinationDefinitionID: "destination-definition-id",
		TransformationID:        "transformation-id",
		TransformationVersionID: "transformation-version-id",
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
		WorkspaceID: "workspace-id",
		Transformations: []backendconfig.TransformationT{
			{
				VersionID: "version-id",
				ID:        "id",
				Config:    map[string]interface{}{"key": "value"},
			},
		},
		IsProcessorEnabled: true,
		RevisionID:         "revision-id",
	},
	Libraries: []backendconfig.LibraryT{
		{
			VersionID: "version-id",
		},
	},
}

var violationErrors = []ValidationError{
	{
		Type:    "type",
		Message: "message",
		Meta:    map[string]string{"key": "value"},
	},
}

type mockGRPCServer struct {
	transformerpb.UnimplementedTransformerServiceServer
}

func (m *mockGRPCServer) Transform(_ context.Context, req *transformerpb.TransformRequest) (*transformerpb.TransformResponse, error) {
	response := &transformerpb.TransformResponse{}
	for _, event := range req.GetEvents() {
		response.Response = append(response.Response, &transformerpb.Response{
			Output:     event.Message,
			Metadata:   event.Metadata,
			StatusCode: http.StatusOK,
			Error:      "error",
			ValidationError: lo.Map(violationErrors, func(violationError ValidationError, _ int) *transformerpb.ValidationError {
				return &transformerpb.ValidationError{
					Type:    violationError.Type,
					Message: violationError.Message,
					Meta:    violationError.Meta,
				}
			}),
		})
	}
	return response, nil
}

func TestGrpcRequest(t *testing.T) {
	tcpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	listener, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(tcpPort)))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, listener.Close())
	}()

	ctx, stopTest := context.WithCancel(context.Background())
	defer stopTest()

	mockServer := &mockGRPCServer{}

	grpcServer := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	transformerpb.RegisterTransformerServiceServer(grpcServer, mockServer)

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer func() {
			grpcServer.Stop()
		}()
		return grpcServer.Serve(listener)
	})

	conf := config.New()
	conf.Set("TRANSFORM_GRPC_URL", fmt.Sprintf("localhost:%d", tcpPort))
	conf.Set("Processor.Transformer.useGrpcClient", true)

	trans := NewTransformer(conf, logger.NOP, stats.NOP)
	response := trans.UserTransform(ctx, []TransformerEvent{transformerEvent}, 10)
	require.Equal(t,
		Response{
			Events: []TransformerResponse{
				{
					Output:           transformerEvent.Message,
					Metadata:         transformerEvent.Metadata,
					StatusCode:       http.StatusOK,
					Error:            "error",
					ValidationErrors: violationErrors,
				},
			},
		},
		response)
}
