package api

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/controlplane"
	"github.com/rudderlabs/rudder-server/processor/stages"
	proto "github.com/rudderlabs/rudder-server/proto/processor"
	"github.com/rudderlabs/rudder-server/utils/shared"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

// ProcessorGRPCServer implements the Processor service
type ProcessorGRPCServer struct {
	proto.UnimplementedProcessorServer
	conf              *config.Config
	logger            logger.Logger
	connectionManager *controlplane.ConnectionManager

	config struct {
		region         string
		cpRouterURL    string
		cpRouterUseTLS bool
		instanceID     string
	}
}

// NewProcessorGRPCServer creates a new processor gRPC server
func NewProcessorGRPCServer(conf *config.Config, logger logger.Logger, statsFactory stats.Stats) (*ProcessorGRPCServer, error) {
	s := &ProcessorGRPCServer{
		conf:   conf,
		logger: logger.Child("processor-grpc"),
	}

	// Load configuration
	s.config.region = conf.GetString("region", "")
	s.config.cpRouterUseTLS = conf.GetBool("CP_ROUTER_USE_TLS", true)
	s.config.cpRouterURL = config.GetString("CP_ROUTER_URL", "https://cp-router.rudderlabs.com")
	s.config.cpRouterURL = strings.ReplaceAll(s.config.cpRouterURL, "https://", "")
	s.config.cpRouterURL = strings.ReplaceAll(s.config.cpRouterURL, "http://", "")
	s.config.instanceID = conf.GetString("INSTANCE_ID", "1")

	// Get connection token and type
	connectionToken, tokenType, _, err := deployment.GetConnectionToken()
	if err != nil {
		return nil, fmt.Errorf("connection token: %w", err)
	}

	// Set up labels
	labels := map[string]string{}
	if s.config.region != "" {
		labels["region"] = s.config.region
	}

	// Create connection manager
	s.connectionManager = &controlplane.ConnectionManager{
		AuthInfo: controlplane.AuthInfo{
			Service:         "processor",
			ConnectionToken: connectionToken,
			InstanceID:      s.config.instanceID,
			TokenType:       tokenType,
			Labels:          labels,
		},
		UseTLS: s.config.cpRouterUseTLS,
		Logger: s.logger,
		Options: []grpc.ServerOption{
			grpc.UnaryInterceptor(statsInterceptor(statsFactory)),
		},
		RegisterService: func(srv *grpc.Server) {
			proto.RegisterProcessorServer(srv, s)
			s.logger.Info("Processor gRPC service registered successfully")
		},
	}

	// Set configuration for connection manager
	s.connectionManager.SetConfig(conf)

	return s, nil
}

// Start starts the processor gRPC service with a persistent connection
func (s *ProcessorGRPCServer) Start(ctx context.Context) {
	// Establish persistent connection to control plane
	s.connectionManager.Apply(s.config.cpRouterURL, true)

	// Wait for context cancellation
	<-ctx.Done()
}

// statsInterceptor provides gRPC stats collection
func statsInterceptor(statsFactory stats.Stats) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		res, err := handler(ctx, req)
		statusCode := codes.Unknown
		if s, ok := status.FromError(err); ok {
			statusCode = s.Code()
		}
		tags := stats.Tags{
			"reqType": info.FullMethod,
			"code":    strconv.Itoa(runtime.HTTPStatusFromCode(statusCode)),
		}
		statsFactory.NewTaggedStat("processor.grpc.response_time", stats.TimerType, tags).Since(start)
		return res, err
	}
}

// TestDataMapper implements the TestDataMapper RPC method
func (s *ProcessorGRPCServer) TestDataMapper(ctx context.Context, req *proto.TestDataMapperRequest) (*proto.TestDataMapperResponse, error) {
	s.logger.Infon("Processing data mapper test request")

	// Extract events from proto Struct messages
	protoEvents := req.GetEvents()
	eventList := make([]*shared.InputEvent, len(protoEvents))

	for i, protoEvent := range protoEvents {
		// Convert structpb.Struct to map[string]interface{}
		var eventData map[string]interface{}
		if protoEvent != nil {
			eventData = protoEvent.AsMap()
		} else {
			eventData = make(map[string]interface{})
		}

		eventList[i] = &shared.InputEvent{
			Event: eventData,
		}
	}

	// Convert proto DataMappings to backend config DataMappings
	dataMappings := convertProtoDataMappings(req.GetDataMappings())

	// Create data mapper stage and process
	dataMapper := stages.NewDataMapperStage(dataMappings)
	processedEvents, err := dataMapper.Process(ctx, eventList)
	if err != nil {
		return nil, fmt.Errorf("failed to process events: %w", err)
	}

	// Convert processed events to structpb.Struct
	transformedEventStructs := make([]*structpb.Struct, len(processedEvents))
	for i, event := range processedEvents {
		// Convert AppliedMappingsMetadata to basic types before structpb conversion
		convertedEvent := convertAppliedMappingsToBasicTypes(event.Event)

		eventStruct, err := structpb.NewStruct(convertedEvent)
		if err != nil {
			return nil, fmt.Errorf("failed to convert event to struct: %w", err)
		}
		transformedEventStructs[i] = eventStruct
	}

	return &proto.TestDataMapperResponse{
		Data: &proto.TestDataMapperData{
			TransformedEvents: transformedEventStructs,
		},
	}, nil
}

// convertProtoDataMappings converts proto DataMappings to backend config DataMappings
func convertProtoDataMappings(protoMappings *proto.DataMappings) backendconfig.DataMappings {
	if protoMappings == nil {
		return backendconfig.DataMappings{}
	}

	dataMappings := backendconfig.DataMappings{
		Events:     make([]backendconfig.Mapping, len(protoMappings.GetEvents())),
		Properties: make([]backendconfig.Mapping, len(protoMappings.GetProperties())),
	}

	for i, event := range protoMappings.GetEvents() {
		dataMappings.Events[i] = backendconfig.Mapping{
			ID:      event.GetId(),
			From:    event.GetFrom(),
			To:      event.GetTo(),
			Enabled: event.GetEnabled(),
		}
	}

	for i, property := range protoMappings.GetProperties() {
		dataMappings.Properties[i] = backendconfig.Mapping{
			ID:      property.GetId(),
			From:    property.GetFrom(),
			To:      property.GetTo(),
			Enabled: property.GetEnabled(),
		}
	}

	return dataMappings
}

// convertAppliedMappingsToBasicTypes converts stages.AppliedMappingsMetadata to basic types
// that can be handled by structpb.NewStruct
func convertAppliedMappingsToBasicTypes(event map[string]interface{}) map[string]interface{} {
	if event == nil {
		return event
	}

	// Check if context contains dataMappings
	if context, exists := event["context"].(map[string]interface{}); exists {
		if dataMappings, exists := context["dataMappings"]; exists {
			// Convert AppliedMappingsMetadata to basic types via JSON
			jsonBytes, err := jsonrs.Marshal(dataMappings)
			if err == nil {
				var basicDataMappings map[string]interface{}
				if jsonrs.Unmarshal(jsonBytes, &basicDataMappings) == nil {
					context["dataMappings"] = basicDataMappings
				}
			}
		}
	}

	return event
}
