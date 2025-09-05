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

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/controlplane"
	proto "github.com/rudderlabs/rudder-server/proto/processor"
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
