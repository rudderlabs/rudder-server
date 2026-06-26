// Package cpservice exposes the processor's control-plane-facing gRPC surface
// (ProcessorService) and opens the dataplane-initiated connection to cp-router
// over which that surface is served.
//
// cp-router reaches the dataplane over a gRPC channel that the dataplane
// *initiates* (yamux/TCP) — the same connection model rudder-sources and
// warehouse use, implemented by the shared controlplane.ConnectionManager. To
// cap the number of inbound connections cp-router must hold (one per dataplane,
// not one per processor replica) and to keep a single owner of any privileged
// work the service does, only the node-0 pod opens this connection — see
// [ShouldConnect].
package cpservice

import (
	"github.com/rudderlabs/rudder-go-kit/logger"

	proto "github.com/rudderlabs/rudder-server/proto/processor"
)

// ServiceName is the cp-router connection-model service token the processor
// registers as. It must match cp-router's allowed-services list.
const ServiceName = "rudderstack-processor"

// Service implements the ProcessorService gRPC server — the processor's single CP-facing entrypoint.
// TODO: Implement Forward RPC (op routing + HTTP forward to pyt)
// until then the embedded UnimplementedProcessorServiceServer
// makes the service register and serve safely.
type Service struct {
	proto.UnimplementedProcessorServiceServer

	log logger.Logger
}

// NewService builds the ProcessorService gRPC handler.
func NewService(log logger.Logger) *Service {
	return &Service{log: log.Child("cpservice")}
}
