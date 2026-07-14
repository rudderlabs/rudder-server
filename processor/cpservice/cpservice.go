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
	"context"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/processor/internal/pytscaler"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/user_transformer"
	proto "github.com/rudderlabs/rudder-server/proto/processor"
)

// ServiceName is the cp-router connection-model service token the processor
// registers as. It must match cp-router's allowed-services list.
const ServiceName = "rudderstack-processor"

// Forwarder forwards a control-plane transformation-test request to a workspace's
// per-workspace pyt transformer, one method per pyt endpoint, returning the pyt
// HTTP status and body. It is satisfied by *user_transformer.Client, reusing the
// processor's existing transformer HTTP client (connection pooling, cold-start
// retry).
//
// NOTE: this path handles Python transformation tests only — those are what run
// on the per-workspace pyt deployment. JavaScript tests stay on rudder-transformer
// and never reach the processor; routing JS here would require its own handling.
type Forwarder interface {
	Test(ctx context.Context, workspaceID string, payload []byte) (statusCode int, body []byte, err error)
	TestRun(ctx context.Context, workspaceID string, payload []byte) (statusCode int, body []byte, err error)
	TestLibrary(ctx context.Context, workspaceID string, payload []byte) (statusCode int, body []byte, err error)
	ExtractLibs(ctx context.Context, workspaceID string, payload []byte) (statusCode int, body []byte, err error)
}

// Service implements the ProcessorService gRPC server — the processor's single
// CP-facing entrypoint. Its sole RPC, Forward, scales up the workspace's pyt
// Deployment and forwards the request to it over HTTP (see [Service.Forward]).
type Service struct {
	proto.UnimplementedProcessorServiceServer

	log       logger.Logger
	scaler    pytscaler.Scaler
	forwarder Forwarder
	scaleTo   config.ValueLoader[int]
	// scaleGroup collapses concurrent scale checks for the same workspace so a
	// burst of Forwards triggers one scale-up, not one per request.
	scaleGroup singleflight.Group
}

// ServiceOpt overrides a Service dependency, used by tests to inject fakes in
// place of the k8s-backed scaler and HTTP forwarder built by default.
type ServiceOpt func(*Service)

// WithScaler injects the pyt scaler instead of building a k8s-backed one.
func WithScaler(scaler pytscaler.Scaler) ServiceOpt {
	return func(s *Service) { s.scaler = scaler }
}

// WithForwarder injects the pyt forwarder instead of building a user_transformer client.
func WithForwarder(forwarder Forwarder) ServiceOpt {
	return func(s *Service) { s.forwarder = forwarder }
}

// NewService builds the ProcessorService gRPC handler. By default it builds a
// k8s-backed pyt scaler — falling back to a no-op scaler (forward without
// scaling) when the k8s client can't be built — and a forwarder over the
// processor's user_transformer HTTP client. Tests override these via
// [WithScaler]/[WithForwarder].
func NewService(conf *config.Config, log logger.Logger, stat stats.Stats, opts ...ServiceOpt) *Service {
	s := &Service{
		log:     log.Child("cpservice"),
		scaleTo: conf.GetReloadableIntVar(2, 1, "Processor.UserTransformer.pytTestScaleReplicas"),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.scaler == nil {
		scaler, err := pytscaler.New(conf, log)
		if err != nil {
			log.Warnn("pyt scaler unavailable, forwarding without scaling", obskit.Error(err))
			scaler = pytscaler.NewNoop(log)
		}
		s.scaler = scaler
	}
	if s.forwarder == nil {
		// The forward path is time-boxed by cp-router's ~60s test deadline, so
		// override the event path's large default retry backoff with a short one,
		// and cap the retries, to recover from a pyt cold start quickly within that
		// window (the ctx deadline remains the hard bound).
		s.forwarder = user_transformer.New(conf, log, stat,
			user_transformer.WithMaxRetryBackoffInterval(conf.GetReloadableDurationVar(1, time.Second, "Processor.UserTransformer.pytTestMaxRetryBackoffInterval")),
			user_transformer.WithMaxRetry(conf.GetReloadableIntVar(60, 1, "Processor.UserTransformer.pytTestMaxRetry")))
	}
	return s
}
