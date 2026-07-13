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
	"errors"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/processor/internal/pytdeployer"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/user_transformer"
	proto "github.com/rudderlabs/rudder-server/proto/processor"
)

// ServiceName is the cp-router connection-model service token the processor
// registers as. It must match cp-router's allowed-services list.
const ServiceName = "rudderstack-processor"

// Forwarder forwards a control-plane transformation-test request to a target
// pyt transformer's baseURL, one method per pyt endpoint, returning the pyt
// HTTP status and body. It is satisfied by *user_transformer.Client, reusing the
// processor's existing transformer HTTP client (connection pooling, cold-start
// retry).
//
// baseURL is resolved by the caller: [Service.Forward] passes the freshly
// created ephemeral pyt Service's address for execution ops (test, testRun)
// and the static AST deployment URL for AST ops (testLibrary, extractLibs).
// workspaceID is carried through for logging/metrics only.
//
// NOTE: this path handles Python transformation tests only. JavaScript tests
// stay on rudder-transformer and never reach the processor; routing JS here
// would require its own handling.
type Forwarder interface {
	Test(ctx context.Context, baseURL, workspaceID string, payload []byte) (statusCode int, body []byte, err error)
	TestRun(ctx context.Context, baseURL, workspaceID string, payload []byte) (statusCode int, body []byte, err error)
	TestLibrary(ctx context.Context, baseURL, workspaceID string, payload []byte) (statusCode int, body []byte, err error)
	ExtractLibs(ctx context.Context, baseURL, workspaceID string, payload []byte) (statusCode int, body []byte, err error)
}

// Service implements the ProcessorService gRPC server — the processor's single
// CP-facing entrypoint. Its sole RPC, Forward, routes execution ops (test,
// testRun) to a fresh ephemeral pyt Deployment via deployer — or, for
// config-flagged workspaces, to the workspace's production pyt deployment —
// and AST ops (testLibrary, extractLibs) to the shared static AST deployment,
// then forwards the request over HTTP — see [Service.Forward].
type Service struct {
	proto.UnimplementedProcessorServiceServer

	conf            *config.Config
	log             logger.Logger
	deployer        pytdeployer.Deployer
	forwarder       Forwarder
	staticASTURL    string
	prodURLTemplate string
}

// ServiceOpt overrides a Service dependency, used by tests to inject fakes in
// place of the k8s-backed deployer and HTTP forwarder built by default.
type ServiceOpt func(*Service)

// WithDeployer injects the ephemeral pyt deployer instead of building a
// k8s-backed one.
func WithDeployer(deployer pytdeployer.Deployer) ServiceOpt {
	return func(s *Service) { s.deployer = deployer }
}

// WithForwarder injects the pyt forwarder instead of building a user_transformer client.
func WithForwarder(forwarder Forwarder) ServiceOpt {
	return func(s *Service) { s.forwarder = forwarder }
}

// errDeployerUnavailable marks forward errors caused by the ephemeral deployer
// being unavailable (rather than by a transient forwarding failure), so
// [Service.Forward] can map them to FailedPrecondition instead of Unavailable.
var errDeployerUnavailable = errors.New("ephemeral pyt deployer unavailable")

// unavailableDeployer is wired in when a real, k8s-backed pytdeployer.Deployer
// can't be built (no in-cluster config, k8s API unreachable, ...). It always
// fails RunOnEphemeral with a clear error: pytdeployer has deliberately no
// no-op implementation, since execution ops run untrusted customer code and
// there is no safe fallback target for them. AST ops don't go through the
// deployer at all, so they keep working even when this is wired in.
type unavailableDeployer struct{ err error }

func (u *unavailableDeployer) RunOnEphemeral(
	context.Context, string, func(ctx context.Context, baseURL string) (int, []byte, error),
) (int, []byte, error) {
	return 0, nil, fmt.Errorf("%w: %w", errDeployerUnavailable, u.err)
}

// NewService builds the ProcessorService gRPC handler, a k8s-backed ephemeral
// pyt deployer, and a forwarder over the processor's user_transformer HTTP
// client. When the deployer can't be built, execution ops (test, testRun)
// fail with a clear error but AST ops (testLibrary, extractLibs) keep working
// — see [unavailableDeployer]. Tests override the dependencies via
// [WithDeployer]/[WithForwarder].
func NewService(conf *config.Config, log logger.Logger, stat stats.Stats, opts ...ServiceOpt) *Service {
	s := &Service{
		log:             log.Child("cpservice"),
		conf:            conf,
		staticASTURL:    conf.GetStringVar("", "Processor.pytTestStaticASTURL"),
		prodURLTemplate: conf.GetStringVar(user_transformer.DefaultPerWorkspacePyTURLTemplate, "Processor.UserTransformer.perWorkspacePyTURLTemplate"),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.deployer == nil {
		deployer, err := pytdeployer.New(conf, log)
		if err != nil {
			log.Warnn("ephemeral pyt deployer unavailable; execution test ops will fail", obskit.Error(err))
			deployer = &unavailableDeployer{err: err}
		}
		s.deployer = deployer
	}
	if s.forwarder == nil {
		// The forward path is time-boxed by cp-router's test deadline (the ctx).
		// On the ephemeral path the deployer already waits for pod readiness, so
		// retries only cover residual latency; on the prod-routed path there is
		// no readiness wait — the retries must ride out the production
		// deployment's scale-from-zero cold start, so the budget is sized to
		// out-last any realistic deadline and the ctx is what ends them.
		s.forwarder = user_transformer.New(conf, log, stat,
			user_transformer.WithMaxRetryBackoffInterval(conf.GetReloadableDurationVar(500, time.Millisecond, "Processor.pytTestOverrides.pytTestMaxRetryBackoffInterval")),
			user_transformer.WithMaxRetry(conf.GetReloadableIntVar(60, 1, "Processor.pytTestOverrides.pytTestMaxRetry")))
	}
	return s
}
