package cpservice

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/user_transformer"
	proto "github.com/rudderlabs/rudder-server/proto/processor"
)

// endpointForward is a Forwarder method bound to a single pyt endpoint,
// parameterized over the target baseURL so the same function value can be
// pointed at either a freshly created ephemeral pyt Service or the shared
// static AST deployment.
type endpointForward func(ctx context.Context, baseURL, workspaceID string, payload []byte) (int, []byte, error)

// validWorkspaceID gates what Forward substitutes into the pyt URL template and
// the pyt Deployment name. Anything beyond alphanumerics and hyphens must be
// rejected here: URL-significant characters (@, #, ?, /, :) surviving into the
// template would let a request redirect the forward to an arbitrary host
// (SSRF). The first and last characters must be alphanumeric: the ID becomes a
// k8s label value (and part of a DNS-1123 name), where a leading/trailing
// hyphen is rejected by the apiserver as a non-transient Create failure. 59
// keeps "pyt-" + the lowercased ID within the 63-char DNS label limit the
// Deployment name is subject to anyway.
var validWorkspaceID = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,57}[a-zA-Z0-9])?$`)

// Forward is the processor's only CP-facing RPC. It maps req.Op to the matching
// pyt endpoint and forwards req.Payload to it, returning the pyt response status
// and body unchanged.
//
// Execution ops (test, testRun) run arbitrary customer Python, so each request
// gets its own fresh ephemeral pyt Deployment + Service via s.deployer,
// created just for this call and deleted best-effort afterwards — see
// [pytdeployer.Deployer.RunOnEphemeral]. Workspaces flagged in the
// Processor.pytTestOverrides.routeToProduction map are the exception: their
// tests forward to the workspace's production pyt deployment instead (see
// [Service.isProdRouted]).
// AST ops (testLibrary, extractLibs) never execute user code; they go straight
// to the shared static AST deployment with no k8s API involvement.
func (s *Service) Forward(ctx context.Context, req *proto.ForwardRequest) (*proto.ForwardResponse, error) {
	start := time.Now()
	defer func() {
		s.log.Debugn(
			"time to forward to pyt",
			obskit.WorkspaceID(req.WorkspaceId),
			logger.NewStringField("op", req.Op.String()),
			logger.NewDurationField("duration", time.Since(start)),
		)
	}()
	forward, isExecutionOp, ok := s.forwardForOp(req.Op)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown op %v", req.Op)
	}
	if !validWorkspaceID.MatchString(req.WorkspaceId) {
		return nil, status.Error(codes.InvalidArgument, "workspaceId must be 1-59 alphanumeric or hyphen characters, starting and ending with an alphanumeric")
	}

	route := "static"
	if isExecutionOp {
		if s.isProdRouted(req.WorkspaceId) {
			route = "production"
		} else {
			route = "ephemeral"
		}
	}
	tags := stats.Tags{
		"op":          req.Op.String(),
		"route":       route,
		"workspaceId": req.WorkspaceId,
	}
	// timedForward measures just the HTTP request to the pyt pod and emits the
	// request timer itself, so on the ephemeral route it excludes the
	// deployment creation and readiness wait (which RunOnEphemeral performs
	// before invoking it) that the total timer includes — and a deployer
	// failure that never invokes it produces no request sample.
	timedForward := func(ctx context.Context, baseURL string) (int, []byte, error) {
		execStart := time.Now()
		statusCode, body, err := forward(ctx, baseURL, req.WorkspaceId, req.Payload)
		s.stat.NewTaggedStat("processor_pyt_request_handle_time", stats.TimerType, tags).Since(execStart)
		return statusCode, body, err
	}

	var statusCode int
	var body []byte
	var err error
	switch route {
	case "production":
		// Config-flagged workspace: its tests must run with prod-only config
		// (custom DNS, pinned egress IPs, ...) the generic ephemeral spec can't
		// reproduce, so forward straight to the workspace's production pyt
		// deployment. No readiness wait — the forwarder's cold-start retries
		// ride out the prod deployment's scale-from-zero window.
		statusCode, body, err = timedForward(ctx,
			user_transformer.PerWorkspacePyTBaseURL(s.prodURLTemplate, req.WorkspaceId))
	case "ephemeral":
		statusCode, body, err = s.deployer.RunOnEphemeral(ctx, req.WorkspaceId, timedForward)
	default:
		statusCode, body, err = timedForward(ctx, s.staticASTURL)
	}
	s.stat.NewTaggedStat("processor_pyt_forward_rpc_time", stats.TimerType, tags).Since(start)
	if err != nil {
		s.log.Warnn("forwarding to pyt",
			obskit.WorkspaceID(req.WorkspaceId), logger.NewStringField("op", req.Op.String()), obskit.Error(err))
		// Misconfiguration (no deployer, no static AST URL) is a precondition
		// failure, not a transient outage — callers must not retry it away.
		if errors.Is(err, errDeployerUnavailable) || errors.Is(err, user_transformer.ErrEmptyForwardBaseURL) {
			return nil, status.Errorf(codes.FailedPrecondition, "forwarding to pyt: %v", err)
		}
		return nil, status.Errorf(codes.Unavailable, "forwarding to pyt: %v", err)
	}
	return &proto.ForwardResponse{
		StatusCode: int32(statusCode),
		Body:       body,
	}, nil
}

// isProdRouted reports whether the workspace's test traffic is config-flagged
// to run on its production pyt deployment instead of an ephemeral one. The
// flags live in a single reloadable map — registered once in [NewService], so
// no per-workspace vars accumulate in the config registry —
// Processor.pytTestOverrides.routeToProduction, keyed by workspace ID with
// "*" flagging every workspace at once; a workspace's own entry wins over
// "*", so `{"*": true, "ws-1": false}` exempts ws-1. The config layer
// lowercases map keys, so the lookup is case-insensitive, mirroring the
// lowercased deployment-name convention.
func (s *Service) isProdRouted(workspaceID string) bool {
	flags := s.prodRouted.Load()
	if v, ok := flags[strings.ToLower(workspaceID)]; ok {
		return flagValue(v)
	}
	v, ok := flags["*"]
	return ok && flagValue(v)
}

// flagValue interprets a routeToProduction map entry as a bool, whichever form
// the config source delivered it in (bool from YAML, string from env).
func flagValue(v any) bool {
	b, err := strconv.ParseBool(fmt.Sprint(v))
	return err == nil && b
}

// forwardForOp resolves a control-plane op to the Forwarder method that serves
// it and whether that op is an execution op (runs user code: on a fresh
// ephemeral deployment, or on the workspace's production pyt deployment when
// config-flagged) versus an AST-only op (forwarded straight to the static AST
// deployment). Reports ok=false for an unspecified or unknown op.
func (s *Service) forwardForOp(op proto.Op) (fn endpointForward, isExecutionOp, ok bool) {
	switch op {
	case proto.Op_OP_TEST:
		return s.forwarder.Test, true, true
	case proto.Op_OP_TEST_RUN:
		return s.forwarder.TestRun, true, true
	case proto.Op_OP_TEST_LIBRARY:
		return s.forwarder.TestLibrary, false, true
	case proto.Op_OP_EXTRACT_LIBS:
		return s.forwarder.ExtractLibs, false, true
	default:
		return nil, false, false
	}
}
