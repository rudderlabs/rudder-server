package cpservice

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/logger"
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
// (SSRF). 59 keeps "pyt-" + the lowercased ID within the 63-char DNS label
// limit the Deployment name is subject to anyway.
var validWorkspaceID = regexp.MustCompile(`^[a-zA-Z0-9-]{1,59}$`)

// Forward is the processor's only CP-facing RPC. It maps req.Op to the matching
// pyt endpoint and forwards req.Payload to it, returning the pyt response status
// and body unchanged.
//
// Execution ops (test, testRun) run arbitrary customer Python, so each request
// gets its own fresh ephemeral pyt Deployment + Service via s.deployer,
// created just for this call and deleted best-effort afterwards — see
// [pytdeployer.Deployer.RunOnEphemeral]. Workspaces flagged via
// Processor.pytTestOverrides.<workspaceID>.routeToProduction are the
// exception: their tests forward to the workspace's production pyt deployment
// instead (see [Service.isProdRouted]).
// AST ops (testLibrary, extractLibs) never execute user code; they go straight
// to the shared static AST deployment with no k8s API involvement.
func (s *Service) Forward(ctx context.Context, req *proto.ForwardRequest) (*proto.ForwardResponse, error) {
	start := time.Now()
	defer func() {
		s.log.Infon("time to forward to pyt",
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
		return nil, status.Error(codes.InvalidArgument, "workspaceId must be 1-59 alphanumeric or hyphen characters")
	}

	var statusCode int
	var body []byte
	var err error
	switch {
	case isExecutionOp && s.isProdRouted(req.WorkspaceId):
		// Config-flagged workspace: its tests must run with prod-only config
		// (custom DNS, pinned egress IPs, ...) the generic ephemeral spec can't
		// reproduce, so forward straight to the workspace's production pyt
		// deployment. No readiness wait — the forwarder's cold-start retries
		// ride out the prod deployment's scale-from-zero window.
		statusCode, body, err = forward(ctx,
			user_transformer.PerWorkspacePyTBaseURL(s.prodURLTemplate, req.WorkspaceId),
			req.WorkspaceId, req.Payload)
	case isExecutionOp:
		statusCode, body, err = s.deployer.RunOnEphemeral(ctx, req.WorkspaceId,
			func(ctx context.Context, baseURL string) (int, []byte, error) {
				return forward(ctx, baseURL, req.WorkspaceId, req.Payload)
			})
	default:
		statusCode, body, err = forward(ctx, s.staticASTURL, req.WorkspaceId, req.Payload)
	}
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
// flag is a per-workspace reloadable bool —
// Processor.pytTestOverrides.<workspaceID>.routeToProduction, falling back to
// the global Processor.pytTestOverrides.routeToProduction — so a workspace
// (or all of them) can be flagged without a restart. Config keys are matched
// case-insensitively, mirroring the lowercased deployment-name convention.
// workspaceID has already passed validWorkspaceID in Forward, so it is safe
// to interpolate into the key.
func (s *Service) isProdRouted(workspaceID string) bool {
	return s.conf.GetReloadableBoolVar(false,
		fmt.Sprintf("Processor.pytTestOverrides.%s.routeToProduction", workspaceID),
		"Processor.pytTestOverrides.routeToProduction").Load()
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
