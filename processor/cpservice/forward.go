package cpservice

import (
	"context"
	"errors"
	"regexp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/processor/internal/pytscaler"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/user_transformer"
	proto "github.com/rudderlabs/rudder-server/proto/processor"
)

// endpointForward is a Forwarder method bound to a single pyt endpoint.
type endpointForward func(ctx context.Context, workspaceID string, payload []byte) (int, []byte, error)

// validWorkspaceID gates what Forward substitutes into the pyt URL template and
// the pyt Deployment name. Anything beyond alphanumerics and hyphens must be
// rejected here: URL-significant characters (@, #, ?, /, :) surviving into the
// template would let a request redirect the forward to an arbitrary host
// (SSRF). 59 keeps "pyt-" + the lowercased ID within the 63-char DNS label
// limit the Deployment name is subject to anyway.
var validWorkspaceID = regexp.MustCompile(`^[a-zA-Z0-9-]{1,59}$`)

// Forward is the processor's only CP-facing RPC. It maps req.Op to the matching
// pyt endpoint and forwards req.Payload to it, returning the pyt response status
// and body unchanged. When explicit scaling is enabled (pytTestScalingEnabled)
// it first ensures the workspace's pyt Deployment is scaled up; by default the
// scaler is a no-op and waking the pyt is left to its own traffic-based
// autoscaling.
//
// It does not poll for pyt readiness: the forward itself retries the cold-start
// window within the caller's deadline (see [user_transformer.Client.Test] et al).
func (s *Service) Forward(ctx context.Context, req *proto.ForwardRequest) (*proto.ForwardResponse, error) {
	forward, ok := s.forwardForOp(req.Op)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown op %v", req.Op)
	}
	if !validWorkspaceID.MatchString(req.WorkspaceId) {
		return nil, status.Error(codes.InvalidArgument, "workspaceId must be 1-59 alphanumeric or hyphen characters")
	}

	if err := s.ensureScaled(ctx, req.WorkspaceId); err != nil {
		s.log.Errorn("scaling pyt deployment",
			obskit.WorkspaceID(req.WorkspaceId), obskit.Error(err))
		if errors.Is(err, pytscaler.ErrDeploymentNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "no pyt deployment for workspace %s", req.WorkspaceId)
		}
		return nil, status.Errorf(codes.Internal, "scaling pyt deployment: %v", err)
	}

	statusCode, body, err := forward(ctx, req.WorkspaceId, req.Payload)
	if err != nil {
		if errors.Is(err, user_transformer.ErrPerWorkspacePyTNotEnabled) {
			return nil, status.Error(codes.FailedPrecondition, "per-workspace PyT is not enabled")
		}
		s.log.Warnn("forwarding to pyt",
			obskit.WorkspaceID(req.WorkspaceId), logger.NewStringField("op", req.Op.String()), obskit.Error(err))
		return nil, status.Errorf(codes.Unavailable, "forwarding to pyt: %v", err)
	}
	return &proto.ForwardResponse{
		StatusCode: int32(statusCode),
		Body:       body,
	}, nil
}

// ensureScaled scales the workspace's pyt Deployment up to the configured target
// when it is currently at zero replicas; the scaler's EnsureScaled is idempotent
// and a no-op when the deployment is already running.
//
// Concurrent Forwards for the same workspace are collapsed by singleflight so a
// burst of test requests triggers a single scale check — not one per request —
// avoiding redundant cluster-wide List calls against the k8s API. Requests for
// different workspaces key on distinct IDs and run independently. (The shared
// call runs on the first caller's ctx; a fast scale makes that window
// negligible, and on error the caller retries the whole RPC.)
func (s *Service) ensureScaled(ctx context.Context, workspaceID string) error {
	_, err, _ := s.scaleGroup.Do(workspaceID, func() (any, error) {
		return nil, s.scaler.EnsureScaled(ctx, workspaceID, s.scaleTo.Load())
	})
	return err
}

// forwardForOp resolves a control-plane op to the Forwarder method that serves it,
// reporting false for an unspecified or unknown op.
func (s *Service) forwardForOp(op proto.Op) (endpointForward, bool) {
	switch op {
	case proto.Op_OP_TEST:
		return s.forwarder.Test, true
	case proto.Op_OP_TEST_RUN:
		return s.forwarder.TestRun, true
	case proto.Op_OP_TEST_LIBRARY:
		return s.forwarder.TestLibrary, true
	case proto.Op_OP_EXTRACT_LIBS:
		return s.forwarder.ExtractLibs, true
	default:
		return nil, false
	}
}
