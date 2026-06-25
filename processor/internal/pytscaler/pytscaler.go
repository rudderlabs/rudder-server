// Package pytscaler reads and sets the replica count of a workspace's
// per-workspace rudder-pytransformer (pyt-{workspaceID}) Deployment, so the
// transformation test flow can scale it up from zero before forwarding a request
// to it.
//
// The pyt Deployments default to 0 replicas (Keda scales them back to 0 when
// idle) and live in one of several pyt namespaces — not a single fixed one and
// not the processor's own namespace. Because the Kubernetes scale subresource is
// namespace-scoped, the scaler first locates the Deployment by name across all
// namespaces (a cluster-wide List by metadata.name) and then operates in the
// namespace it was found in. It only ever touches the replica count via the scale
// subresource, never the pod spec, so the deployment's Kata RuntimeClass is
// preserved.
//
// The [Scaler] interface deliberately exposes just two thin primitives over the
// scale subresource — [Scaler.GetReplicaCount] and [Scaler.SetReplicaCount].
// Deciding whether a scale-up is actually needed (e.g. "set to N only when the
// current count is 0") is the caller's responsibility, not this layer's.
//
// The Kubernetes dependency is hidden behind the [Scaler] interface. [New] builds
// the real k8s-backed implementation; [NewNoop] returns a do-nothing
// implementation so the processor can start and serve even when Kubernetes is not
// available (local dev, k8s client build failure, or the feature disabled).
package pytscaler

import (
	"context"
	"errors"
	"strings"

	"k8s.io/client-go/kubernetes"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// ErrDeploymentNotFound is returned by [Scaler.EnsureScaled] when no pyt
// Deployment exists for the given workspace in any namespace.
var ErrDeploymentNotFound = errors.New("pyt deployment not found")

// Scaler reads and sets the replica count of a workspace's pyt Deployment. It is
// a thin wrapper over the Kubernetes scale subresource; the get-before-set
// idempotency decision is left to the caller.
type Scaler interface {
	// GetReplicaCount returns the current replica count of the workspace's
	// pyt-{workspaceID} Deployment, locating it by name across all namespaces.
	// It returns ErrDeploymentNotFound if no such Deployment exists.
	GetReplicaCount(ctx context.Context, workspaceID string) (int, error)
	// SetReplicaCount sets the replica count of the workspace's pyt-{workspaceID}
	// Deployment via the scale subresource. It always writes the given count
	// unconditionally — ensuring it is only called when a change is needed (e.g.
	// the current count is 0) is the caller's responsibility.
	SetReplicaCount(ctx context.Context, workspaceID string, count int) error
}

// Opt configures the k8s-backed Scaler built by [New].
type Opt func(*k8sScaler)

// WithClientset injects a Kubernetes clientset instead of building an in-cluster
// one. Used in tests with a fake clientset.
func WithClientset(client kubernetes.Interface) Opt {
	return func(s *k8sScaler) { s.client = client }
}

// New builds a Kubernetes-backed [Scaler]. Unless a clientset is injected via
// [WithClientset], it builds an in-cluster client (falling back to a kubeconfig
// file when not running in-cluster) and returns an error if that fails — callers
// that want the processor to start regardless should fall back to [NewNoop].
func New(conf *config.Config, log logger.Logger, opts ...Opt) (Scaler, error) {
	s := &k8sScaler{
		log:   log.Child("pytscaler"),
		retry: newRetrySettings(conf),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.client == nil {
		client, err := newInClusterClientset(conf)
		if err != nil {
			return nil, err
		}
		s.client = client
	}
	return s, nil
}

// NewNoop returns a [Scaler] that does nothing. Use it when k8s scaling is
// unavailable or disabled, so the processor can still start and serve.
func NewNoop(log logger.Logger) Scaler {
	return &noopScaler{log: log.Child("pytscaler")}
}

type noopScaler struct {
	log logger.Logger
}

// GetReplicaCount reports the deployment as already scaled (1) so callers using
// the get-before-set pattern skip scaling when k8s is unavailable.
func (n *noopScaler) GetReplicaCount(_ context.Context, workspaceID string) (int, error) {
	n.log.Debugn("pyt scaler disabled; reporting deployment as already scaled",
		logger.NewStringField("workspaceID", workspaceID))
	return 1, nil
}

func (n *noopScaler) SetReplicaCount(_ context.Context, workspaceID string, count int) error {
	n.log.Debugn("pyt scaler disabled; skipping scale",
		logger.NewStringField("workspaceID", workspaceID),
		logger.NewIntField("count", int64(count)))
	return nil
}

// deploymentName returns the Deployment name convention for a workspace's pyt
// transformer: pyt-{lowercase workspaceID}.
func deploymentName(workspaceID string) string {
	return "pyt-" + strings.ToLower(workspaceID)
}
