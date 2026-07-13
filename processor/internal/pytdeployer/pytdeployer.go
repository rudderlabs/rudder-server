// Package pytdeployer runs a single Python-transformation-test request on a
// fresh, ephemeral rudder-pytransformer Deployment + Service, then tears both
// down.
//
// The test flow's execution ops (test, testRun) run arbitrary, untrusted
// customer Python. Rather than reusing a workspace's shared production pyt
// Deployment (the superseded pytscaler design), every execution request gets
// its own pod, created just for that request and destroyed after it — one
// customer per pod. [Deployer.RunOnEphemeral] owns the full lifecycle: create,
// wait for readiness, forward, delete (best-effort).
//
// A separate reaper CronJob (INFRA-2) cleans up anything this package fails to
// delete. It, and the RBAC that scopes what the processor's service account
// may touch (INFRA-1), key off the labels [New] stamps on every object this
// package creates — see [LabelManagedBy] and [LabelPurpose]. Their values are
// a cross-component contract: do not change them.
//
// The Kubernetes dependency is hidden behind the [Deployer] interface so
// callers can inject a fake clientset in tests. Unlike pytscaler, there is no
// no-op implementation: execution ops run untrusted code, so a caller that
// cannot build a real deployer has no safe fallback target and must fail the
// request instead.
package pytdeployer

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// Cross-component label contract. The reaper CronJob (INFRA-2) and the RBAC
// scoping the processor's service account (INFRA-1) both key off these exact
// keys/values — do not change them without updating those components too.
const (
	// LabelManagedBy identifies objects created by the processor.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// LabelManagedByValue is the value the processor stamps for LabelManagedBy.
	LabelManagedByValue = "rudderstack-processor"
	// LabelPurpose identifies why the processor created the object.
	LabelPurpose = "rudderstack.com/purpose"
	// LabelPurposeValue is the value the processor stamps for LabelPurpose on
	// every ephemeral test Deployment/Service.
	LabelPurposeValue = "pyt-test"
	// LabelWorkspaceID carries the (lowercased) workspace ID for traceability.
	LabelWorkspaceID = "rudderstack.com/workspace-id"

	// defaultTolerationsJSON mirrors the prod pyt chart's kata.tolerations: the
	// Kata nodepool is tainted, and without this toleration the pod stays Pending
	// forever.
	defaultTolerationsJSON = `[{"key":"dedicated","operator":"Equal","value":"kata","effect":"NoSchedule"}]`
)

// Deployer runs a single test request on a fresh ephemeral pyt deployment.
type Deployer interface {
	// RunOnEphemeral creates a Deployment + Service in the configured test
	// namespace, waits for the Deployment to report at least one ready
	// replica, invokes forward against the ephemeral Service's base URL, then
	// deletes both best-effort. forward's result (status/body/err) is
	// returned even when the best-effort delete itself fails; a delete
	// failure is logged, never surfaced to the caller — the reaper CronJob
	// covers anything left behind.
	RunOnEphemeral(ctx context.Context, workspaceID string, forward func(ctx context.Context, baseURL string) (int, []byte, error)) (int, []byte, error)
}

// Opt configures the k8s-backed Deployer built by [New].
type Opt func(*k8sDeployer)

// WithClientset injects a Kubernetes clientset instead of building an
// in-cluster one. Used in tests with a fake clientset.
func WithClientset(client kubernetes.Interface) Opt {
	return func(d *k8sDeployer) { d.client = client }
}

// New builds a Kubernetes-backed [Deployer]. Unless a clientset is injected
// via [WithClientset], it builds an in-cluster client (falling back to a
// kubeconfig file when not running in-cluster) and returns an error if that
// fails. Callers must not fall back to forwarding execution ops elsewhere on
// error — there is no safe target for untrusted code without a real deployer.
func New(conf *config.Config, log logger.Logger, opts ...Opt) (Deployer, error) {
	d := &k8sDeployer{
		log:    log.Child("pytdeployer"),
		config: loadConfig(conf),
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.client == nil {
		client, err := newInClusterClientset(conf)
		if err != nil {
			return nil, fmt.Errorf("building kubernetes client for ephemeral pyt deployments: %w", err)
		}
		d.client = client
	}
	return d, nil
}

// deployerConfig holds the settings loaded once at construction time (per the
// house rule: reloadable values go through config.ValueLoader, registered
// once in loadConfig and Load()ed where used, never re-registered per
// request). readinessTimeout, env, labels, and podAnnotations are reloadable
// so they can change without a restart.
//
// The defaults mirror the production per-workspace pytransformer chart
// (rudderstack-operator helm-charts/rudderstack-aux/charts/pytransformer), so
// an ephemeral test pod schedules and runs the same way a prod pyt pod does
// unless a key is deliberately overridden.
type deployerConfig struct {
	namespace        string
	image            string
	imagePullSecret  string
	runtimeClass     string
	zone             string
	nodeSelector     map[string]string
	tolerations      []corev1.Toleration
	cpuRequest       resource.Quantity
	memoryRequest    resource.Quantity
	readinessTimeout config.ValueLoader[time.Duration]
	env              config.ValueLoader[map[string]any]
	labels           config.ValueLoader[map[string]any]
	podAnnotations   config.ValueLoader[map[string]any]
	retry            retrySettings
}

func loadConfig(conf *config.Config) deployerConfig {
	return deployerConfig{
		namespace:       conf.GetStringVar("test-pytransformer", "Processor.pytDeployer.pytTestNamespace"),
		image:           conf.GetStringVar("", "Processor.pytDeployer.pytTestImage"),
		imagePullSecret: conf.GetStringVar("regcred", "Processor.pytDeployer.pytTestImagePullSecret"),
		runtimeClass:    conf.GetStringVar("kata-fc", "Processor.pytDeployer.pytTestRuntimeClass"),
		zone:            conf.GetStringVar("", "AVAILABILITY_ZONE"),
		nodeSelector: toStringMap(conf.GetStringMapVar(
			map[string]any{"karpenter.sh/nodepool": "kata"}, "Processor.pytDeployer.pytTestNodeSelector")),
		tolerations:      loadTolerations(conf),
		cpuRequest:       parseQuantity(conf.GetStringVar("200m", "Processor.pytDeployer.pytTestCPURequest"), "200m"),
		memoryRequest:    parseQuantity(conf.GetStringVar("500Mi", "Processor.pytDeployer.pytTestMemoryRequest"), "500Mi"),
		readinessTimeout: conf.GetReloadableDurationVar(60, time.Second, "Processor.pytDeployer.pytTestReadinessTimeout"),
		env:              conf.GetReloadableStringMapVar(nil, "Processor.pytDeployer.pytTestEnv"),
		labels:           conf.GetReloadableStringMapVar(nil, "Processor.pytDeployer.pytTestLabels"),
		podAnnotations:   conf.GetReloadableStringMapVar(nil, "Processor.pytDeployer.pytTestPodAnnotations"),
		retry:            newRetrySettings(conf),
	}
}

// toStringMap flattens a config map (map[string]any) into map[string]string.
func toStringMap(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for name, value := range m {
		out[name] = fmt.Sprint(value)
	}
	return out
}

// parseQuantity parses an operator-supplied resource quantity, falling back to
// the built-in default (a literal we control, so MustParse is safe) rather
// than blocking the processor from starting on a malformed value.
func parseQuantity(value, fallback string) resource.Quantity {
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.MustParse(fallback)
	}
	return q
}

// loadTolerations parses Processor.pytDeployer.pytTestTolerations — a JSON
// array of k8s tolerations, e.g.
// [{"key":"dedicated","operator":"Equal","value":"kata","effect":"NoSchedule"}]
// — falling back to the built-in default on a malformed value rather than
// blocking the processor from starting. An explicit empty array ([]) is
// honoured: it removes all tolerations.
func loadTolerations(conf *config.Config) []corev1.Toleration {
	raw := conf.GetStringVar(defaultTolerationsJSON, "Processor.pytDeployer.pytTestTolerations")
	var tolerations []corev1.Toleration
	if err := jsonrs.Unmarshal([]byte(raw), &tolerations); err != nil {
		_ = jsonrs.Unmarshal([]byte(defaultTolerationsJSON), &tolerations)
	}
	return tolerations
}
