package pytdeployer

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

const testNamespace = "pyt-test-ns"

func TestRunOnEphemeral(t *testing.T) {
	t.Run("creates a Deployment + Service stamped with the label contract and generic defaults", func(t *testing.T) {
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, baseConfig())

		var gotBaseURL string
		status, body, err := dep.RunOnEphemeral(context.Background(), "WS-1",
			func(ctx context.Context, baseURL string) (int, []byte, error) {
				gotBaseURL = baseURL
				return 200, []byte("ok"), nil
			})
		require.NoError(t, err)
		require.Equal(t, 200, status)
		require.Equal(t, []byte("ok"), body)
		require.NotEmpty(t, gotBaseURL)

		created := findCreatedDeployment(t, cs)
		require.Equal(t, LabelManagedByValue, created.Labels[LabelManagedBy])
		require.Equal(t, LabelPurposeValue, created.Labels[LabelPurpose])
		require.Equal(t, "ws-1", created.Labels[LabelWorkspaceID], "the workspace label must be lowercased")
		require.Equal(t, "pytransformer/pyt:latest", created.Spec.Template.Spec.Containers[0].Image)
		require.Equal(t, "kata-fc", *created.Spec.Template.Spec.RuntimeClassName)
		require.EqualValues(t, 1, *created.Spec.Replicas)
		require.Equal(t, int32(pytContainerPort), created.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)

		createdSvc := findCreatedService(t, cs)
		require.Equal(t, LabelManagedByValue, createdSvc.Labels[LabelManagedBy])
		require.Equal(t, created.Name, createdSvc.Name, "the Deployment and Service share the same run name")
		require.Equal(t, created.Labels, createdSvc.Spec.Selector, "the Service selector must match the Deployment's pod labels")
	})

	t.Run("applies the generic env to the container, deterministically ordered", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestEnv", map[string]any{"LOG_LEVEL": "info", "CONFIG_BACKEND_URL": "http://cb:5000"})
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, conf)

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)

		created := findCreatedDeployment(t, cs)
		require.Equal(t, []corev1.EnvVar{
			{Name: "CONFIG_BACKEND_URL", Value: "http://cb:5000"},
			{Name: "LOG_LEVEL", Value: "info"},
			{Name: "WORKSPACE_ID", Value: "ws-1"},
		}, created.Spec.Template.Spec.Containers[0].Env)
	})

	t.Run("injects WORKSPACE_ID per run, overriding any config-provided value", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestEnv", map[string]any{"WORKSPACE_ID": "from-config"})
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, conf)

		_, _, err := dep.RunOnEphemeral(context.Background(), "WS-Original-Case",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)

		require.Equal(t, []corev1.EnvVar{{Name: "WORKSPACE_ID", Value: "WS-Original-Case"}},
			findCreatedDeployment(t, cs).Spec.Template.Spec.Containers[0].Env,
			"exactly one WORKSPACE_ID, the per-run one, original case (matching prod)")
	})

	t.Run("matches the production pod spec: kata scheduling, hardening, probes, resources, /tmp", func(t *testing.T) {
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, baseConfig())

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)

		spec := findCreatedDeployment(t, cs).Spec.Template.Spec
		require.Equal(t, "kata", spec.NodeSelector["karpenter.sh/nodepool"], "must land on the Kata nodepool")
		require.Contains(t, spec.Tolerations, corev1.Toleration{
			Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "kata", Effect: corev1.TaintEffectNoSchedule,
		}, "the Kata nodepool is tainted; without the toleration the pod stays Pending forever")
		require.Equal(t, []corev1.LocalObjectReference{{Name: "regcred"}}, spec.ImagePullSecrets)
		require.False(t, *spec.EnableServiceLinks, "other tenants' Services must not be injected into the untrusted pod's env")

		container := spec.Containers[0]
		sc := container.SecurityContext
		require.True(t, *sc.RunAsNonRoot)
		require.EqualValues(t, 1000, *sc.RunAsUser)
		require.True(t, *sc.ReadOnlyRootFilesystem, "read-only root fs is one of the sandbox layers")
		require.False(t, *sc.AllowPrivilegeEscalation)

		require.Equal(t, "/health/ready", container.ReadinessProbe.HTTPGet.Path,
			"without a readinessProbe, waitReady would resolve before the app serves")
		require.Equal(t, "/health/live", container.LivenessProbe.HTTPGet.Path)

		require.Equal(t, "200m", container.Resources.Requests.Cpu().String())
		require.Equal(t, "500Mi", container.Resources.Requests.Memory().String())
		require.Equal(t, "400m", container.Resources.Limits.Cpu().String(),
			"untrusted code must not consume unbounded node CPU")
		require.Equal(t, "700Mi", container.Resources.Limits.Memory().String(),
			"untrusted code must not consume unbounded node memory")

		require.Contains(t, container.VolumeMounts, corev1.VolumeMount{Name: "tmp", MountPath: "/tmp"},
			"read-only root fs needs a writable /tmp")
		require.Len(t, spec.Volumes, 1)
		require.NotNil(t, spec.Volumes[0].EmptyDir)

		require.EqualValues(t, pytMetricsPort, container.Ports[1].ContainerPort, "prom metrics port for scraping")
	})

	t.Run("config-provided tolerations replace the default kata toleration", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestTolerations",
			`[{"key":"dedicated","operator":"Equal","value":"pyt-test","effect":"NoSchedule"}]`)
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, conf)

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)

		require.Equal(t, []corev1.Toleration{{
			Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "pyt-test", Effect: corev1.TaintEffectNoSchedule,
		}}, findCreatedDeployment(t, cs).Spec.Template.Spec.Tolerations)
	})

	t.Run("malformed tolerations config falls back to the default kata toleration", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestTolerations", `not-json`)
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, conf)

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)

		require.Contains(t, findCreatedDeployment(t, cs).Spec.Template.Spec.Tolerations, corev1.Toleration{
			Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "kata", Effect: corev1.TaintEffectNoSchedule,
		}, "a bad config value must not silently drop the toleration that lets pods schedule at all")
	})

	t.Run("applies config-provided pod annotations", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestPodAnnotations", map[string]any{"egress.rudderstack.com/policy": "restricted"})
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, conf)

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)
		require.Equal(t, "restricted",
			findCreatedDeployment(t, cs).Spec.Template.Annotations["egress.rudderstack.com/policy"])
	})

	t.Run("applies config-provided labels to the Deployment, pod template, and Service", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestLabels", map[string]any{"team": "data-platform", "app.kubernetes.io/part-of": "transformations"})
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, conf)

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)

		created := findCreatedDeployment(t, cs)
		require.Equal(t, "data-platform", created.Labels["team"])
		require.Equal(t, "transformations", created.Labels["app.kubernetes.io/part-of"])
		require.Equal(t, "data-platform", created.Spec.Template.Labels["team"])

		createdSvc := findCreatedService(t, cs)
		require.Equal(t, "data-platform", createdSvc.Labels["team"])
		require.Subset(t, created.Spec.Template.Labels, createdSvc.Spec.Selector,
			"every Service selector entry must be present on the pod labels or the Service selects nothing")
	})

	t.Run("config labels can never override the reaper's label contract", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestLabels", map[string]any{
			LabelPurpose:   "not-a-test",
			LabelManagedBy: "someone-else",
		})
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, conf)

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)

		created := findCreatedDeployment(t, cs)
		require.Equal(t, LabelPurposeValue, created.Labels[LabelPurpose],
			"the contract labels are stamped after config labels, so config must lose")
		require.Equal(t, LabelManagedByValue, created.Labels[LabelManagedBy])
	})

	t.Run("pins the pod to the configured zone and scopes the Service selector to it", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("AVAILABILITY_ZONE", "us-east-1a")
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, conf)

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)

		created := findCreatedDeployment(t, cs)
		require.Equal(t, "us-east-1a", created.Spec.Template.Spec.NodeSelector["topology.kubernetes.io/zone"])
		require.Equal(t, "kata", created.Spec.Template.Spec.NodeSelector["karpenter.sh/nodepool"],
			"the zone entry must merge with, not replace, the base nodeSelector")
		require.Equal(t, "us-east-1a", created.Spec.Template.Labels["zone"],
			"the pod must carry the zone label the Service selector matches on")

		createdSvc := findCreatedService(t, cs)
		require.Equal(t, "us-east-1a", createdSvc.Spec.Selector["zone"])
		require.Subset(t, created.Spec.Template.Labels, createdSvc.Spec.Selector,
			"every Service selector entry must be present on the pod labels or the Service selects nothing")
	})

	t.Run("a config-provided \"zone\" label loses to the configured zone, consistently everywhere", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("AVAILABILITY_ZONE", "us-east-1a")
		conf.Set("Processor.pytDeployer.pytTestLabels", map[string]any{"zone": "from-config"})
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, conf)

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)

		created := findCreatedDeployment(t, cs)
		require.Equal(t, "us-east-1a", created.Spec.Template.Labels["zone"])
		require.Equal(t, "us-east-1a", findCreatedService(t, cs).Spec.Selector["zone"])
		require.Equal(t, created.Spec.Template.Labels, created.Spec.Selector.MatchLabels,
			"a diverging zone value between selector and template would be rejected by the apiserver")
	})

	t.Run("no zone configured: no zone nodeSelector entry and no zone in the Service selector", func(t *testing.T) {
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, baseConfig()) // baseConfig sets no AVAILABILITY_ZONE

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)

		created := findCreatedDeployment(t, cs)
		require.NotContains(t, created.Spec.Template.Spec.NodeSelector, "topology.kubernetes.io/zone",
			"an empty-valued zone nodeSelector would match no nodes at all")
		require.NotContains(t, findCreatedService(t, cs).Spec.Selector, "zone")
	})

	t.Run("uses a unique name per run so two concurrent runs create distinct objects", func(t *testing.T) {
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, baseConfig())

		const n = 2
		baseURLs := make([]string, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
					func(ctx context.Context, baseURL string) (int, []byte, error) {
						baseURLs[i] = baseURL
						return 200, nil, nil
					})
				errs[i] = err
			}(i)
		}
		wg.Wait()

		for i, err := range errs {
			require.NoErrorf(t, err, "run %d", i)
		}
		require.NotEmpty(t, baseURLs[0])
		require.NotEmpty(t, baseURLs[1])
		require.NotEqual(t, baseURLs[0], baseURLs[1], "concurrent runs must not collide on the same object name")
	})

	t.Run("resolves once the Deployment reports at least one ready replica", func(t *testing.T) {
		cs := newDelayedReadyClient(3) // not ready on the first couple of polls
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestReadinessTimeout", "10s")
		dep := newDeployer(t, cs, conf)

		var forwarded bool
		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { forwarded = true; return 200, nil, nil })
		require.NoError(t, err)
		require.True(t, forwarded, "forward must only run once the deployment is ready")
	})

	t.Run("returns a clear error on a readiness timeout and deletes what it created", func(t *testing.T) {
		cs := newFakeClient() // never becomes ready
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestReadinessTimeout", "300ms")
		dep := newDeployer(t, cs, conf)

		var forwarded bool
		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { forwarded = true; return 200, nil, nil })
		require.Error(t, err)
		require.False(t, forwarded, "forward must never run when readiness times out")
		require.True(t, hasAction(cs, "deployments"), "the timed-out deployment must still be deleted best-effort")
		require.True(t, hasAction(cs, "services"), "the timed-out service must still be deleted best-effort")
	})

	t.Run("deletes the Deployment+Service after a successful forward", func(t *testing.T) {
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, baseConfig())

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)
		require.True(t, hasAction(cs, "deployments"))
		require.True(t, hasAction(cs, "services"))
	})

	t.Run("deletes the Deployment+Service after a forward error, and returns the forward error unchanged", func(t *testing.T) {
		cs := newAutoReadyClient()
		dep := newDeployer(t, cs, baseConfig())
		forwardErr := errors.New("pyt returned a 500")

		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 0, nil, forwardErr })
		require.ErrorIs(t, err, forwardErr)
		require.True(t, hasAction(cs, "deployments"))
		require.True(t, hasAction(cs, "services"))
	})

	t.Run("a delete failure is swallowed: it is not returned and does not change the forward result", func(t *testing.T) {
		cs := newAutoReadyClient()
		cs.PrependReactor("delete", "deployments", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("etcd is down")
		})
		cs.PrependReactor("delete", "services", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("etcd is down")
		})
		dep := newDeployer(t, cs, baseConfig())

		status, body, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 201, []byte("ok"), nil })
		require.NoError(t, err, "a failed best-effort delete must not surface as the call's error")
		require.Equal(t, 201, status)
		require.Equal(t, []byte("ok"), body)
		require.True(t, hasAction(cs, "deployments"), "delete must still have been attempted")
	})

	t.Run("errors without calling forward when creating the Deployment fails", func(t *testing.T) {
		cs := newFakeClient()
		cs.PrependReactor("create", "deployments", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("quota exceeded")
		})
		dep := newDeployer(t, cs, baseConfig())

		var forwarded bool
		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { forwarded = true; return 200, nil, nil })
		require.Error(t, err)
		require.False(t, forwarded)
	})

	t.Run("retries transient k8s errors on create and still serves the request", func(t *testing.T) {
		cs := newAutoReadyClient()
		var attempts atomic.Int32
		// Fail the first Deployment create with a 429; fall through to the
		// auto-ready reactor on the retry.
		cs.PrependReactor("create", "deployments", func(ktesting.Action) (bool, runtime.Object, error) {
			if attempts.Add(1) == 1 {
				return true, nil, apierrors.NewTooManyRequests("throttled", 1)
			}
			return false, nil, nil
		})
		dep := newDeployer(t, cs, fastRetryConfig())

		status, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err, "a transient 429 must be retried, not fail the test run")
		require.Equal(t, 200, status)
		require.GreaterOrEqual(t, attempts.Load(), int32(2))
	})

	t.Run("treats AlreadyExists on a create retry as success (own earlier attempt landed)", func(t *testing.T) {
		cs := newAutoReadyClient()
		var attempts atomic.Int32
		cs.PrependReactor("create", "deployments", func(action ktesting.Action) (bool, runtime.Object, error) {
			switch attempts.Add(1) {
			case 1:
				// Ambiguous transient failure: the object actually landed.
				ca := action.(ktesting.CreateActionImpl)
				d := ca.GetObject().(*appsv1.Deployment).DeepCopy()
				d.Status.ReadyReplicas = 1
				if err := cs.Tracker().Create(ca.GetResource(), d, d.Namespace); err != nil {
					return true, nil, err
				}
				return true, nil, apierrors.NewTimeoutError("request timed out", 1)
			case 2:
				return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Group: "apps", Resource: "deployments"}, "pyt-test-x")
			default:
				return false, nil, nil
			}
		})
		dep := newDeployer(t, cs, fastRetryConfig())

		status, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { return 200, nil, nil })
		require.NoError(t, err)
		require.Equal(t, 200, status)
	})

	t.Run("cleans up the Deployment and errors without calling forward when creating the Service fails", func(t *testing.T) {
		cs := newFakeClient()
		cs.PrependReactor("create", "services", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("service quota exceeded")
		})
		dep := newDeployer(t, cs, baseConfig())

		var forwarded bool
		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { forwarded = true; return 200, nil, nil })
		require.Error(t, err)
		require.False(t, forwarded)
		require.True(t, hasAction(cs, "deployments"), "the Deployment created before the Service failure must be cleaned up")
	})

	t.Run("errors immediately when the test namespace is explicitly emptied", func(t *testing.T) {
		cs := newAutoReadyClient()
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestNamespace", "") // override the non-empty default
		dep := newDeployer(t, cs, conf)

		var forwarded bool
		_, _, err := dep.RunOnEphemeral(context.Background(), "ws-1",
			func(context.Context, string) (int, []byte, error) { forwarded = true; return 200, nil, nil })
		require.Error(t, err)
		require.False(t, forwarded)
		require.Empty(t, cs.Actions(), "no k8s calls should be made when misconfigured")
	})
}

func TestNew(t *testing.T) {
	t.Run("returns an error when no clientset is injected and no in-cluster/kubeconfig is available", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.inCluster", false)
		conf.Set("Processor.pytDeployer.kubeConfigPath", "/nonexistent/kubeconfig")

		_, err := New(conf, logger.NOP)
		require.Error(t, err)
	})

	t.Run("returns an error when pytTestImage is not configured", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestImage", "")

		_, err := New(conf, logger.NOP, WithClientset(fake.NewClientset()))
		require.ErrorContains(t, err, "pytTestImage")
	})

	t.Run("returns an error when pytTestImagePullSecret is not configured", func(t *testing.T) {
		conf := baseConfig()
		conf.Set("Processor.pytDeployer.pytTestImagePullSecret", "")

		_, err := New(conf, logger.NOP, WithClientset(fake.NewClientset()))
		require.ErrorContains(t, err, "pytTestImagePullSecret")
	})

	t.Run("succeeds with an injected clientset", func(t *testing.T) {
		d, err := New(baseConfig(), logger.NOP, WithClientset(fake.NewClientset()))
		require.NoError(t, err)
		require.NotNil(t, d)
	})
}

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"API 429 is transient", apierrors.NewTooManyRequests("throttled", 1), true},
		{"API timeout is transient", apierrors.NewTimeoutError("request timed out", 1), true},
		{"connection reset is transient", syscall.ECONNRESET, true},
		{"connection refused is transient", syscall.ECONNREFUSED, true},
		{"unexpected EOF is transient", io.ErrUnexpectedEOF, true},
		{"net timeout is transient", &net.OpError{Op: "dial", Err: timeoutError{}}, true},
		{"API NotFound is permanent", apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "deployments"}, "x"), false},
		{"API BadRequest is permanent", apierrors.NewBadRequest("bad"), false},
		{"generic error is permanent", errors.New("boom"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isTransient(tc.err))
		})
	}
}

// timeoutError satisfies net.Error with Timeout() == true, standing in for a
// dial/read deadline error from a real connection.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// --- test helpers ---

func baseConfig() *config.Config {
	conf := config.New()
	conf.Set("Processor.pytDeployer.pytTestNamespace", testNamespace)
	conf.Set("Processor.pytDeployer.pytTestImage", "pytransformer/pyt:latest")
	conf.Set("Processor.pytDeployer.pytTestImagePullSecret", "regcred")
	conf.Set("Processor.pytDeployer.pytTestReadinessTimeout", "5s")
	return conf
}

// fastRetryConfig is baseConfig with near-zero retry backoff so tests
// exercising the transient-error retry path don't sleep for real.
func fastRetryConfig() *config.Config {
	conf := baseConfig()
	conf.Set("Processor.pytDeployer.retry.initialInterval", "1ms")
	conf.Set("Processor.pytDeployer.retry.maxInterval", "2ms")
	return conf
}

func newDeployer(t *testing.T, cs *fake.Clientset, conf *config.Config) Deployer {
	t.Helper()
	d, err := New(conf, logger.NOP, WithClientset(cs))
	require.NoError(t, err)
	return d
}

// newFakeClient returns a bare fake clientset: created Deployments start with
// zero ready replicas and never become ready on their own (real clusters need
// a controller to update .status; the fake doesn't run one).
func newFakeClient() *fake.Clientset {
	return fake.NewClientset()
}

// newAutoReadyClient returns a fake clientset whose "create" reactor for
// Deployments immediately marks the created object as having 1 ready
// replica, so [k8sDeployer.waitReady] resolves on its very first poll.
func newAutoReadyClient() *fake.Clientset {
	cs := fake.NewClientset()
	cs.PrependReactor("create", "deployments", func(action ktesting.Action) (bool, runtime.Object, error) {
		ca := action.(ktesting.CreateActionImpl)
		d := ca.GetObject().(*appsv1.Deployment).DeepCopy()
		d.Status.ReadyReplicas = 1
		if err := cs.Tracker().Create(ca.GetResource(), d, d.Namespace); err != nil {
			return true, nil, err
		}
		return true, d, nil
	})
	return cs
}

// newDelayedReadyClient returns a fake clientset whose Deployment starts with
// zero ready replicas and only reports ready starting from the readyOnGet'th
// Get call, deterministically exercising [k8sDeployer.waitReady]'s poll loop
// without relying on real time.
func newDelayedReadyClient(readyOnGet int) *fake.Clientset {
	cs := fake.NewClientset()
	var gets atomic.Int32
	cs.PrependReactor("get", "deployments", func(action ktesting.Action) (bool, runtime.Object, error) {
		ga := action.(ktesting.GetActionImpl)
		obj, err := cs.Tracker().Get(ga.GetResource(), ga.GetNamespace(), ga.GetName())
		if err != nil {
			return true, nil, err
		}
		d := obj.(*appsv1.Deployment).DeepCopy()
		if int(gets.Add(1)) >= readyOnGet {
			d.Status.ReadyReplicas = 1
		}
		return true, d, nil
	})
	return cs
}

func findCreatedDeployment(t *testing.T, cs *fake.Clientset) *appsv1.Deployment {
	t.Helper()
	for _, a := range cs.Actions() {
		if a.GetVerb() == "create" && a.GetResource().Resource == "deployments" {
			return a.(ktesting.CreateActionImpl).GetObject().(*appsv1.Deployment)
		}
	}
	t.Fatal("no Deployment create action found")
	return nil
}

func findCreatedService(t *testing.T, cs *fake.Clientset) *corev1.Service {
	t.Helper()
	for _, a := range cs.Actions() {
		if a.GetVerb() == "create" && a.GetResource().Resource == "services" {
			return a.(ktesting.CreateActionImpl).GetObject().(*corev1.Service)
		}
	}
	t.Fatal("no Service create action found")
	return nil
}

func hasAction(cs *fake.Clientset, resource string) bool {
	for _, a := range cs.Actions() {
		if a.GetVerb() == "delete" && a.GetResource().Resource == resource {
			return true
		}
	}
	return false
}
