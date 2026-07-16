package pytdeployer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

const (
	pytContainerPort      = 8080
	pytMetricsPort        = 9091
	runIDLength           = 8
	readinessPollInterval = 250 * time.Millisecond
	// deleteTimeout bounds the best-effort cleanup so it can't hang the
	// request past its own deadline even when ctx is already cancelled.
	deleteTimeout = 15 * time.Second
)

type k8sDeployer struct {
	client kubernetes.Interface
	log    logger.Logger
	config deployerConfig
}

// RunOnEphemeral implements [Deployer].
func (d *k8sDeployer) RunOnEphemeral(
	ctx context.Context,
	workspaceID string,
	forward func(ctx context.Context, baseURL string) (int, []byte, error),
) (int, []byte, error) {
	if d.config.namespace == "" {
		return 0, nil, errors.New("pyt test namespace is not configured")
	}
	name := "pyt-test-" + rand.String(runIDLength)
	dep, svc := d.buildResources(name, workspaceID)

	if _, err := withRetry(ctx, d.config.retry, func() (*appsv1.Deployment, error) {
		created, err := d.client.AppsV1().Deployments(d.config.namespace).Create(ctx, dep, metav1.CreateOptions{})
		// The name is unique per run, so AlreadyExists can only mean our own
		// previous attempt landed despite a transient error — treat as success.
		if apierrors.IsAlreadyExists(err) {
			return created, nil
		}
		return created, err
	}); err != nil {
		return 0, nil, fmt.Errorf("creating ephemeral pyt deployment %s/%s: %w", d.config.namespace, name, err)
	}
	if _, err := withRetry(ctx, d.config.retry, func() (*corev1.Service, error) {
		created, err := d.client.CoreV1().Services(d.config.namespace).Create(ctx, svc, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			return created, nil
		}
		return created, err
	}); err != nil {
		d.delete(ctx, name)
		return 0, nil, fmt.Errorf("creating ephemeral pyt service %s/%s: %w", d.config.namespace, name, err)
	}
	// Delete runs regardless of what happens below — on a readiness timeout,
	// a forward error, or success alike. It never changes the response
	// returned to the caller; a failed delete is only logged.
	defer d.delete(ctx, name)

	if err := d.waitReady(ctx, name); err != nil {
		return 0, nil, err
	}

	baseURL := fmt.Sprintf("http://%s.%s.svc:%d", name, d.config.namespace, pytContainerPort)
	return forward(ctx, baseURL)
}

// buildResources builds the Deployment + Service for a single ephemeral run
// from the generic spec (pytTestImage / pytTestEnv / pytTestRuntimeClass).
// Workspaces whose tests can't run on the generic spec (custom DNS, pinned
// egress IPs, ...) are routed to their production pyt deployment by cpservice
// instead — they never reach this builder.
func (d *k8sDeployer) buildResources(name, workspaceID string) (*appsv1.Deployment, *corev1.Service) {
	// Config-provided labels are seeded first, and the contract labels are
	// stamped after them, so config can never override what the reaper and the
	// RBAC key on. (Note: the config layer lowercases map keys — fine for
	// labels, whose keys are conventionally lowercase.)
	labels := make(map[string]string)
	for name, value := range d.config.labels.Load() {
		labels[name] = fmt.Sprint(value)
	}
	labels[LabelManagedBy] = LabelManagedByValue
	labels[LabelPurpose] = LabelPurposeValue
	labels[LabelWorkspaceID] = strings.ToLower(workspaceID)

	// WORKSPACE_ID is per-run, so it is injected programmatically on top of the
	// config-provided env (prod injects it per workspace the same way). The
	// config layer lowercases map keys and envVars uppercases them back, so
	// delete the only collision form before setting ours.
	env := maps.Clone(d.config.env.Load())
	if env == nil {
		env = map[string]any{}
	}
	delete(env, "workspace_id")
	env["WORKSPACE_ID"] = workspaceID

	// the test pod runs the untrusted code, so it must schedule the same way (Kata nodepool selector +
	// toleration) and carry the same sandbox hardening (non-root, read-only
	// root filesystem with a writable /tmp emptyDir, no privilege escalation).
	// The readinessProbe is what makes waitReady meaningful — without it a
	// container is Ready the moment it starts, before uvicorn serves.
	podSpec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyAlways,
		// Service links would inject every Service in the shared test
		// namespace (other tenants' concurrent test runs included) into the
		// untrusted pod's env.
		EnableServiceLinks: new(false),
		NodeSelector:       maps.Clone(d.config.nodeSelector),
		// Tolerations come from config (default: the prod chart's kata
		// toleration). They only permit scheduling, never force it, so they are
		// applied regardless of whether the Kata RuntimeClass is enabled.
		Tolerations:      slices.Clone(d.config.tolerations),
		ImagePullSecrets: []corev1.LocalObjectReference{{Name: d.config.imagePullSecret}},
		Containers: []corev1.Container{{
			Name:  "pytransformer",
			Image: d.config.image,
			SecurityContext: &corev1.SecurityContext{
				RunAsNonRoot:             new(true),
				RunAsUser:                new(int64(1000)),
				RunAsGroup:               new(int64(1000)),
				ReadOnlyRootFilesystem:   new(true),
				AllowPrivilegeEscalation: new(false),
			},
			Ports: []corev1.ContainerPort{
				{Name: "http", ContainerPort: pytContainerPort, Protocol: corev1.ProtocolTCP},
				{Name: "prom", ContainerPort: pytMetricsPort, Protocol: corev1.ProtocolTCP},
			},
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{Path: "/health/live", Port: intstr.FromInt32(pytContainerPort)},
				},
				InitialDelaySeconds: 10, PeriodSeconds: 30, TimeoutSeconds: 10, FailureThreshold: 3,
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{Path: "/health/ready", Port: intstr.FromInt32(pytContainerPort)},
				},
				InitialDelaySeconds: 5, PeriodSeconds: 10, TimeoutSeconds: 5, FailureThreshold: 3,
			},
			// Limits are mandatory here, not an optimization: the container
			// runs untrusted, non-production-grade code, so it must not be
			// able to consume unbounded node CPU/memory.
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    d.config.cpuRequest,
					corev1.ResourceMemory: d.config.memoryRequest,
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    d.config.cpuLimit,
					corev1.ResourceMemory: d.config.memoryLimit,
				},
			},
			Env:          envVars(env),
			VolumeMounts: []corev1.VolumeMount{{Name: "tmp", MountPath: "/tmp"}},
		}},
		Volumes: []corev1.Volume{{
			Name:         "tmp",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		}},
	}
	if d.config.runtimeClass != "" {
		podSpec.RuntimeClassName = &d.config.runtimeClass
	}

	// Zone pinning: schedule the pod onto the configured zone and scope the
	// Service to zone-labeled pods. The zone label is stamped into labels
	// BEFORE the pod/selector clones below so the Deployment selector, pod
	// template labels, and Service selector all agree on it — a
	// config-provided "zone" label surviving in one of them but not the
	// others would make the Deployment selector mismatch its own template
	// (rejected by the apiserver) or the Service select nothing. Skipped
	// entirely when no zone is configured (e.g. single-zone clusters, local
	// dev): an empty-valued nodeSelector would only match nodes labeled with
	// an empty zone, i.e. nothing.
	if d.config.zone != "" {
		podSpec.NodeSelector["topology.kubernetes.io/zone"] = d.config.zone
		labels["zone"] = d.config.zone
	}
	podLabels := maps.Clone(labels)
	svcSelector := maps.Clone(labels)

	var podAnnotations map[string]string
	if annotations := d.config.podAnnotations.Load(); len(annotations) > 0 {
		podAnnotations = toStringMap(annotations)
	}

	replicas := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: d.config.namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels, Annotations: podAnnotations},
				Spec:       podSpec,
			},
		},
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: d.config.namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			Selector: svcSelector,
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       pytContainerPort,
				TargetPort: intstr.FromInt32(pytContainerPort),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
	return dep, svc
}

// envVars converts the generic env map (map[string]any, as the config layer's
// GetReloadableStringMapVar returns it) into a deterministically ordered
// (sorted by name) slice so the generated PodSpec is stable across calls —
// useful for tests asserting on the built object.
//
// Names are uppercased: the config layer (viper) lowercases map keys, which
// would silently mangle env names ("LOG_LEVEL" arrives as "log_level").
// pytransformer's env is uppercase snake-case throughout, so uppercasing
// restores the intended names regardless of the case the operator wrote.
func envVars(env map[string]any) []corev1.EnvVar {
	upper := make(map[string]string, len(env))
	for name, value := range env {
		upper[strings.ToUpper(name)] = fmt.Sprint(value)
	}
	names := slices.Sorted(maps.Keys(upper))
	vars := make([]corev1.EnvVar, 0, len(names))
	for _, name := range names {
		vars = append(vars, corev1.EnvVar{Name: name, Value: upper[name]})
	}
	return vars
}

// waitReady polls the Deployment until it reports at least one ready
// replica, bounded by min(ctx's deadline, the configured readiness timeout).
func (d *k8sDeployer) waitReady(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, d.config.readinessTimeout.Load())
	defer cancel()

	ticker := time.NewTicker(readinessPollInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		dep, err := d.client.AppsV1().Deployments(d.config.namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil && dep.Status.ReadyReplicas >= 1 {
			return nil
		}
		if err != nil {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			// lastErr distinguishes "pod just wasn't ready in time" from a
			// persistent Get failure (RBAC, API outage) hiding behind the timeout.
			return fmt.Errorf("waiting for ephemeral pyt deployment %s/%s to become ready: %w",
				d.config.namespace, name, errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

// delete removes the Deployment + Service best-effort: failures are logged,
// never returned, so a cleanup problem never changes the response the caller
// already has. It runs on its own bounded timeout, detached from ctx's
// cancellation, so cleanup still gets a chance to run when ctx is already
// done (deadline exceeded, caller gone).
func (d *k8sDeployer) delete(ctx context.Context, name string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deleteTimeout)
	defer cancel()

	var errs []error
	if _, err := withRetry(ctx, d.config.retry, func() (struct{}, error) {
		return struct{}{}, d.client.AppsV1().Deployments(d.config.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}); err != nil && !apierrors.IsNotFound(err) {
		errs = append(errs, fmt.Errorf("deleting deployment %s/%s: %w", d.config.namespace, name, err))
	}
	if _, err := withRetry(ctx, d.config.retry, func() (struct{}, error) {
		return struct{}{}, d.client.CoreV1().Services(d.config.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}); err != nil && !apierrors.IsNotFound(err) {
		errs = append(errs, fmt.Errorf("deleting service %s/%s: %w", d.config.namespace, name, err))
	}
	if len(errs) > 0 {
		d.log.Errorn("cleaning up ephemeral pyt deployment; the reaper will collect any orphan",
			logger.NewStringField("namespace", d.config.namespace),
			logger.NewStringField("name", name),
			obskit.Error(errors.Join(errs...)))
	}
}

// newInClusterClientset builds a real Kubernetes clientset: in-cluster
// config by default, falling back to a kubeconfig file for local dev. Moved
// here from the superseded pytscaler package, which this package replaces.
func newInClusterClientset(conf *config.Config) (kubernetes.Interface, error) {
	cfg, err := restConfig(conf)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes clientset: %w", err)
	}
	return client, nil
}

func restConfig(conf *config.Config) (*rest.Config, error) {
	if conf.GetBoolVar(true, "Processor.pytDeployer.inCluster", "K8S_IN_CLUSTER") {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("creating in-cluster kubernetes config: %w", err)
		}
		return cfg, nil
	}
	kubeConfigPath := conf.GetStringVar("", "Processor.pytDeployer.kubeConfigPath", "KUBECONFIG")
	if kubeConfigPath == "" {
		kubeConfigPath = clientcmd.RecommendedHomeFile
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeConfigPath},
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes config from %s: %w", kubeConfigPath, err)
	}
	return cfg, nil
}
