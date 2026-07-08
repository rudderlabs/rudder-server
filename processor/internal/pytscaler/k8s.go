package pytscaler

import (
	"context"
	"fmt"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type k8sScaler struct {
	client kubernetes.Interface
	log    logger.Logger
	retry  retrySettings
}

func (s *k8sScaler) EnsureScaled(ctx context.Context, workspaceID string, count int) error {
	if workspaceID == "" {
		return fmt.Errorf("workspaceID is empty")
	}
	name := deploymentName(workspaceID)
	found, err := s.findDeployment(ctx, name)
	if err != nil {
		return err
	}
	if found.replicas > 0 {
		// Already running; no-op.
		return nil
	}
	scale := &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: found.namespace},
		Spec:       autoscalingv1.ScaleSpec{Replicas: int32(count)}, //nolint:gosec // replica count is a small, bounded value
	}
	if _, err := withRetry(ctx, s.retry, func() (*autoscalingv1.Scale, error) {
		return s.client.AppsV1().Deployments(found.namespace).UpdateScale(ctx, name, scale, metav1.UpdateOptions{})
	}); err != nil {
		return fmt.Errorf("scaling deployment %s/%s to %d replicas: %w", found.namespace, name, count, err)
	}
	s.log.Infon("scaled pyt deployment",
		logger.NewStringField("namespace", found.namespace),
		logger.NewStringField("deployment", name),
		logger.NewIntField("replicas", int64(count)))
	return nil
}

// findDeployment finds the single Deployment named name across all namespaces
// and returns its namespace and current replica count. metadata.name is a
// universally-supported field selector, so this needs no namespace config or
// ExternalName parsing — and the List response already carries spec.replicas,
// so no separate GetScale read is needed.
func (s *k8sScaler) findDeployment(ctx context.Context, name string) (deploymentRef, error) {
	found, err := withRetry(ctx, s.retry, func() ([]deploymentRef, error) {
		list, err := s.client.AppsV1().Deployments(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String(),
		})
		if err != nil {
			return nil, err
		}
		out := make([]deploymentRef, 0, len(list.Items))
		for _, d := range list.Items {
			ref := deploymentRef{namespace: d.Namespace, replicas: 1} // nil spec.replicas defaults to 1
			if d.Spec.Replicas != nil {
				ref.replicas = *d.Spec.Replicas
			}
			out = append(out, ref)
		}
		return out, nil
	})
	if err != nil {
		return deploymentRef{}, fmt.Errorf("listing deployments named %s: %w", name, err)
	}
	switch len(found) {
	case 0:
		return deploymentRef{}, fmt.Errorf("%w: %s", ErrDeploymentNotFound, name)
	case 1:
		return found[0], nil
	default:
		return deploymentRef{}, fmt.Errorf("found %d deployments named %s across namespaces; expected exactly one",
			len(found), name)
	}
}

// deploymentRef carries the namespace and current replica count of a Deployment
// matched by name. It exists so withRetry returns a small value rather than a
// full DeploymentList.
type deploymentRef struct {
	namespace string
	replicas  int32
}

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
	if conf.GetBoolVar(true, "Processor.UserTransformer.pytScaler.inCluster", "K8S_IN_CLUSTER") {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("creating in-cluster kubernetes config: %w", err)
		}
		return cfg, nil
	}
	kubeConfigPath := conf.GetStringVar("", "Processor.UserTransformer.pytScaler.kubeConfigPath", "KUBECONFIG")
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
