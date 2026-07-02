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

func (s *k8sScaler) GetReplicaCount(ctx context.Context, workspaceID string) (int, error) {
	if workspaceID == "" {
		return 0, fmt.Errorf("workspaceID is empty")
	}
	name := deploymentName(workspaceID)
	namespace, err := s.resolveNamespace(ctx, name)
	if err != nil {
		return 0, err
	}
	scale, err := withRetry(ctx, s.retry, func() (*autoscalingv1.Scale, error) {
		return s.client.AppsV1().Deployments(namespace).GetScale(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return 0, fmt.Errorf("getting scale for deployment %s/%s: %w", namespace, name, err)
	}
	return int(scale.Spec.Replicas), nil
}

func (s *k8sScaler) SetReplicaCount(ctx context.Context, workspaceID string, count int) error {
	if workspaceID == "" {
		return fmt.Errorf("workspaceID is empty")
	}
	name := deploymentName(workspaceID)
	namespace, err := s.resolveNamespace(ctx, name)
	if err != nil {
		return err
	}
	scale := &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       autoscalingv1.ScaleSpec{Replicas: int32(count)}, //nolint:gosec // replica count is a small, bounded value
	}
	if _, err := withRetry(ctx, s.retry, func() (*autoscalingv1.Scale, error) {
		return s.client.AppsV1().Deployments(namespace).UpdateScale(ctx, name, scale, metav1.UpdateOptions{})
	}); err != nil {
		return fmt.Errorf("scaling deployment %s/%s to %d replicas: %w", namespace, name, count, err)
	}
	s.log.Infon("scaled pyt deployment",
		logger.NewStringField("namespace", namespace),
		logger.NewStringField("deployment", name),
		logger.NewIntField("replicas", int64(count)))
	return nil
}

// resolveNamespace finds the single Deployment named name across all namespaces
// and returns its namespace. metadata.name is a universally-supported field
// selector, so this needs no namespace config or ExternalName parsing.
func (s *k8sScaler) resolveNamespace(ctx context.Context, name string) (string, error) {
	deployments, err := withRetry(ctx, s.retry, func() (*deploymentNames, error) {
		list, err := s.client.AppsV1().Deployments(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String(),
		})
		if err != nil {
			return nil, err
		}
		out := &deploymentNames{}
		for _, d := range list.Items {
			out.namespaces = append(out.namespaces, d.Namespace)
		}
		return out, nil
	})
	if err != nil {
		return "", fmt.Errorf("listing deployments named %s: %w", name, err)
	}
	switch len(deployments.namespaces) {
	case 0:
		return "", fmt.Errorf("%w: %s", ErrDeploymentNotFound, name)
	case 1:
		return deployments.namespaces[0], nil
	default:
		return "", fmt.Errorf("found %d deployments named %s across namespaces; expected exactly one",
			len(deployments.namespaces), name)
	}
}

// deploymentNames carries the namespaces of the Deployments matched by name. It
// exists so withRetry returns a small value rather than a full DeploymentList.
type deploymentNames struct {
	namespaces []string
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
