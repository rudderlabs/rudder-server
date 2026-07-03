package pytscaler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	workspaceID = "2abcDEF" // pyt-2abcdef (lowercased)
	deployName  = "pyt-2abcdef"
)

func TestGetReplicaCount(t *testing.T) {
	t.Run("returns the current replica count", func(t *testing.T) {
		cs := newFakeClient(t, deployment(deployName, "pyt-tenant-a", 3))
		s := newScaler(t, cs, config.New())

		count, err := s.GetReplicaCount(context.Background(), workspaceID)
		require.NoError(t, err)
		require.Equal(t, 3, count)
	})

	t.Run("locates the deployment by name across namespaces, ignoring others", func(t *testing.T) {
		cs := newFakeClient(
			t,
			deployment("pyt-other", "pyt-tenant-a", 5),       // different name
			deployment(deployName, "pyt-tenant-b", 2),        // the one we want
			deployment("some-deployment", "pyt-tenant-b", 7), // unrelated
		)
		s := newScaler(t, cs, config.New())

		count, err := s.GetReplicaCount(context.Background(), workspaceID)
		require.NoError(t, err)
		require.Equal(t, 2, count)
	})

	t.Run("returns ErrDeploymentNotFound when no deployment matches", func(t *testing.T) {
		cs := newFakeClient(t, deployment("pyt-someoneelse", "pyt-tenant-a", 0))
		s := newScaler(t, cs, config.New())

		_, err := s.GetReplicaCount(context.Background(), workspaceID)
		require.ErrorIs(t, err, ErrDeploymentNotFound)
	})

	t.Run("errors when the same name exists in multiple namespaces", func(t *testing.T) {
		cs := newFakeClient(
			t,
			deployment(deployName, "pyt-tenant-a", 0),
			deployment(deployName, "pyt-tenant-b", 0),
		)
		s := newScaler(t, cs, config.New())

		_, err := s.GetReplicaCount(context.Background(), workspaceID)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrDeploymentNotFound)
	})

	t.Run("errors on an empty workspaceID", func(t *testing.T) {
		s := newScaler(t, newFakeClient(t), config.New())
		_, err := s.GetReplicaCount(context.Background(), "")
		require.Error(t, err)
	})

	t.Run("retries transient k8s errors then succeeds", func(t *testing.T) {
		cs := newFakeClient(t, deployment(deployName, "pyt-tenant-a", 1))
		// Fail the first two GetScale calls with a transient error, then let it through.
		var attempts int
		cs.PrependReactor("get", "deployments", func(action ktesting.Action) (bool, runtime.Object, error) {
			if action.(ktesting.GetActionImpl).GetSubresource() != "scale" {
				return false, nil, nil
			}
			attempts++
			if attempts <= 2 {
				return true, nil, apierrors.NewTooManyRequests("slow down", 1)
			}
			return false, nil, nil // fall through to the scale reactor
		})

		conf := config.New()
		conf.Set("Processor.UserTransformer.pytScaler.retry.initialInterval", "1ms")
		conf.Set("Processor.UserTransformer.pytScaler.retry.maxElapsedTime", "5s")
		s := newScaler(t, cs, conf)

		count, err := s.GetReplicaCount(context.Background(), workspaceID)
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.GreaterOrEqual(t, attempts, 3, "GetScale should have been retried")
	})
}

func TestSetReplicaCount(t *testing.T) {
	t.Run("sets the replica count via the scale subresource", func(t *testing.T) {
		cs := newFakeClient(t, deployment(deployName, "pyt-tenant-a", 0))
		s := newScaler(t, cs, config.New())

		require.NoError(t, s.SetReplicaCount(context.Background(), workspaceID, 1))

		require.EqualValues(t, 1, currentReplicas(t, cs, "pyt-tenant-a", deployName))
		require.True(t, scaleUpdated(cs), "UpdateScale should have been called")
	})

	t.Run("writes the count unconditionally, even when already scaled up", func(t *testing.T) {
		// The layer has no idempotency of its own — that is the caller's duty.
		cs := newFakeClient(t, deployment(deployName, "pyt-tenant-a", 2))
		s := newScaler(t, cs, config.New())

		require.NoError(t, s.SetReplicaCount(context.Background(), workspaceID, 5))

		require.EqualValues(t, 5, currentReplicas(t, cs, "pyt-tenant-a", deployName))
	})

	t.Run("locates the deployment by name across namespaces", func(t *testing.T) {
		cs := newFakeClient(
			t,
			deployment("pyt-other", "pyt-tenant-a", 0),
			deployment(deployName, "pyt-tenant-b", 0),
		)
		s := newScaler(t, cs, config.New())

		require.NoError(t, s.SetReplicaCount(context.Background(), workspaceID, 1))

		require.EqualValues(t, 1, currentReplicas(t, cs, "pyt-tenant-b", deployName))
		require.EqualValues(t, 0, currentReplicas(t, cs, "pyt-tenant-a", "pyt-other"),
			"deployments with other names must be left untouched")
	})

	t.Run("returns ErrDeploymentNotFound when no deployment matches", func(t *testing.T) {
		cs := newFakeClient(t, deployment("pyt-someoneelse", "pyt-tenant-a", 0))
		s := newScaler(t, cs, config.New())

		err := s.SetReplicaCount(context.Background(), workspaceID, 1)
		require.ErrorIs(t, err, ErrDeploymentNotFound)
	})

	t.Run("errors on an empty workspaceID", func(t *testing.T) {
		s := newScaler(t, newFakeClient(t), config.New())
		require.Error(t, s.SetReplicaCount(context.Background(), "", 1))
	})
}

func TestNoopScaler(t *testing.T) {
	s := NewNoop(logger.NOP)
	// Reports as already scaled so the get-before-set caller skips scaling.
	count, err := s.GetReplicaCount(context.Background(), "anyWorkspace")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NoError(t, s.SetReplicaCount(context.Background(), "anyWorkspace", 1))
}

// newScaler builds a k8sScaler over an injected (fake) clientset.
func newScaler(t *testing.T, cs kubernetes.Interface, conf *config.Config) Scaler {
	t.Helper()
	s, err := New(conf, logger.NOP, WithClientset(cs))
	require.NoError(t, err)
	return s
}

func deployment(name, namespace string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
	}
}

// newFakeClient returns a fake clientset that emulates real cluster behaviour for
// the two things the default fake does NOT do and which this scaler relies on:
//  1. the scale subresource (GetScale/UpdateScale) backed by the deployment's
//     replica count — the default fake would panic type-asserting the deployment
//     to a *Scale;
//  2. metadata.name field-selector filtering on List — the default fake honours
//     only label selectors, so a cluster-wide List would otherwise return every
//     deployment regardless of name.
func newFakeClient(t *testing.T, deployments ...*appsv1.Deployment) *fake.Clientset {
	t.Helper()
	objs := make([]runtime.Object, len(deployments))
	for i, d := range deployments {
		objs[i] = d
	}
	cs := fake.NewClientset(objs...)
	gvk := appsv1.SchemeGroupVersion.WithKind("Deployment")

	cs.PrependReactor("list", "deployments", func(action ktesting.Action) (bool, runtime.Object, error) {
		la := action.(ktesting.ListActionImpl)
		obj, err := cs.Tracker().List(la.GetResource(), gvk, la.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		list := obj.(*appsv1.DeploymentList)
		fieldSel := la.GetListRestrictions().Fields
		if fieldSel == nil || fieldSel.Empty() {
			return true, list, nil
		}
		filtered := &appsv1.DeploymentList{}
		for i := range list.Items {
			d := list.Items[i]
			if fieldSel.Matches(fields.Set{"metadata.name": d.Name, "metadata.namespace": d.Namespace}) {
				filtered.Items = append(filtered.Items, d)
			}
		}
		return true, filtered, nil
	})

	cs.PrependReactor("get", "deployments", func(action ktesting.Action) (bool, runtime.Object, error) {
		ga := action.(ktesting.GetActionImpl)
		if ga.GetSubresource() != "scale" {
			return false, nil, nil
		}
		d, err := getDeployment(cs, ga.GetNamespace(), ga.GetName())
		if err != nil {
			return true, nil, err
		}
		return true, scaleOf(d), nil
	})

	cs.PrependReactor("update", "deployments", func(action ktesting.Action) (bool, runtime.Object, error) {
		ua := action.(ktesting.UpdateActionImpl)
		if ua.GetSubresource() != "scale" {
			return false, nil, nil
		}
		scale := ua.GetObject().(*autoscalingv1.Scale)
		d, err := getDeployment(cs, ua.GetNamespace(), scale.Name)
		if err != nil {
			return true, nil, err
		}
		replicas := scale.Spec.Replicas
		d.Spec.Replicas = &replicas
		if err := cs.Tracker().Update(action.GetResource(), d, d.Namespace); err != nil {
			return true, nil, err
		}
		return true, scale, nil
	})

	return cs
}

func getDeployment(cs *fake.Clientset, namespace, name string) (*appsv1.Deployment, error) {
	gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	obj, err := cs.Tracker().Get(gvr, namespace, name)
	if err != nil {
		return nil, err
	}
	return obj.(*appsv1.Deployment), nil
}

func scaleOf(d *appsv1.Deployment) *autoscalingv1.Scale {
	var replicas int32
	if d.Spec.Replicas != nil {
		replicas = *d.Spec.Replicas
	}
	return &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: d.Name, Namespace: d.Namespace},
		Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
	}
}

func currentReplicas(t *testing.T, cs *fake.Clientset, namespace, name string) int32 {
	t.Helper()
	d, err := getDeployment(cs, namespace, name)
	require.NoError(t, err)
	require.NotNil(t, d.Spec.Replicas)
	return *d.Spec.Replicas
}

func scaleUpdated(cs *fake.Clientset) bool {
	for _, a := range cs.Actions() {
		if a.GetVerb() == "update" && a.GetSubresource() == "scale" {
			return true
		}
	}
	return false
}
