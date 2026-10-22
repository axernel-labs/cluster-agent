package workload

import (
	"context"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"stackdome.io/cluster-agent/api/core/v1alpha1"
	"stackdome.io/cluster-agent/internal/controller"
)

// workloadKinds is every object type a StackResource can run as.
func workloadKinds() []client.Object {
	return []client.Object{&appsv1.Deployment{}, &appsv1.StatefulSet{}, &batchv1.Job{}, &batchv1.CronJob{}}
}

// retireStaleWorkloads deletes what the resource ran as before its workload
// type changed, e.g. the Deployment left behind by a Service → CronJob switch.
//   - Every kind is named after the resource, so a Get by name finds it.
//   - Call it only once `current` exists, so something is always running.
//   - Only objects this resource controls are deleted.
func (r *Reconciler) retireStaleWorkloads(ctx context.Context, resource *v1alpha1.StackResource, current client.Object) error {
	key := client.ObjectKey{Name: GetWorkloadNameForResource(resource), Namespace: resource.Namespace}
	for _, stale := range workloadKinds() {
		if reflect.TypeOf(stale) == reflect.TypeOf(current) {
			continue
		}
		switch err := r.Client.Get(ctx, key, stale); {
		case apierrors.IsNotFound(err):
			continue
		case err != nil:
			return err
		}
		if !v1.IsControlledBy(stale, resource) {
			continue
		}
		if err := r.Client.Delete(ctx, stale, client.PropagationPolicy(v1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			return err
		}
		controller.LoggerFromContext(ctx).Info("retired workload left by a previous workload type",
			"kind", reflect.TypeOf(stale).Elem().Name(), "name", key.Name)
	}
	return nil
}
