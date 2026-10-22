package workload

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"stackdome.io/cluster-agent/api/core/v1alpha1"
	"stackdome.io/cluster-agent/internal/controller"
)

func (r *Reconciler) reconcileStatefulSet(ctx context.Context, resource *v1alpha1.StackResource) (controller.SubReconcilerResult, error) {
	sts, restartApplied, err := r.applyStatefulSet(ctx, resource)
	if err != nil {
		return controller.ResultNil, err
	}
	if sts == nil {
		return controller.ResultStop, nil
	}
	if err := r.retireStaleWorkloads(ctx, resource, sts); err != nil {
		return controller.ResultNil, err
	}
	if restartApplied {
		resource.Status.LastRestartRequestProcessedAt = ptr.To(v1.NewTime(time.Now().UTC()))
		r.Status.ReportNotReady(ctx, resource, "StackResourceStatefulSetNotReady", "StackResource statefulset restart requested")
		return controller.ResultStop, nil
	}
	return r.evaluateStatefulSetStatus(ctx, resource, sts), nil
}

func (r *Reconciler) applyStatefulSet(ctx context.Context, resource *v1alpha1.StackResource) (*appsv1.StatefulSet, bool, error) {
	volumeMountInfo, err := r.getVolumeMountInfoMap(ctx, resource)
	if err != nil {
		return nil, false, err
	}
	image, err := r.getImageForResource(ctx, resource)
	if err != nil {
		return nil, false, err
	}
	envVars := buildEnvVars(resource)
	needsRestart := r.requiresRestart(resource)
	probes, err := buildProbes(resource)
	if err != nil {
		r.Status.ReportFailed(ctx, resource, "InvalidSpec", err.Error())
		return nil, false, nil
	}

	sts := &appsv1.StatefulSet{ObjectMeta: v1.ObjectMeta{Name: GetWorkloadNameForResource(resource), Namespace: resource.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.ObjectMeta.Labels = mergeLabels(sts.ObjectMeta.Labels, IdentityLabels(resource))
		sts.Spec.Replicas = ptr.To(int32(1))
		sts.Spec.ServiceName = GetWorkloadNameForResource(resource)
		sts.Spec.PodManagementPolicy = appsv1.OrderedReadyPodManagement
		sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType}
		// Require the pod to stay Ready for this long before it counts toward
		// AvailableReplicas, which evaluateStatefulSetStatus gates convergence on.
		sts.Spec.MinReadySeconds = workloadMinReadySeconds
		sts.Spec.Selector = &v1.LabelSelector{MatchLabels: GetWorkloadLabelForResource(resource)}
		sts.Spec.Template = r.buildPodTemplateSpec(podTemplateConfig{
			resource: resource, image: *image, envVars: envVars, probes: probes,
			volumeMountInfo: volumeMountInfo, restartPolicy: corev1.RestartPolicyAlways, needsRestart: needsRestart,
		})
		if err := r.resolveAndSetImagePullSecret(ctx, resource, &sts.Spec.Template.Spec); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(resource, sts, r.Scheme)
	})
	if err != nil {
		return nil, false, err
	}
	return sts, needsRestart, nil
}

func (r *Reconciler) evaluateStatefulSetStatus(ctx context.Context, resource *v1alpha1.StackResource, sts *appsv1.StatefulSet) controller.SubReconcilerResult {
	resource.Status.Replicas = sts.Status.Replicas
	resource.Status.AvailableReplicas = sts.Status.AvailableReplicas
	resource.Status.UpdatedReplicas = sts.Status.UpdatedReplicas

	rolledOut := sts.Status.ObservedGeneration == sts.Generation && sts.Status.CurrentRevision == sts.Status.UpdateRevision
	converged := rolledOut &&
		sts.Status.ReadyReplicas == 1 && sts.Status.UpdatedReplicas == 1 && sts.Status.AvailableReplicas == 1

	if converged {
		r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadAvailable, true, "StatefulSetAvailable", "statefulset available")
		r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadConverged, true, "FullyConverged", "statefulset pod ready on the target revision")
		r.stampLastConverged(resource)
		resource.Status.LastFailureDetails = nil
		resource.Status.LastFailureDeploymentRevision = ""

		// The kubelet probe guards only the primary port, so a secondary port
		// nobody listens on would otherwise stay silently green.
		if requeueAfter := r.checkPorts(ctx, resource, sts.Status.UpdateRevision, true); requeueAfter > 0 {
			// The deferred requeue lets the rest of the chain run meanwhile.
			return controller.ResultDeferredRequeue(requeueAfter)
		}
		return controller.ResultContinue
	}

	r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadConverged, false, "NotConverged",
		fmt.Sprintf("statefulset not converged: ready=%d updated=%d available=%d", sts.Status.ReadyReplicas, sts.Status.UpdatedReplicas, sts.Status.AvailableReplicas))
	r.capturePodFailureDetailsOnce(ctx, resource, sts.Status.UpdateRevision)
	// A crash produces details above. A container that runs but never turns
	// Ready is usually a port nobody listens on, which only the verifier names.
	portRequeueAfter := r.checkPorts(ctx, resource, sts.Status.UpdateRevision, false)
	r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadAvailable, false, "StatefulSetNotAvailable", "statefulset pod not ready")
	r.Status.ReportNotReady(ctx, resource, "StackResourceStatefulSetNotReady", "statefulset pod not yet ready")

	// StatefulSets have no ProgressDeadlineSeconds, so there is no built-in "settled"
	// signal like the Deployment path uses. Once we've captured why the pod is failing,
	// stop the active 10s requeue loop and rely on the StatefulSet watch to re-trigger
	// on recovery (pod becoming Ready) or a new revision — otherwise a crash-looping
	// StatefulSet requeues forever. While no failure is captured yet (pod still
	// starting), keep polling: a crash-looping pod does not reliably change the
	// StatefulSet's status, so the watch alone would not surface the crash.
	//
	// TODO: a pod stuck Pending/unschedulable (e.g. insufficient resources) never
	// enters a crash state, so LastFailureDetails stays empty and this still requeues
	// indefinitely. The Deployment path bounds that via ProgressDeadlineSeconds; a
	// StatefulSet equivalent would need a per-revision "rollout started" timestamp on
	// status and our own deadline. Accepted for now.
	//
	// A running grace clock outranks the stop: a refused port is nobody's
	// event, so without this the clock would never run out and the resource
	// would sit Pending forever.
	if portRequeueAfter > 0 {
		return controller.ResultRequeueAfter(portRequeueAfter)
	}
	if len(resource.Status.LastFailureDetails) > 0 {
		return controller.ResultStop
	}
	return controller.ResultRequeueAfter(10 * time.Second)
}
