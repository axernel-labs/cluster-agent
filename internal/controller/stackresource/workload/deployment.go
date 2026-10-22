package workload

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"stackdome.io/cluster-agent/api/core/v1alpha1"
	storagev1alpha1 "stackdome.io/cluster-agent/api/storage/v1alpha1"
	"stackdome.io/cluster-agent/internal/controller"
)

const (
	deploymentRevisionAnnotation = "deployment.kubernetes.io/revision"
	// After a new ReplicaSet becomes available, wait this long before declaring the
	// rollout settled. Gives pods time to crash so we can capture failure details.
	deploymentGracePeriodAfterNewReplicaSetAvailable = 3 * time.Minute
	// Pods must stay Ready for this many seconds before the workload controller
	// counts them as available. For Deployments this also keeps the old ReplicaSet
	// alive when a new pod starts then crashes shortly after; for StatefulSets it
	// guards the AvailableReplicas-based convergence check against a pod that goes
	// Ready then immediately crash-loops.
	workloadMinReadySeconds = 10
)

func (r *Reconciler) reconcileDeployment(ctx context.Context, resource *v1alpha1.StackResource) (controller.SubReconcilerResult, error) {
	logger := controller.LoggerFromContext(ctx)

	deployment, restartAnnotationApplied, err := r.applyDeployment(ctx, resource)
	if err != nil {
		return controller.ResultNil, err
	}
	if deployment == nil {
		return controller.ResultStop, nil
	}
	if err := r.retireStaleWorkloads(ctx, resource, deployment); err != nil {
		return controller.ResultNil, err
	}

	if restartAnnotationApplied {
		resource.Status.LastRestartRequestProcessedAt = ptr.To(v1.NewTime(time.Now().UTC()))
		r.Status.ReportNotReady(ctx, resource, "StackResourceDeploymentNotReady", "StackResource deployment restart requested")
		return controller.ResultStop, nil
	}

	logger.Info("deployment reconciled")
	return r.evaluateDeploymentStatus(ctx, resource, deployment), nil
}

// applyDeployment resolves inputs and creates or updates the Deployment for a
// StackResource. Returns (nil, nil) when the spec is invalid (terminal failure
// already reported on the resource).
func (r *Reconciler) applyDeployment(ctx context.Context, resource *v1alpha1.StackResource) (
	*appsv1.Deployment, bool, error) {
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

	replicas := ptr.To(int32(1))
	if resource.Spec.Replicas != nil {
		replicas = resource.Spec.Replicas
	}

	probes, err := buildProbes(resource)
	if err != nil {
		r.Status.ReportFailed(ctx, resource, "InvalidSpec", err.Error())
		return nil, false, nil
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: v1.ObjectMeta{
			Name:      GetWorkloadNameForResource(resource),
			Namespace: resource.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, deployment, func() error {
		r.applyDeploymentSpec(deployment, resource, *image, replicas, envVars, probes, volumeMountInfo, needsRestart)
		if err := r.resolveAndSetImagePullSecret(ctx, resource, &deployment.Spec.Template.Spec); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(resource, deployment, r.Scheme)
	})
	if err != nil {
		return nil, false, err
	}

	return deployment, needsRestart, nil
}

// evaluateDeploymentStatus syncs replica counts, evaluates convergence and
// availability conditions, captures failure details, and returns the
// appropriate sub-reconciler result.
func (r *Reconciler) evaluateDeploymentStatus(ctx context.Context, resource *v1alpha1.StackResource, deployment *appsv1.Deployment) controller.SubReconcilerResult {
	// --- Sync replica counts ---
	resource.Status.Replicas = deployment.Status.Replicas
	resource.Status.AvailableReplicas = deployment.Status.AvailableReplicas
	resource.Status.UpdatedReplicas = deployment.Status.UpdatedReplicas

	converged := deploymentConverged(deployment)
	serving := deploymentServing(deployment)

	// --- Convergence condition ---
	if converged {
		r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadConverged, true, "FullyConverged", "all replicas updated and available on the target revision")
		r.stampLastConverged(resource)
	} else {
		r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadConverged, false, "NotConverged", convergenceMessage(deployment))
	}

	revision := deployment.Annotations[deploymentRevisionAnnotation]

	// --- Serving: deployment has minimum availability ---
	if serving {
		r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadAvailable, true, "DeploymentServing", "deployment serving at minimum availability")
		if converged {
			resource.Status.LastFailureDetails = nil
			resource.Status.LastFailureDeploymentRevision = ""
		} else {
			r.captureFailureDetailsOnce(ctx, resource, revision)
		}

		// The kubelet probe covers the primary port, so a dead primary never
		// reaches this branch. Only the other declared ports are still in doubt.
		if requeueAfter := r.checkPorts(ctx, resource, revision, true); requeueAfter > 0 {
			// The deferred requeue lets the rest of the chain run meanwhile.
			return controller.ResultDeferredRequeue(requeueAfter)
		}

		if !converged && len(resource.Status.LastFailureDetails) == 0 &&
			!controller.DeploymentRolloutSettled(deployment, deploymentGracePeriodAfterNewReplicaSetAvailable) {
			return controller.ResultDeferredRequeue(10 * time.Second)
		}
		return controller.ResultContinue
	}

	// --- Not serving: capture failures, requeue until settled ---
	controller.LoggerFromContext(ctx).Info("deployment not serving")
	r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadAvailable, false, "DeploymentNotAvailable", "deployment is not yet available")
	r.captureFailureDetailsOnce(ctx, resource, revision)
	// A crash produces details above. A container that runs but never turns
	// Ready is usually a port nobody listens on, which only the verifier names.
	portRequeueAfter := r.checkPorts(ctx, resource, revision, false)
	r.Status.ReportNotReady(ctx, resource, "StackResourceDeploymentNotReady", "StackResourceDeploymentNotReady")

	if !controller.DeploymentRolloutSettled(deployment, deploymentGracePeriodAfterNewReplicaSetAvailable) {
		return controller.ResultRequeueAfter(10 * time.Second)
	}
	// A refused port is nobody's event: without this the grace clock would
	// never run out and the resource would sit Pending forever.
	if portRequeueAfter > 0 {
		return controller.ResultRequeueAfter(portRequeueAfter)
	}
	return controller.ResultStop
}

// stampLastConverged records the convergence timestamp for the current revision,
// write-once per revision.
func (r *Reconciler) stampLastConverged(resource *v1alpha1.StackResource) {
	rev, ok := resource.Annotations[v1alpha1.RevisionAnnotation]
	if !ok {
		return
	}
	if lc := resource.Status.LastConverged; lc == nil || lc.Revision != rev {
		resource.Status.LastConverged = &v1alpha1.StackResourceConvergenceRecord{
			Revision: rev,
			At:       v1.Now(),
		}
	}
}

// captureFailureDetailsOnce records crash/error details from pods for the
// current deployment revision. The revision tag prevents redundant pod
// listings on subsequent reconciles.
func (r *Reconciler) captureFailureDetailsOnce(ctx context.Context, resource *v1alpha1.StackResource, deploymentRevision string) {
	if resource.Status.LastFailureDeploymentRevision == deploymentRevision {
		return
	}
	failureDetails, err := captureLastFailureDetails(ctx, r.UncachedClient, resource, deploymentRevision)
	if err != nil {
		controller.LoggerFromContext(ctx).Error(err, "failed to capture failure details")
	}
	resource.Status.LastFailureDetails = failureDetails
	if len(resource.Status.LastFailureDetails) > 0 {
		resource.Status.LastFailureDeploymentRevision = deploymentRevision
	}
}

func convergenceMessage(deployment *appsv1.Deployment) string {
	desired := int32(1)
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}
	msg := fmt.Sprintf("rollout not converged: %d/%d updated, %d/%d ready, %d unavailable",
		deployment.Status.UpdatedReplicas, desired,
		deployment.Status.ReadyReplicas, desired,
		deployment.Status.UnavailableReplicas)
	for _, c := range deployment.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse {
			msg += "; " + c.Reason
			break
		}
	}
	return msg
}

// ---------------------------------------------------------------------------
// Deployment spec construction
// ---------------------------------------------------------------------------

func (r *Reconciler) applyDeploymentSpec(
	deployment *appsv1.Deployment,
	resource *v1alpha1.StackResource,
	image string,
	replicas *int32,
	envVars []corev1.EnvVar,
	probes probeSet,
	volumeMountInfo map[string]*storagev1alpha1.Volume,
	needsRestart bool,
) {
	deployment.ObjectMeta.Labels = mergeLabels(deployment.ObjectMeta.Labels, IdentityLabels(resource))
	deployment.Spec.Selector = &v1.LabelSelector{
		MatchLabels: GetWorkloadLabelForResource(resource),
	}
	deployment.Spec.Replicas = replicas
	deployment.Spec.Strategy.Type = appsv1.RollingUpdateDeploymentStrategyType
	deployment.Spec.Strategy.RollingUpdate = &appsv1.RollingUpdateDeployment{
		MaxUnavailable: ptr.To(intstr.FromInt32(maxUnavailableForReplicas(*replicas))),
		MaxSurge:       ptr.To(intstr.FromString("25%")),
	}
	deployment.Spec.ProgressDeadlineSeconds = ptr.To(int32(300))
	deployment.Spec.MinReadySeconds = workloadMinReadySeconds

	deployment.Spec.Template = r.buildPodTemplateSpec(podTemplateConfig{
		resource:        resource,
		image:           image,
		envVars:         envVars,
		probes:          probes,
		volumeMountInfo: volumeMountInfo,
		needsRestart:    needsRestart,
	})
}
