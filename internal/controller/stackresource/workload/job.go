package workload

import (
	"context"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"stackdome.io/cluster-agent/api/core/v1alpha1"
	"stackdome.io/cluster-agent/internal/controller"
)

func (r *Reconciler) reconcileJob(ctx context.Context, resource *v1alpha1.StackResource) (controller.SubReconcilerResult, error) {
	job := &batchv1.Job{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: GetWorkloadNameForResource(resource), Namespace: resource.Namespace}, job)
	switch {
	case apierrors.IsNotFound(err):
		created, applyErr := r.applyJob(ctx, resource)
		if applyErr != nil || created == nil {
			if applyErr == nil {
				// Unreachable today (applyJob returns a Job or an error), but a
				// stop without a verdict would derive as available off the
				// previous run's conditions if that ever changes.
				r.Status.ReportNotReady(ctx, resource, "JobRunning", "waiting for the job to be created")
			}
			return controller.ResultStop, applyErr
		}
		if err := r.retireStaleWorkloads(ctx, resource, created); err != nil {
			return controller.ResultNil, err
		}
		r.Status.ReportNotReady(ctx, resource, "JobRunning", "job created, waiting for completion")
		return controller.ResultRequeueAfter(5 * time.Second), nil
	case err != nil:
		return controller.ResultNil, err
	}
	if err := r.retireStaleWorkloads(ctx, resource, job); err != nil {
		return controller.ResultNil, err
	}

	// If we have a new revision we delete the old job and requeue after 2 seconds to wait for the new job to be created.
	if jobFinished(job) && job.Annotations[v1alpha1.RevisionAnnotation] != resource.Annotations[v1alpha1.RevisionAnnotation] {
		if delErr := r.Client.Delete(ctx, job, client.PropagationPolicy(v1.DeletePropagationBackground)); delErr != nil && !apierrors.IsNotFound(delErr) {
			return controller.ResultNil, delErr
		}
		// The old Job is gone and the new one does not exist yet. Without a
		// verdict this window derives as available off the previous run's
		// WorkloadConverged=True.
		r.Status.ReportNotReady(ctx, resource, "JobRunning", "waiting for the new revision's job to be created")
		return controller.ResultRequeueAfter(2 * time.Second), nil
	}
	return r.evaluateJobStatus(ctx, resource, job), nil
}

func jobFinished(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *Reconciler) applyJob(ctx context.Context, resource *v1alpha1.StackResource) (*batchv1.Job, error) {
	volumeMountInfo, err := r.getVolumeMountInfoMap(ctx, resource)
	if err != nil {
		return nil, err
	}
	image, err := r.getImageForResource(ctx, resource)
	if err != nil {
		return nil, err
	}
	job := &batchv1.Job{
		ObjectMeta: v1.ObjectMeta{
			Name:        GetWorkloadNameForResource(resource),
			Namespace:   resource.Namespace,
			Labels:      mergeLabels(GetWorkloadLabelForResource(resource), IdentityLabels(resource)),
			Annotations: map[string]string{v1alpha1.RevisionAnnotation: resource.Annotations[v1alpha1.RevisionAnnotation]},
		},
		Spec: batchv1.JobSpec{
			Completions:  ptr.To(int32(1)),
			Parallelism:  ptr.To(int32(1)),
			BackoffLimit: ptr.To(int32(6)),
			Template: r.buildPodTemplateSpec(podTemplateConfig{
				resource: resource, image: *image, envVars: buildEnvVars(resource),
				volumeMountInfo: volumeMountInfo, restartPolicy: corev1.RestartPolicyOnFailure,
			}),
		},
	}
	if err := r.resolveAndSetImagePullSecret(ctx, resource, &job.Spec.Template.Spec); err != nil {
		return nil, err
	}
	if err := controllerutil.SetControllerReference(resource, job, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Client.Create(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

func (r *Reconciler) evaluateJobStatus(ctx context.Context, resource *v1alpha1.StackResource, job *batchv1.Job) controller.SubReconcilerResult {
	if job.Status.Succeeded >= 1 {
		resource.Status.LastRunTime = job.Status.CompletionTime
		resource.Status.LastRunSucceeded = ptr.To(true)
		resource.Status.LastFailureDetails = nil
		resource.Status.LastFailureDeploymentRevision = ""
		r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadConverged, true, "JobComplete", "job completed successfully")
		r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadAvailable, true, "JobComplete", "job completed successfully")
		return controller.ResultContinue
	}
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			resource.Status.LastRunTime = ptr.To(v1.Now())
			resource.Status.LastRunSucceeded = ptr.To(false)
			r.capturePodFailureDetailsOnce(ctx, resource, resource.Annotations[v1alpha1.RevisionAnnotation])
			r.Status.ReportFailed(ctx, resource, "JobFailed", c.Message)
			return controller.ResultStop
		}
	}
	// Job still running. Capture why it is failing if a pod has already crashed
	// (capturePodFailureDetailsOnce dedups on revision).
	r.capturePodFailureDetailsOnce(ctx, resource, resource.Annotations[v1alpha1.RevisionAnnotation])
	r.Status.ReportNotReady(ctx, resource, "JobRunning", "job is still running")

	// Mirror the StatefulSet path: once the failure is captured, stop the active
	// requeue loop and rely on the Job watch. Under OnFailure the kubelet restarts
	// the container in place, which does not change the Job's status, so a
	// crash-looping Job would otherwise requeue every 10s until backoffLimit is
	// exhausted. The terminal transition (JobFailed or Succeeded) does change Job
	// status and re-triggers reconcile via the owner-referenced Job watch. While
	// nothing is captured yet (pod still starting/running), keep polling so a
	// developing crash is recorded.
	//
	// TODO: same limitation as the StatefulSet path — a pod stuck Pending/unschedulable
	// never crashes, so LastFailureDetails stays empty and this still requeues
	// indefinitely. Accepted for now.
	if len(resource.Status.LastFailureDetails) > 0 {
		return controller.ResultStop
	}
	return controller.ResultRequeueAfter(10 * time.Second)
}
