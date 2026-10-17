package workload

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"stackdome.io/cluster-agent/api/core/v1alpha1"
	"stackdome.io/cluster-agent/internal/controller"
)

func (r *Reconciler) reconcileCronJob(ctx context.Context, resource *v1alpha1.StackResource) (controller.SubReconcilerResult, error) {
	cj, err := r.applyCronJob(ctx, resource)
	if err != nil {
		return controller.ResultNil, err
	}
	if cj == nil {
		// Unreachable today (applyCronJob returns a CronJob or an error), but a
		// stop without a verdict would derive as available off the previous
		// pass's conditions if that ever changes.
		r.Status.ReportNotReady(ctx, resource, "CronJobNotReady", "waiting for the cronjob to be created")
		return controller.ResultStop, nil
	}
	if err := r.retireStaleWorkloads(ctx, resource, cj); err != nil {
		return controller.ResultNil, err
	}
	return r.evaluateCronJobStatus(ctx, resource, cj), nil
}

func (r *Reconciler) applyCronJob(ctx context.Context, resource *v1alpha1.StackResource) (*batchv1.CronJob, error) {
	volumeMountInfo, err := r.getVolumeMountInfoMap(ctx, resource)
	if err != nil {
		return nil, err
	}
	image, err := r.getImageForResource(ctx, resource)
	if err != nil {
		return nil, err
	}
	cj := &batchv1.CronJob{ObjectMeta: v1.ObjectMeta{Name: GetWorkloadNameForResource(resource), Namespace: resource.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cj, func() error {
		cj.ObjectMeta.Labels = mergeLabels(cj.ObjectMeta.Labels, IdentityLabels(resource))
		cj.Spec.Schedule = resource.Spec.Schedule
		cj.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
		cj.Spec.SuccessfulJobsHistoryLimit = ptr.To(int32(3))
		cj.Spec.FailedJobsHistoryLimit = ptr.To(int32(1))
		cj.Spec.JobTemplate.Spec.Completions = ptr.To(int32(1))
		cj.Spec.JobTemplate.Spec.Parallelism = ptr.To(int32(1))
		cj.Spec.JobTemplate.Spec.BackoffLimit = ptr.To(int32(6))
		cj.Spec.JobTemplate.Spec.Template = r.buildPodTemplateSpec(podTemplateConfig{
			resource: resource, image: *image, envVars: buildEnvVars(resource),
			volumeMountInfo: volumeMountInfo, restartPolicy: corev1.RestartPolicyOnFailure,
		})
		if err := r.resolveAndSetImagePullSecret(ctx, resource, &cj.Spec.JobTemplate.Spec.Template.Spec); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(resource, cj, r.Scheme)
	})
	if err != nil {
		return nil, err
	}
	return cj, nil
}

func (r *Reconciler) evaluateCronJobStatus(ctx context.Context, resource *v1alpha1.StackResource, cj *batchv1.CronJob) controller.SubReconcilerResult {
	r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadConverged, true, "CronJobScheduled", "cronjob installed with valid schedule")
	r.Status.SetCondition(resource, v1alpha1.StackResourceWorkloadAvailable, true, "CronJobScheduled", "cronjob installed with valid schedule")
	if cj.Status.LastScheduleTime != nil {
		resource.Status.LastRunTime = cj.Status.LastScheduleTime
	}
	if latest := r.latestCompletedChildJob(ctx, resource, cj); latest != nil {
		succeeded := latest.Status.Succeeded >= 1
		resource.Status.LastRunSucceeded = ptr.To(succeeded)
		if succeeded {
			resource.Status.LastFailureDetails = nil
			resource.Status.LastFailureDeploymentRevision = ""
		} else {
			r.capturePodFailureDetailsOnce(ctx, resource, latest.Name)
		}
	}
	return controller.ResultContinue
}

func (r *Reconciler) latestCompletedChildJob(ctx context.Context, resource *v1alpha1.StackResource, cj *batchv1.CronJob) *batchv1.Job {
	jobs := &batchv1.JobList{}
	if err := r.Client.List(ctx, jobs, client.InNamespace(resource.Namespace)); err != nil {
		controller.LoggerFromContext(ctx).Error(err, "failed to list cronjob child jobs")
		return nil
	}
	var latest *batchv1.Job
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if !v1.IsControlledBy(j, cj) || j.Status.CompletionTime == nil {
			continue
		}
		if latest == nil || j.Status.CompletionTime.After(latest.Status.CompletionTime.Time) {
			latest = j
		}
	}
	return latest
}
