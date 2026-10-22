package workload

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"stackdome.io/cluster-agent/api/core/v1alpha1"
	"stackdome.io/cluster-agent/internal/controller"
	"stackdome.io/cluster-agent/internal/controller/mocks"
)

func setupCronJobCreateOrUpdate(mockClient *mocks.MockClient, cj *batchv1.CronJob) {
	mockClient.EXPECT().
		Get(gomock.Any(), client.ObjectKey{Name: cj.Name, Namespace: cj.Namespace}, gomock.AssignableToTypeOf(&batchv1.CronJob{})).
		DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
			*obj.(*batchv1.CronJob) = *cj
			return nil
		})
	mockClient.EXPECT().
		Update(gomock.Any(), gomock.AssignableToTypeOf(&batchv1.CronJob{}), gomock.Any()).
		Return(nil).
		AnyTimes()
}

func setupChildJobList(mockClient *mocks.MockClient, jobs []batchv1.Job) {
	mockClient.EXPECT().
		List(gomock.Any(), gomock.AssignableToTypeOf(&batchv1.JobList{}), gomock.Any()).
		DoAndReturn(func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
			list.(*batchv1.JobList).Items = jobs
			return nil
		})
}

func setupCrashingPodsForCronJob(mockUncached *mocks.MockClient, exitCode int32) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-resource-job-pod",
			Namespace:         "test-ns",
			CreationTimestamp: metav1.Now(),
			Labels: map[string]string{
				"resource": "test-resource",
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:         "test-resource",
					RestartCount: 1,
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: exitCode,
							Reason:   "Error",
						},
					},
				},
			},
		},
	}

	mockUncached.EXPECT().
		List(gomock.Any(), gomock.AssignableToTypeOf(&corev1.PodList{}), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
			list.(*corev1.PodList).Items = []corev1.Pod{pod}
			return nil
		})
}

var _ = Describe("cronJobReconciler", func() {
	var (
		mockCtrl     *gomock.Controller
		mockClient   *mocks.MockClient
		mockUncached *mocks.MockClient
		mockDepCheck *mocks.MockDependencyChecker
		reconciler   *Reconciler
		resource     *v1alpha1.StackResource
		ctx          context.Context
		scheme       *runtime.Scheme
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockClient = mocks.NewMockClient(mockCtrl)
		mockUncached = mocks.NewMockClient(mockCtrl)
		mockDepCheck = mocks.NewMockDependencyChecker(mockCtrl)
		ctx = context.Background()

		scheme = runtime.NewScheme()
		Expect(v1alpha1.AddToScheme(scheme)).To(Succeed())
		Expect(batchv1.AddToScheme(scheme)).To(Succeed())
		Expect(corev1.AddToScheme(scheme)).To(Succeed())

		resource = newTestResource()
		resource.Spec.WorkloadType = v1alpha1.WorkloadTypeCronJob
		resource.Spec.Schedule = "*/5 * * * *"
		resource.Spec.Ports = nil

		reconciler = &Reconciler{
			Client:            mockClient,
			Scheme:            scheme,
			DependencyChecker: mockDepCheck,
			UncachedClient:    mockUncached,
			Status:            testStatusReporter{},
		}

		mockDepCheck.EXPECT().DependenciesAvailable(gomock.Any(), gomock.Any()).
			Return(true, "", nil).AnyTimes()
		mockDepCheck.EXPECT().VolumeMountsReadyForUse(gomock.Any(), gomock.Any()).
			Return(true, "", nil).AnyTimes()
		allowNoStaleWorkloads(mockClient, &batchv1.CronJob{})
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("CronJob create or update", func() {
		It("should create CronJob with correct spec", func() {
			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.CronJob{})).
				Return(apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "cronjobs"}, "test-resource"))

			mockClient.EXPECT().
				Create(gomock.Any(), gomock.AssignableToTypeOf(&batchv1.CronJob{}), gomock.Any()).
				DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
					cj := obj.(*batchv1.CronJob)
					Expect(cj.Spec.Schedule).To(Equal("*/5 * * * *"))
					Expect(cj.Spec.ConcurrencyPolicy).To(Equal(batchv1.ForbidConcurrent))
					Expect(*cj.Spec.SuccessfulJobsHistoryLimit).To(Equal(int32(3)))
					Expect(*cj.Spec.FailedJobsHistoryLimit).To(Equal(int32(1)))
					Expect(*cj.Spec.JobTemplate.Spec.Completions).To(Equal(int32(1)))
					Expect(*cj.Spec.JobTemplate.Spec.Parallelism).To(Equal(int32(1)))
					Expect(cj.Spec.JobTemplate.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyOnFailure))

					// Simulate k8s populating the CronJob after creation
					cj.UID = "cj-uid"
					return nil
				})

			// Mock the child job list call from evaluateCronJobStatus
			setupChildJobList(mockClient, nil)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultContinue))
		})
	})

	Context("CronJob status evaluation", func() {
		It("should report converged and available immediately", func() {
			cj := &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					UID:       "cj-uid",
				},
			}
			setupCronJobCreateOrUpdate(mockClient, cj)
			setupChildJobList(mockClient, nil)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultContinue))

			convergedCond := findCondition(resource.Status.Conditions, string(v1alpha1.StackResourceWorkloadConverged))
			Expect(convergedCond).NotTo(BeNil())
			Expect(convergedCond.Status).To(Equal(metav1.ConditionTrue))

			workloadCond := findCondition(resource.Status.Conditions, string(v1alpha1.StackResourceWorkloadAvailable))
			Expect(workloadCond).NotTo(BeNil())
			Expect(workloadCond.Status).To(Equal(metav1.ConditionTrue))
		})

		It("should set LastRunTime from LastScheduleTime", func() {
			lastSchedule := metav1.NewTime(time.Now().Add(-5 * time.Minute))
			cj := &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					UID:       "cj-uid",
				},
				Status: batchv1.CronJobStatus{
					LastScheduleTime: &lastSchedule,
				},
			}
			setupCronJobCreateOrUpdate(mockClient, cj)
			setupChildJobList(mockClient, nil)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultContinue))
			Expect(resource.Status.LastRunTime).NotTo(BeNil())
			Expect(resource.Status.LastRunTime.Time).To(BeTemporally("~", lastSchedule.Time, time.Second))
		})

		It("should set LastRunSucceeded=true when latest child Job succeeded", func() {
			cj := &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					UID:       "cj-uid",
				},
			}
			setupCronJobCreateOrUpdate(mockClient, cj)

			completionTime := metav1.Now()
			childJob := batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cj-test-12345",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "batch/v1",
						Kind:       "CronJob",
						Name:       "test-resource",
						UID:        "cj-uid",
						Controller: ptr.To(true),
					}},
				},
				Status: batchv1.JobStatus{
					Succeeded:      1,
					CompletionTime: &completionTime,
				},
			}
			setupChildJobList(mockClient, []batchv1.Job{childJob})

			// Pre-set failure details to verify they get cleared
			resource.Status.LastFailureDetails = []v1alpha1.LastFailureDetail{
				{ContainerName: "old-failure"},
			}
			resource.Status.LastFailureDeploymentRevision = "old-key"

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultContinue))
			Expect(resource.Status.LastRunSucceeded).NotTo(BeNil())
			Expect(*resource.Status.LastRunSucceeded).To(BeTrue())
			Expect(resource.Status.LastFailureDetails).To(BeNil())
			Expect(resource.Status.LastFailureDeploymentRevision).To(BeEmpty())
		})

		It("should set LastRunSucceeded=false and capture failures when latest child Job failed", func() {
			cj := &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					UID:       "cj-uid",
				},
			}
			setupCronJobCreateOrUpdate(mockClient, cj)

			completionTime := metav1.Now()
			failedJob := batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cj-test-failed-1",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "batch/v1",
						Kind:       "CronJob",
						Name:       "test-resource",
						UID:        "cj-uid",
						Controller: ptr.To(true),
					}},
				},
				Status: batchv1.JobStatus{
					Succeeded:      0,
					Failed:         1,
					CompletionTime: &completionTime,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
					},
				},
			}
			setupChildJobList(mockClient, []batchv1.Job{failedJob})

			// capturePodFailureDetailsOnce calls capturePodFailureDetails which lists pods via UncachedClient
			setupCrashingPodsForCronJob(mockUncached, 42)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultContinue))
			Expect(resource.Status.LastRunSucceeded).NotTo(BeNil())
			Expect(*resource.Status.LastRunSucceeded).To(BeFalse())
			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].LastTerminationExitCode).NotTo(BeNil())
			Expect(*resource.Status.LastFailureDetails[0].LastTerminationExitCode).To(Equal(int32(42)))
		})

		It("should pick the most recently completed child Job", func() {
			cj := &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					UID:       "cj-uid",
				},
			}
			setupCronJobCreateOrUpdate(mockClient, cj)

			olderTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
			newerTime := metav1.NewTime(time.Now().Add(-1 * time.Minute))

			olderSucceededJob := batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cj-test-older",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "batch/v1",
						Kind:       "CronJob",
						Name:       "test-resource",
						UID:        "cj-uid",
						Controller: ptr.To(true),
					}},
				},
				Status: batchv1.JobStatus{
					Succeeded:      1,
					CompletionTime: &olderTime,
				},
			}

			newerFailedJob := batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cj-test-newer",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "batch/v1",
						Kind:       "CronJob",
						Name:       "test-resource",
						UID:        "cj-uid",
						Controller: ptr.To(true),
					}},
				},
				Status: batchv1.JobStatus{
					Succeeded:      0,
					Failed:         1,
					CompletionTime: &newerTime,
				},
			}

			setupChildJobList(mockClient, []batchv1.Job{olderSucceededJob, newerFailedJob})

			// The newer job failed, so capturePodFailureDetailsOnce will be called
			setupCrashingPodsForCronJob(mockUncached, 1)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultContinue))
			Expect(resource.Status.LastRunSucceeded).NotTo(BeNil())
			Expect(*resource.Status.LastRunSucceeded).To(BeFalse(), "should use the newer failed job")
		})

		It("should ignore child Jobs not owned by the CronJob", func() {
			cj := &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					UID:       "cj-uid",
				},
			}
			setupCronJobCreateOrUpdate(mockClient, cj)

			completionTime := metav1.Now()
			unownedJob := batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "other-cronjob-12345",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "batch/v1",
						Kind:       "CronJob",
						Name:       "other-resource",
						UID:        "other-uid",
						Controller: ptr.To(true),
					}},
				},
				Status: batchv1.JobStatus{
					Succeeded:      1,
					CompletionTime: &completionTime,
				},
			}
			setupChildJobList(mockClient, []batchv1.Job{unownedJob})

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultContinue))
			Expect(resource.Status.LastRunSucceeded).To(BeNil())
		})
	})

	Context("latestCompletedChildJob edge cases", func() {
		It("should return nil when no child Jobs exist", func() {
			cj := &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					UID:       "cj-uid",
				},
			}
			setupCronJobCreateOrUpdate(mockClient, cj)
			setupChildJobList(mockClient, nil)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultContinue))
			Expect(resource.Status.LastRunSucceeded).To(BeNil())
		})

		It("should ignore active child Jobs", func() {
			cj := &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					UID:       "cj-uid",
				},
			}
			setupCronJobCreateOrUpdate(mockClient, cj)

			activeJob := batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cj-test-active",
					Namespace: "test-ns",
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "batch/v1",
						Kind:       "CronJob",
						Name:       "test-resource",
						UID:        "cj-uid",
						Controller: ptr.To(true),
					}},
				},
				Status: batchv1.JobStatus{
					Active:         1,
					CompletionTime: nil, // still running
				},
			}
			setupChildJobList(mockClient, []batchv1.Job{activeJob})

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultContinue))
			Expect(resource.Status.LastRunSucceeded).To(BeNil())
		})
	})
})
