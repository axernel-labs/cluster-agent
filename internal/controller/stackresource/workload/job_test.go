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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"stackdome.io/cluster-agent/api/core/v1alpha1"
	"stackdome.io/cluster-agent/internal/controller"
	"stackdome.io/cluster-agent/internal/controller/mocks"
)

var _ = Describe("jobReconciler", func() {
	var (
		mockCtrl     *gomock.Controller
		mockClient   *mocks.MockClient
		mockUncached *mocks.MockClient
		mockDepCheck *mocks.MockDependencyChecker
		reconciler   *Reconciler
		resource     *v1alpha1.StackResource
		ctx          context.Context
		verdicts     *controller.VerdictCollector
		scheme       *runtime.Scheme
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockClient = mocks.NewMockClient(mockCtrl)
		mockUncached = mocks.NewMockClient(mockCtrl)
		mockDepCheck = mocks.NewMockDependencyChecker(mockCtrl)
		ctx, verdicts = testCtx()

		scheme = runtime.NewScheme()
		Expect(v1alpha1.AddToScheme(scheme)).To(Succeed())
		Expect(batchv1.AddToScheme(scheme)).To(Succeed())
		Expect(corev1.AddToScheme(scheme)).To(Succeed())

		resource = newTestResource()
		resource.Spec.WorkloadType = v1alpha1.WorkloadTypeJob
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
		allowNoStaleWorkloads(mockClient, &batchv1.Job{})
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("Job creation", func() {
		It("should create Job when not found", func() {
			resource.Annotations = map[string]string{
				v1alpha1.RevisionAnnotation: "rev-1",
			}

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.Job{})).
				Return(apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "test-resource"))

			var capturedJob *batchv1.Job
			mockClient.EXPECT().
				Create(gomock.Any(), gomock.AssignableToTypeOf(&batchv1.Job{}), gomock.Any()).
				DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
					capturedJob = obj.(*batchv1.Job)
					return nil
				})

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultRequeueAfter(5 * time.Second)))

			Expect(capturedJob).NotTo(BeNil())
			Expect(*capturedJob.Spec.Completions).To(Equal(int32(1)))
			Expect(*capturedJob.Spec.Parallelism).To(Equal(int32(1)))
			Expect(*capturedJob.Spec.BackoffLimit).To(Equal(int32(6)))
			Expect(capturedJob.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyOnFailure))
			Expect(capturedJob.Annotations[v1alpha1.RevisionAnnotation]).To(Equal("rev-1"))
		})

		It("should set image pull secret on pod template", func() {
			resource.Annotations = map[string]string{
				v1alpha1.RevisionAnnotation: "rev-1",
			}
			resource.Spec.ImageSpec.PullAuth = &v1alpha1.RegistryAuth{
				Type: v1alpha1.RegistryAuthTypeDockerHub,
				DockerConfigAuth: &v1alpha1.DockerConfigAuth{
					SecretKey: ".dockerconfigjson",
					SecretRef: &corev1.SecretReference{
						Name:      "my-pull-secret",
						Namespace: "test-ns",
					},
				},
			}

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.Job{})).
				Return(apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, "test-resource"))

			// Mock the secret lookup for resolveAndSetImagePullSecret
			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "my-pull-secret", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&corev1.Secret{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					s := obj.(*corev1.Secret)
					s.Name = "my-pull-secret"
					s.Namespace = "test-ns"
					return nil
				})

			var capturedJob *batchv1.Job
			mockClient.EXPECT().
				Create(gomock.Any(), gomock.AssignableToTypeOf(&batchv1.Job{}), gomock.Any()).
				DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
					capturedJob = obj.(*batchv1.Job)
					return nil
				})

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultRequeueAfter(5 * time.Second)))
			Expect(capturedJob).NotTo(BeNil())
			Expect(capturedJob.Spec.Template.Spec.ImagePullSecrets).To(HaveLen(1))
			Expect(capturedJob.Spec.Template.Spec.ImagePullSecrets[0].Name).To(Equal("my-pull-secret"))
		})
	})

	Context("Job delete-recreate on revision change", func() {
		It("should delete finished Job when revision mismatches", func() {
			resource.Annotations = map[string]string{
				v1alpha1.RevisionAnnotation: "new-rev",
			}

			completedJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					Annotations: map[string]string{
						v1alpha1.RevisionAnnotation: "old-rev",
					},
				},
				Status: batchv1.JobStatus{
					Succeeded: 1,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
					},
				},
			}

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.Job{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					*obj.(*batchv1.Job) = *completedJob
					return nil
				})

			mockClient.EXPECT().
				Delete(gomock.Any(), gomock.AssignableToTypeOf(&batchv1.Job{}), gomock.Any()).
				Return(nil)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultRequeueAfter(2 * time.Second)))
			// Old job deleted, new one not created yet: without a verdict this
			// window derives as available off the previous run's WorkloadConverged.
			Expect(verdicts.NotReady()).NotTo(BeNil())
		})

		It("should not delete active Job even if revision mismatches", func() {
			resource.Annotations = map[string]string{
				v1alpha1.RevisionAnnotation: "new-rev",
			}

			activeJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					Annotations: map[string]string{
						v1alpha1.RevisionAnnotation: "old-rev",
					},
				},
				Status: batchv1.JobStatus{
					Active: 1,
				},
			}

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.Job{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					*obj.(*batchv1.Job) = *activeJob
					return nil
				})

			// No Delete should be called — gomock will fail if it is.
			// evaluateJobStatus will call capturePodFailureDetailsOnce, which
			// needs failureKey ("new-rev") != LastFailureDeploymentRevision ("").
			setupEmptyUncachedPodList(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultRequeueAfter(10 * time.Second)))
			Expect(verdicts.NotReady()).NotTo(BeNil(), "a not-ready verdict is what derivation turns into Phase=Pending")
		})
	})

	Context("when Job succeeds", func() {
		It("should set LastRunSucceeded=true and clear failure details", func() {
			resource.Annotations = map[string]string{
				v1alpha1.RevisionAnnotation: "rev-1",
			}
			resource.Status.LastFailureDetails = []v1alpha1.LastFailureDetail{
				{ContainerName: "old-failure"},
			}
			resource.Status.LastFailureDeploymentRevision = "old-rev"

			completionTime := metav1.Now()
			succeededJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					Annotations: map[string]string{
						v1alpha1.RevisionAnnotation: "rev-1",
					},
				},
				Status: batchv1.JobStatus{
					Succeeded:      1,
					CompletionTime: &completionTime,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
					},
				},
			}

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.Job{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					*obj.(*batchv1.Job) = *succeededJob
					return nil
				})

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultContinue))
			Expect(resource.Status.LastRunSucceeded).NotTo(BeNil())
			Expect(*resource.Status.LastRunSucceeded).To(BeTrue())
			Expect(resource.Status.LastRunTime).NotTo(BeNil())
			Expect(resource.Status.LastFailureDetails).To(BeNil())
			Expect(resource.Status.LastFailureDeploymentRevision).To(BeEmpty())

			convergedCond := findCondition(resource.Status.Conditions, string(v1alpha1.StackResourceWorkloadConverged))
			Expect(convergedCond).NotTo(BeNil())
			Expect(convergedCond.Status).To(Equal(metav1.ConditionTrue))

			workloadCond := findCondition(resource.Status.Conditions, string(v1alpha1.StackResourceWorkloadAvailable))
			Expect(workloadCond).NotTo(BeNil())
			Expect(workloadCond.Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Context("when Job fails", func() {
		It("should set LastRunSucceeded=false and capture failure details", func() {
			resource.Annotations = map[string]string{
				v1alpha1.RevisionAnnotation: "rev-1",
			}

			failedJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					Annotations: map[string]string{
						v1alpha1.RevisionAnnotation: "rev-1",
					},
				},
				Status: batchv1.JobStatus{
					Failed: 6,
					Conditions: []batchv1.JobCondition{
						{
							Type:    batchv1.JobFailed,
							Status:  corev1.ConditionTrue,
							Message: "BackoffLimitExceeded",
						},
					},
				},
			}

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.Job{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					*obj.(*batchv1.Job) = *failedJob
					return nil
				})

			setupCrashingPodsForSelector(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultStop))
			Expect(resource.Status.LastRunSucceeded).NotTo(BeNil())
			Expect(*resource.Status.LastRunSucceeded).To(BeFalse())
			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].ContainerName).To(Equal("test-resource"))
			Expect(resource.Status.LastFailureDetails[0].LastTerminationExitCode).NotTo(BeNil())
			Expect(*resource.Status.LastFailureDetails[0].LastTerminationExitCode).To(Equal(int32(1)))
			Expect(verdicts.Failed()).NotTo(BeNil(), "a failed verdict is what derivation turns into Phase=Failed")
		})

		It("should handle failed Job when pods already cleaned up", func() {
			resource.Annotations = map[string]string{
				v1alpha1.RevisionAnnotation: "rev-1",
			}

			failedJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					Annotations: map[string]string{
						v1alpha1.RevisionAnnotation: "rev-1",
					},
				},
				Status: batchv1.JobStatus{
					Failed: 6,
					Conditions: []batchv1.JobCondition{
						{
							Type:    batchv1.JobFailed,
							Status:  corev1.ConditionTrue,
							Message: "BackoffLimitExceeded",
						},
					},
				},
			}

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.Job{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					*obj.(*batchv1.Job) = *failedJob
					return nil
				})

			setupEmptyUncachedPodList(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultStop))
			Expect(resource.Status.LastRunSucceeded).NotTo(BeNil())
			Expect(*resource.Status.LastRunSucceeded).To(BeFalse())
			Expect(resource.Status.LastFailureDetails).To(BeNil())
			Expect(verdicts.Failed()).NotTo(BeNil(), "a failed verdict is what derivation turns into Phase=Failed")
		})
	})

	Context("when Job is still running", func() {
		It("should requeue and report not ready", func() {
			resource.Annotations = map[string]string{
				v1alpha1.RevisionAnnotation: "rev-1",
			}

			activeJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					Annotations: map[string]string{
						v1alpha1.RevisionAnnotation: "rev-1",
					},
				},
				Status: batchv1.JobStatus{
					Active: 1,
				},
			}

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.Job{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					*obj.(*batchv1.Job) = *activeJob
					return nil
				})

			setupEmptyUncachedPodList(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultRequeueAfter(10 * time.Second)))
			Expect(verdicts.NotReady()).NotTo(BeNil(), "a not-ready verdict is what derivation turns into Phase=Pending")
		})

		It("should capture intermediate failures while running", func() {
			resource.Annotations = map[string]string{
				v1alpha1.RevisionAnnotation: "rev-1",
			}
			// LastFailureDetails must be empty so the intermediate capture runs
			resource.Status.LastFailureDetails = nil
			// LastFailureDeploymentRevision must NOT equal the revision annotation
			resource.Status.LastFailureDeploymentRevision = ""

			activeJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					Annotations: map[string]string{
						v1alpha1.RevisionAnnotation: "rev-1",
					},
				},
				Status: batchv1.JobStatus{
					Active: 1,
				},
			}

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.Job{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					*obj.(*batchv1.Job) = *activeJob
					return nil
				})

			setupCrashingPodsForSelector(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			// Failure captured while running → stop and rely on the Job watch for the terminal transition.
			Expect(result).To(Equal(controller.ResultStop))
			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].ContainerName).To(Equal("test-resource"))
			Expect(resource.Status.LastFailureDeploymentRevision).To(Equal("rev-1"))
		})

		It("should skip intermediate capture if already captured", func() {
			resource.Annotations = map[string]string{
				v1alpha1.RevisionAnnotation: "rev-1",
			}
			resource.Status.LastFailureDetails = []v1alpha1.LastFailureDetail{
				{ContainerName: "test-resource", RestartCount: 2, LastTerminationReason: "Error"},
			}
			resource.Status.LastFailureDeploymentRevision = "rev-1"

			activeJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "test-ns",
					Annotations: map[string]string{
						v1alpha1.RevisionAnnotation: "rev-1",
					},
				},
				Status: batchv1.JobStatus{
					Active: 1,
				},
			}

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&batchv1.Job{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					*obj.(*batchv1.Job) = *activeJob
					return nil
				})

			// NO uncached client expectations — capture should be skipped entirely

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			// Details already present → stop rather than keep polling.
			Expect(result).To(Equal(controller.ResultStop))
			// Existing failure details should be preserved
			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].ContainerName).To(Equal("test-resource"))
			Expect(resource.Status.LastFailureDetails[0].RestartCount).To(Equal(int32(2)))
		})
	})

	Context("jobFinished helper", func() {
		DescribeTable("detects finished Jobs",
			func(conditions []batchv1.JobCondition, expected bool) {
				job := &batchv1.Job{Status: batchv1.JobStatus{Conditions: conditions}}
				Expect(jobFinished(job)).To(Equal(expected))
			},
			Entry("Complete=True", []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			}, true),
			Entry("Failed=True", []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
			}, true),
			Entry("no conditions", []batchv1.JobCondition{}, false),
			Entry("Complete=False", []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionFalse},
			}, false),
		)
	})
})
