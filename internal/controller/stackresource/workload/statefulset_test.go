package workload

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
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

// setupSTSCreateOrUpdate mocks the Client.Get and Client.Update calls that
// controllerutil.CreateOrUpdate makes internally for a StatefulSet.
func setupSTSCreateOrUpdate(mockClient *mocks.MockClient, sts *appsv1.StatefulSet) {
	mockClient.EXPECT().
		Get(gomock.Any(), client.ObjectKey{Name: sts.Name, Namespace: sts.Namespace}, gomock.AssignableToTypeOf(&appsv1.StatefulSet{})).
		DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
			*obj.(*appsv1.StatefulSet) = *sts
			return nil
		})
	mockClient.EXPECT().
		Update(gomock.Any(), gomock.AssignableToTypeOf(&appsv1.StatefulSet{}), gomock.Any()).
		Return(nil).
		AnyTimes()
}

// setupEmptyUncachedPodList mocks an empty pod list for the selector-scoped
// capturePodFailureDetailsOnce (StatefulSet/Job/CronJob path).
func setupEmptyUncachedPodList(mockUncached *mocks.MockClient) {
	mockUncached.EXPECT().
		List(gomock.Any(), gomock.AssignableToTypeOf(&corev1.PodList{}), gomock.Any()).
		Return(nil)
}

// setupCrashingPodsForSelector mocks a crashing pod found via label selector
// (the StatefulSet failure-capture path, which lists pods directly — not ReplicaSets).
func setupCrashingPodsForSelector(mockUncached *mocks.MockClient) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-resource-0",
			Namespace:         "test-ns",
			CreationTimestamp: metav1.Now(),
			Labels:            map[string]string{"resource": "test-resource"},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "test-resource",
				RestartCount: 3,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 1, Reason: "Error", Message: "connection refused",
					},
				},
			}},
		},
	}
	mockUncached.EXPECT().
		List(gomock.Any(), gomock.AssignableToTypeOf(&corev1.PodList{}), gomock.Any()).
		DoAndReturn(func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
			list.(*corev1.PodList).Items = []corev1.Pod{pod}
			return nil
		})
}

var _ = Describe("statefulSetReconciler", func() {
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
		Expect(appsv1.AddToScheme(scheme)).To(Succeed())
		Expect(corev1.AddToScheme(scheme)).To(Succeed())

		resource = newTestResource()
		resource.Spec.WorkloadType = v1alpha1.WorkloadTypeStatefulService

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
		allowNoStaleWorkloads(mockClient, &appsv1.StatefulSet{})
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("StatefulSet create or update", func() {
		It("should create StatefulSet with replicas=1 ignoring spec.replicas", func() {
			resource.Spec.Replicas = ptr.To(int32(3))

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&appsv1.StatefulSet{})).
				Return(apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, "test-resource"))

			mockClient.EXPECT().
				Create(gomock.Any(), gomock.AssignableToTypeOf(&appsv1.StatefulSet{}), gomock.Any()).
				DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
					sts := obj.(*appsv1.StatefulSet)
					Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
					Expect(sts.Spec.ServiceName).To(Equal("test-resource"))
					Expect(sts.Spec.PodManagementPolicy).To(Equal(appsv1.OrderedReadyPodManagement))
					Expect(sts.Spec.UpdateStrategy.Type).To(Equal(appsv1.RollingUpdateStatefulSetStrategyType))
					Expect(sts.Spec.MinReadySeconds).To(Equal(int32(workloadMinReadySeconds)))
					Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(0))
					Expect(sts.Spec.Selector.MatchLabels).To(Equal(map[string]string{"resource": "test-resource"}))
					Expect(sts.Spec.Template.Spec.Containers).To(HaveLen(1))
					Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal("busybox:latest"))
					Expect(sts.Spec.Template.Spec.Containers[0].Name).To(Equal("test-resource"))

					// Simulate k8s populating status after creation
					sts.Generation = 1
					sts.Status.ObservedGeneration = 1
					sts.Status.CurrentRevision = "rev-1"
					sts.Status.UpdateRevision = "rev-1"
					sts.Status.Replicas = 1
					sts.Status.ReadyReplicas = 1
					sts.Status.UpdatedReplicas = 1
					sts.Status.AvailableReplicas = 1
					return nil
				})

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultNil))
		})

		It("should set image pull secret on pod template spec", func() {
			resource.Spec.ImageSpec.PullAuth = &v1alpha1.RegistryAuth{
				Type: "dockerConfig",
				DockerConfigAuth: &v1alpha1.DockerConfigAuth{
					SecretKey: ".dockerconfigjson",
					SecretRef: &corev1.SecretReference{
						Name:      "my-pull-secret",
						Namespace: "test-ns",
					},
				},
			}

			// Mock Get for the pull secret
			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "my-pull-secret", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&corev1.Secret{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					secret := obj.(*corev1.Secret)
					secret.Name = "my-pull-secret"
					secret.Namespace = "test-ns"
					return nil
				})

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&appsv1.StatefulSet{})).
				Return(apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, "test-resource"))

			mockClient.EXPECT().
				Create(gomock.Any(), gomock.AssignableToTypeOf(&appsv1.StatefulSet{}), gomock.Any()).
				DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
					sts := obj.(*appsv1.StatefulSet)
					Expect(sts.Spec.Template.Spec.ImagePullSecrets).To(HaveLen(1))
					Expect(sts.Spec.Template.Spec.ImagePullSecrets[0].Name).To(Equal("my-pull-secret"))

					// Simulate converged status.
					sts.Generation = 1
					sts.Status.ObservedGeneration = 1
					sts.Status.CurrentRevision = "rev-1"
					sts.Status.UpdateRevision = "rev-1"
					sts.Status.Replicas = 1
					sts.Status.ReadyReplicas = 1
					sts.Status.UpdatedReplicas = 1
					sts.Status.AvailableReplicas = 1
					return nil
				})

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultNil))
		})
	})

	Context("when StatefulSet is converged", func() {
		It("should return ResultContinue and clear failure details", func() {
			resource.Status.LastFailureDetails = []v1alpha1.LastFailureDetail{
				{ContainerName: "old-failure"},
			}
			resource.Status.LastFailureDeploymentRevision = "rev-1"

			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-resource",
					Namespace:  "test-ns",
					Generation: 1,
				},
				Status: appsv1.StatefulSetStatus{
					ObservedGeneration: 1,
					CurrentRevision:    "rev-1",
					UpdateRevision:     "rev-1",
					Replicas:           1,
					ReadyReplicas:      1,
					UpdatedReplicas:    1,
					AvailableReplicas:  1,
				},
			}
			setupSTSCreateOrUpdate(mockClient, sts)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultNil))
			Expect(resource.Status.LastFailureDetails).To(BeNil())
			Expect(resource.Status.LastFailureDeploymentRevision).To(BeEmpty())

			convergedCond := findCondition(resource.Status.Conditions, string(v1alpha1.StackResourceWorkloadConverged))
			Expect(convergedCond).NotTo(BeNil())
			Expect(convergedCond.Status).To(Equal(metav1.ConditionTrue))
		})
	})

	Context("when StatefulSet is not converged", func() {
		It("should requeue when pod not ready", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-resource",
					Namespace:  "test-ns",
					Generation: 2,
				},
				Status: appsv1.StatefulSetStatus{
					ObservedGeneration: 2,
					CurrentRevision:    "rev-1",
					UpdateRevision:     "rev-2",
					Replicas:           1,
					ReadyReplicas:      0,
					UpdatedReplicas:    0,
					AvailableReplicas:  0,
				},
			}
			setupSTSCreateOrUpdate(mockClient, sts)
			setupEmptyUncachedPodList(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.ResultRequeueAfter).NotTo(BeNil())
			Expect(*result.ResultRequeueAfter).To(Equal(10 * time.Second))

			convergedCond := findCondition(resource.Status.Conditions, string(v1alpha1.StackResourceWorkloadConverged))
			Expect(convergedCond).NotTo(BeNil())
			Expect(convergedCond.Status).To(Equal(metav1.ConditionFalse))

			workloadCond := findCondition(resource.Status.Conditions, string(v1alpha1.StackResourceWorkloadAvailable))
			Expect(workloadCond).NotTo(BeNil())
			Expect(workloadCond.Status).To(Equal(metav1.ConditionFalse))

			Expect(verdicts.NotReady()).NotTo(BeNil(), "a not-ready verdict is what derivation turns into Phase=Pending")
		})

		It("should capture failure details from crashing pods", func() {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-resource",
					Namespace:  "test-ns",
					Generation: 2,
				},
				Status: appsv1.StatefulSetStatus{
					ObservedGeneration: 2,
					CurrentRevision:    "rev-1",
					UpdateRevision:     "rev-2",
					Replicas:           1,
					ReadyReplicas:      0,
					UpdatedReplicas:    0,
					AvailableReplicas:  0,
				},
			}
			setupSTSCreateOrUpdate(mockClient, sts)
			setupCrashingPodsForSelector(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			// Failure captured → stop the active requeue loop (rely on the StatefulSet watch).
			Expect(result).To(Equal(controller.ResultStop))
			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].ContainerName).To(Equal("test-resource"))
			Expect(resource.Status.LastFailureDetails[0].LastTerminationReason).To(Equal("Error"))
			Expect(resource.Status.LastFailureDeploymentRevision).To(Equal("rev-2"))
		})

		It("should skip capture when already captured for same revision", func() {
			resource.Status.LastFailureDeploymentRevision = "rev-2"
			resource.Status.LastFailureDetails = []v1alpha1.LastFailureDetail{
				{ContainerName: "test-resource", RestartCount: 3},
			}

			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-resource",
					Namespace:  "test-ns",
					Generation: 2,
				},
				Status: appsv1.StatefulSetStatus{
					ObservedGeneration: 2,
					CurrentRevision:    "rev-1",
					UpdateRevision:     "rev-2",
					Replicas:           1,
					ReadyReplicas:      0,
					UpdatedReplicas:    0,
					AvailableReplicas:  0,
				},
			}
			setupSTSCreateOrUpdate(mockClient, sts)
			// No uncached client expectations for pod listing.

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			// Details already present → stop rather than requeue forever.
			Expect(result).To(Equal(controller.ResultStop))
			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].ContainerName).To(Equal("test-resource"))
		})
	})

	Context("when restart is requested", func() {
		It("should process restart and return ResultStop", func() {
			now := metav1.Now()
			resource.Spec.RestartRequest = &now

			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-resource",
					Namespace:  "test-ns",
					Generation: 1,
				},
				Status: appsv1.StatefulSetStatus{
					ObservedGeneration: 1,
					CurrentRevision:    "rev-1",
					UpdateRevision:     "rev-1",
					Replicas:           1,
					ReadyReplicas:      0,
					UpdatedReplicas:    0,
					AvailableReplicas:  0,
				},
			}
			setupSTSCreateOrUpdate(mockClient, sts)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultStop))
			Expect(resource.Status.LastRestartRequestProcessedAt).NotTo(BeNil())
		})

		It("should not restart when already processed", func() {
			restartTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))
			processedTime := metav1.NewTime(time.Now())
			resource.Spec.RestartRequest = &restartTime
			resource.Status.LastRestartRequestProcessedAt = &processedTime

			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-resource",
					Namespace:  "test-ns",
					Generation: 1,
				},
				Status: appsv1.StatefulSetStatus{
					ObservedGeneration: 1,
					CurrentRevision:    "rev-1",
					UpdateRevision:     "rev-1",
					Replicas:           1,
					ReadyReplicas:      1,
					UpdatedReplicas:    1,
					AvailableReplicas:  1,
				},
			}
			setupSTSCreateOrUpdate(mockClient, sts)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			// Should proceed with normal evaluation (converged) rather than stopping for restart
			Expect(result).NotTo(Equal(controller.ResultStop))
			Expect(result).To(Equal(controller.ResultNil))
		})
	})
})
