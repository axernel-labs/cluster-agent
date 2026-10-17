package workload

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"stackdome.io/cluster-agent/api/core/v1alpha1"
	"stackdome.io/cluster-agent/internal/controller"
	"stackdome.io/cluster-agent/internal/controller/mocks"
)

func TestWorkloadReconciler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Workload Reconciler Suite")
}

// testStatusReporter implements StatusReporter for unit tests exactly as
// production does: verdicts go to the pass collector, domain conditions go on
// the resource. Summary conditions (Available/Stalled/Converged/Phase) are
// written only by deriveSummaryStatus, one package up — so workload tests
// assert the verdict that was reported, not a summary the chain never wrote.
type testStatusReporter struct{}

func (testStatusReporter) ReportNotReady(ctx context.Context, r *v1alpha1.StackResource, reason, msg string) {
	controller.VerdictsFromContext(ctx).ReportNotReady(reason, msg)
}

func (testStatusReporter) ReportFailed(ctx context.Context, r *v1alpha1.StackResource, reason, msg string) {
	controller.VerdictsFromContext(ctx).ReportFailed(reason, msg)
}

func (testStatusReporter) SetCondition(r *v1alpha1.StackResource, condType v1alpha1.StackResourceDomainCondition, ready bool, reason, msg string) {
	condStatus := metav1.ConditionFalse
	if ready {
		condStatus = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&r.Status.Conditions, metav1.Condition{
		Type:               string(condType),
		Status:             condStatus,
		ObservedGeneration: r.Generation,
		Reason:             reason,
		Message:            msg,
	})
}

// testCtx mirrors what Reconcile attaches in production: a fresh verdict
// collector for the pass, so tests can inspect what the chain reported.
func testCtx() (context.Context, *controller.VerdictCollector) {
	return controller.ContextWithVerdicts(context.Background())
}

func newTestResource() *v1alpha1.StackResource {
	return &v1alpha1.StackResource{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-resource",
			Namespace:  "test-ns",
			Generation: 1,
			UID:        "test-uid",
		},
		Spec: v1alpha1.StackResourceSpec{
			ImageSpec: &v1alpha1.ImageSpec{
				Image: "busybox:latest",
			},
			Ports: []v1alpha1.Port{
				{Name: "http", Number: 8080, Protocol: "http"},
			},
		},
	}
}

func deploymentWithConditions(revision string, conditions []appsv1.DeploymentCondition) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-resource",
			Namespace:  "test-ns",
			Generation: 1,
			Annotations: map[string]string{
				deploymentRevisionAnnotation: revision,
			},
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Conditions:         conditions,
		},
	}
}

func availableDeployment(revision string) *appsv1.Deployment {
	deploy := deploymentWithConditions(revision, []appsv1.DeploymentCondition{
		{
			Type:   appsv1.DeploymentAvailable,
			Status: corev1.ConditionTrue,
		},
	})
	deploy.Spec.Replicas = ptr.To(int32(1))
	deploy.Status.Replicas = 1
	deploy.Status.UpdatedReplicas = 1
	deploy.Status.ReadyReplicas = 1
	deploy.Status.AvailableReplicas = 1
	deploy.Status.UnavailableReplicas = 0
	return deploy
}

func notReadyDeployment(revision string) *appsv1.Deployment {
	return deploymentWithConditions(revision, []appsv1.DeploymentCondition{
		{
			Type:   appsv1.DeploymentAvailable,
			Status: corev1.ConditionFalse,
		},
		{
			Type:               appsv1.DeploymentProgressing,
			Status:             corev1.ConditionTrue,
			Reason:             "ReplicaSetUpdated",
			LastTransitionTime: metav1.Now(),
		},
	})
}

func settledDeployment(revision string) *appsv1.Deployment {
	return deploymentWithConditions(revision, []appsv1.DeploymentCondition{
		{
			Type:   appsv1.DeploymentAvailable,
			Status: corev1.ConditionFalse,
		},
		{
			Type:               appsv1.DeploymentProgressing,
			Status:             corev1.ConditionTrue,
			Reason:             "NewReplicaSetAvailable",
			LastTransitionTime: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
	})
}

func progressDeadlineExceededDeployment(revision string) *appsv1.Deployment {
	return deploymentWithConditions(revision, []appsv1.DeploymentCondition{
		{
			Type:   appsv1.DeploymentAvailable,
			Status: corev1.ConditionFalse,
		},
		{
			Type:   appsv1.DeploymentProgressing,
			Status: corev1.ConditionFalse,
			Reason: "ProgressDeadlineExceeded",
		},
	})
}

// setupCreateOrUpdate mocks the Client.Get and Client.Update calls that
// controllerutil.CreateOrUpdate makes internally. Get returns the provided
// deployment (with desired status/annotations). Update is called only if the
// mutate function changes the spec — we allow it with AnyTimes since
// CreateOrUpdate skips it when nothing changed.
func setupCreateOrUpdate(mockClient *mocks.MockClient, deploy *appsv1.Deployment) {
	mockClient.EXPECT().
		Get(gomock.Any(), client.ObjectKey{Name: deploy.Name, Namespace: deploy.Namespace}, gomock.AssignableToTypeOf(&appsv1.Deployment{})).
		DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
			*obj.(*appsv1.Deployment) = *deploy
			return nil
		})
	mockClient.EXPECT().
		Update(gomock.Any(), gomock.AssignableToTypeOf(&appsv1.Deployment{}), gomock.Any()).
		Return(nil).
		AnyTimes()
}

func setupEmptyUncachedList(mockUncached *mocks.MockClient) {
	mockUncached.EXPECT().
		List(gomock.Any(), gomock.AssignableToTypeOf(&appsv1.ReplicaSetList{}), gomock.Any(), gomock.Any()).
		Return(nil)
}

func setupImagePullBackOffPods(mockUncached *mocks.MockClient, revision string) {
	rs := appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-resource-def456",
			Namespace: "test-ns",
			Annotations: map[string]string{
				deploymentRevisionAnnotation: revision,
			},
			Labels: map[string]string{
				"pod-template-hash": "def456",
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"resource": "test-resource",
				},
			},
		},
	}

	mockUncached.EXPECT().
		List(gomock.Any(), gomock.AssignableToTypeOf(&appsv1.ReplicaSetList{}), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
			list.(*appsv1.ReplicaSetList).Items = []appsv1.ReplicaSet{rs}
			return nil
		})

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-resource-def456-xyz",
			Namespace:         "test-ns",
			CreationTimestamp: metav1.Now(),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:         "test-resource",
					RestartCount: 0,
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image",
						},
					},
				},
			},
		},
	}

	mockUncached.EXPECT().
		List(gomock.Any(), gomock.AssignableToTypeOf(&corev1.PodList{}), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
			list.(*corev1.PodList).Items = []corev1.Pod{pod}
			return nil
		})
}

func setupCrashingPods(mockUncached *mocks.MockClient, revision string) {
	rs := appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-resource-abc123",
			Namespace: "test-ns",
			Annotations: map[string]string{
				deploymentRevisionAnnotation: revision,
			},
			Labels: map[string]string{
				"pod-template-hash": "abc123",
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"resource": "test-resource",
				},
			},
		},
	}

	mockUncached.EXPECT().
		List(gomock.Any(), gomock.AssignableToTypeOf(&appsv1.ReplicaSetList{}), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
			list.(*appsv1.ReplicaSetList).Items = []appsv1.ReplicaSet{rs}
			return nil
		})

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-resource-abc123-xyz",
			Namespace:         "test-ns",
			CreationTimestamp: metav1.Now(),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:         "test-resource",
					RestartCount: 3,
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason: "CrashLoopBackOff",
						},
					},
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 1,
							Reason:   "Error",
							Message:  "connection refused",
						},
					},
				},
			},
		},
	}

	mockUncached.EXPECT().
		List(gomock.Any(), gomock.AssignableToTypeOf(&corev1.PodList{}), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
			list.(*corev1.PodList).Items = []corev1.Pod{pod}
			return nil
		})
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

var _ = Describe("workloadReconciler", func() {
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
		allowNoStaleWorkloads(mockClient, &appsv1.Deployment{})
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("when dependencies are not ready", func() {
		BeforeEach(func() {
			// Override the default AnyTimes expectation by creating new mocks
			mockCtrl.Finish()
			mockCtrl = gomock.NewController(GinkgoT())
			mockClient = mocks.NewMockClient(mockCtrl)
			mockUncached = mocks.NewMockClient(mockCtrl)
			mockDepCheck = mocks.NewMockDependencyChecker(mockCtrl)
			reconciler.Client = mockClient
			reconciler.UncachedClient = mockUncached
			reconciler.DependencyChecker = mockDepCheck
		})

		It("should requeue when dependencies are not available", func() {
			mockDepCheck.EXPECT().DependenciesAvailable(gomock.Any(), gomock.Any()).
				Return(false, "waiting for db", nil)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.ResultRequeueAfter).NotTo(BeNil())
			Expect(*result.ResultRequeueAfter).To(Equal(DefaultRequeueTime))
			Expect(verdicts.NotReady()).NotTo(BeNil(), "a not-ready verdict is what derivation turns into Phase=Pending")
		})

		It("should requeue when volume mounts are not ready", func() {
			mockDepCheck.EXPECT().DependenciesAvailable(gomock.Any(), gomock.Any()).
				Return(true, "", nil)
			mockDepCheck.EXPECT().VolumeMountsReadyForUse(gomock.Any(), gomock.Any()).
				Return(false, "volume not synced", nil)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.ResultRequeueAfter).NotTo(BeNil())
			Expect(*result.ResultRequeueAfter).To(Equal(DefaultRequeueTime))
			Expect(verdicts.NotReady()).NotTo(BeNil(), "a not-ready verdict is what derivation turns into Phase=Pending")
		})
	})

	Context("deployment create or update", func() {
		It("should create deployment when it does not exist", func() {

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&appsv1.Deployment{})).
				Return(apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "deployments"}, "test-resource"))

			mockClient.EXPECT().
				Create(gomock.Any(), gomock.AssignableToTypeOf(&appsv1.Deployment{}), gomock.Any()).
				DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.CreateOption) error {
					d := obj.(*appsv1.Deployment)
					Expect(d.Spec.Template.Spec.Containers).To(HaveLen(1))
					Expect(d.Spec.Template.Spec.Containers[0].Image).To(Equal("busybox:latest"))
					Expect(d.Spec.Template.Spec.Containers[0].Name).To(Equal("test-resource"))
					Expect(*d.Spec.ProgressDeadlineSeconds).To(Equal(int32(300)))
					Expect(d.Spec.MinReadySeconds).To(Equal(int32(workloadMinReadySeconds)))
					// Simulate k8s populating the deployment after creation
					d.Generation = 1
					d.Annotations = map[string]string{deploymentRevisionAnnotation: "1"}
					d.Status.ObservedGeneration = 1
					d.Status.Replicas = 1
					d.Status.UpdatedReplicas = 1
					d.Status.ReadyReplicas = 1
					d.Status.AvailableReplicas = 1
					d.Status.Conditions = []appsv1.DeploymentCondition{
						{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue},
					}
					return nil
				})

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultNil))
		})

		It("should not call update when deployment spec is unchanged", func() {
			// Build the deployment that mutate would produce, so DeepEqual passes.
			existing := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-resource",
					Namespace:  "test-ns",
					Generation: 1,
					Annotations: map[string]string{
						deploymentRevisionAnnotation: "1",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(int32(1)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"resource": "test-resource"},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: &appsv1.RollingUpdateDeployment{
							MaxUnavailable: ptr.To(intstr.FromInt32(0)),
							MaxSurge:       ptr.To(intstr.FromString("25%")),
						},
					},
					ProgressDeadlineSeconds: ptr.To(int32(300)),
					MinReadySeconds:         workloadMinReadySeconds,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"resource": "test-resource"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:                     "test-resource",
									Image:                    "busybox:latest",
									ImagePullPolicy:          corev1.PullAlways,
									TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
									Ports:                    []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
								},
							},
						},
					},
				},
				Status: appsv1.DeploymentStatus{
					ObservedGeneration:  1,
					Replicas:            1,
					UpdatedReplicas:     1,
					ReadyReplicas:       1,
					AvailableReplicas:   1,
					UnavailableReplicas: 0,
					Conditions: []appsv1.DeploymentCondition{
						{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue},
					},
				},
			}
			// Set owner reference to match what SetControllerReference will produce.
			Expect(controllerutil.SetControllerReference(resource, existing, scheme)).To(Succeed())

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&appsv1.Deployment{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					*obj.(*appsv1.Deployment) = *existing
					return nil
				})
			// Update should NOT be called — the deployment is already up to date.
			// gomock will fail if Update is called unexpectedly.

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultNil))
		})

		It("should call update when image changes", func() {
			existing := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "test-resource",
					Namespace:  "test-ns",
					Generation: 1,
					Annotations: map[string]string{
						deploymentRevisionAnnotation: "1",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To(int32(1)),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"resource": "test-resource"},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: &appsv1.RollingUpdateDeployment{
							MaxUnavailable: ptr.To(intstr.FromInt32(0)),
							MaxSurge:       ptr.To(intstr.FromString("25%")),
						},
					},
					ProgressDeadlineSeconds: ptr.To(int32(300)),
					MinReadySeconds:         workloadMinReadySeconds,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"resource": "test-resource"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:                     "test-resource",
									Image:                    "old-image:v1",
									ImagePullPolicy:          corev1.PullAlways,
									TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
									Ports:                    []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
								},
							},
						},
					},
				},
				Status: appsv1.DeploymentStatus{
					ObservedGeneration:  1,
					Replicas:            1,
					UpdatedReplicas:     1,
					ReadyReplicas:       1,
					AvailableReplicas:   1,
					UnavailableReplicas: 0,
					Conditions: []appsv1.DeploymentCondition{
						{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue},
					},
				},
			}
			Expect(controllerutil.SetControllerReference(resource, existing, scheme)).To(Succeed())

			mockClient.EXPECT().
				Get(gomock.Any(), client.ObjectKey{Name: "test-resource", Namespace: "test-ns"}, gomock.AssignableToTypeOf(&appsv1.Deployment{})).
				DoAndReturn(func(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
					*obj.(*appsv1.Deployment) = *existing
					return nil
				})
			mockClient.EXPECT().
				Update(gomock.Any(), gomock.AssignableToTypeOf(&appsv1.Deployment{}), gomock.Any()).
				DoAndReturn(func(_ context.Context, obj client.Object, _ ...client.UpdateOption) error {
					d := obj.(*appsv1.Deployment)
					Expect(d.Spec.Template.Spec.Containers[0].Image).To(Equal("busybox:latest"))
					return nil
				})

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultNil))
		})
	})

	Context("when deployment is available", func() {
		It("should return nil result and clear failure details", func() {
			resource.Status.LastFailureDetails = []v1alpha1.LastFailureDetail{
				{ContainerName: "old-failure"},
			}

			setupCreateOrUpdate(mockClient, availableDeployment("1"))

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultNil))
			Expect(resource.Status.LastFailureDetails).To(BeNil())
		})

		It("should also clear LastFailureDeploymentRevision so re-capture is possible if deployment degrades again", func() {
			// Bug: previously only LastFailureDetails was cleared but not LastFailureDeploymentRevision.
			// That left the revision tag set, which caused the capture block to be permanently
			// skipped on subsequent reconciles — so crash details were never repopulated after
			// the pod's brief Running phase between crash loops.
			resource.Status.LastFailureDeploymentRevision = "1"
			resource.Status.LastFailureDetails = []v1alpha1.LastFailureDetail{
				{ContainerName: "old-failure"},
			}

			setupCreateOrUpdate(mockClient, availableDeployment("1"))

			_, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(resource.Status.LastFailureDetails).To(BeNil())
			Expect(resource.Status.LastFailureDeploymentRevision).To(BeEmpty())
		})
	})

	Context("when deployment is not ready", func() {
		It("should requeue and attempt to capture failures for new revision", func() {
			setupCreateOrUpdate(mockClient, notReadyDeployment("1"))
			setupEmptyUncachedList(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.ResultRequeueAfter).NotTo(BeNil())
			Expect(*result.ResultRequeueAfter).To(Equal(10 * time.Second))
			Expect(verdicts.NotReady()).NotTo(BeNil(), "a not-ready verdict is what derivation turns into Phase=Pending")
		})

		It("should capture crash details when pods are crashing", func() {
			setupCreateOrUpdate(mockClient, notReadyDeployment("1"))
			setupCrashingPods(mockUncached, "1")

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.ResultRequeueAfter).NotTo(BeNil())
			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].ContainerName).To(Equal("test-resource"))
			Expect(resource.Status.LastFailureDetails[0].LastTerminationReason).To(Equal("Error"))
			Expect(resource.Status.LastFailureDeploymentRevision).To(Equal("1"))
		})

		It("should skip capture when already captured for this revision", func() {
			resource.Status.LastFailureDeploymentRevision = "1"
			resource.Status.LastFailureDetails = []v1alpha1.LastFailureDetail{
				{ContainerName: "test-resource", RestartCount: 3},
			}

			setupCreateOrUpdate(mockClient, notReadyDeployment("1"))
			// No uncached client expectations — capture should be skipped

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.ResultRequeueAfter).NotTo(BeNil())
			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
		})

		It("should clear stale failures and re-capture for new revision", func() {
			resource.Status.LastFailureDeploymentRevision = "1"
			resource.Status.LastFailureDetails = []v1alpha1.LastFailureDetail{
				{ContainerName: "old-crash"},
			}

			setupCreateOrUpdate(mockClient, notReadyDeployment("2"))
			setupCrashingPods(mockUncached, "2")

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.ResultRequeueAfter).NotTo(BeNil())
			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].ContainerName).To(Equal("test-resource"))
			Expect(resource.Status.LastFailureDeploymentRevision).To(Equal("2"))
		})
	})

	Context("when deployment rollout is settled", func() {
		It("should stop requeuing after NewReplicaSetAvailable grace period", func() {
			setupCreateOrUpdate(mockClient, settledDeployment("1"))
			setupEmptyUncachedList(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultStop))
		})

		It("should stop on ProgressDeadlineExceeded", func() {
			setupCreateOrUpdate(mockClient, progressDeadlineExceededDeployment("1"))
			setupEmptyUncachedList(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultStop))
		})

		It("should keep generic reason when no old replicas are serving", func() {
			deploy := settledDeployment("2")
			deploy.Status.Replicas = 1
			deploy.Status.AvailableReplicas = 0
			deploy.Status.UpdatedReplicas = 1

			setupCreateOrUpdate(mockClient, deploy)
			setupEmptyUncachedList(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultStop))

			Expect(verdicts.NotReady()).NotTo(BeNil())
			Expect(verdicts.NotReady().Reason).To(Equal("StackResourceDeploymentNotReady"))
		})
	})

	Context("when deployment is serving but not converged (broken rollout with old pods alive)", func() {
		It("should pass through to svc reconciler and set Converged=False", func() {
			// Old pod available, new pod crashing: Available=True on deployment
			// but Replicas(2) != UpdatedReplicas(1). deploymentServing returns
			// true, so workload reconciler yields to the svc reconciler.
			deploy := availableDeployment("2")
			deploy.Status.Replicas = 2
			deploy.Status.UpdatedReplicas = 1
			deploy.Status.ReadyReplicas = 1
			deploy.Status.AvailableReplicas = 1
			deploy.Status.UnavailableReplicas = 1
			deploy.Spec.Replicas = ptr.To(int32(1))

			setupCreateOrUpdate(mockClient, deploy)
			setupCrashingPods(mockUncached, "2")

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultNil), "should pass through to svc reconciler")

			workloadCond := findCondition(resource.Status.Conditions, string(v1alpha1.StackResourceWorkloadAvailable))
			Expect(workloadCond).NotTo(BeNil())
			Expect(workloadCond.Status).To(Equal(metav1.ConditionTrue))

			convergedCond := findCondition(resource.Status.Conditions, string(v1alpha1.StackResourceWorkloadConverged))
			Expect(convergedCond).NotTo(BeNil())
			Expect(convergedCond.Status).To(Equal(metav1.ConditionFalse), "not converged: old RS pods still present")
			Expect(convergedCond.Message).To(ContainSubstring("rollout not converged"))
			Expect(convergedCond.Message).To(ContainSubstring("unavailable"))

			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].ContainerName).To(Equal("test-resource"))
			Expect(resource.Status.LastFailureDetails[0].LastTerminationReason).To(Equal("Error"))
		})

		It("should capture ImagePullBackOff failure details and include ProgressDeadlineExceeded in convergence message", func() {
			deploy := deploymentWithConditions("2", []appsv1.DeploymentCondition{
				{
					Type:   appsv1.DeploymentAvailable,
					Status: corev1.ConditionTrue,
				},
				{
					Type:   appsv1.DeploymentProgressing,
					Status: corev1.ConditionFalse,
					Reason: "ProgressDeadlineExceeded",
				},
			})
			deploy.Spec.Replicas = ptr.To(int32(1))
			deploy.Status.Replicas = 2
			deploy.Status.UpdatedReplicas = 1
			deploy.Status.ReadyReplicas = 1
			deploy.Status.AvailableReplicas = 1
			deploy.Status.UnavailableReplicas = 1

			setupCreateOrUpdate(mockClient, deploy)
			setupImagePullBackOffPods(mockUncached, "2")

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultNil))

			convergedCond := findCondition(resource.Status.Conditions, string(v1alpha1.StackResourceWorkloadConverged))
			Expect(convergedCond).NotTo(BeNil())
			Expect(convergedCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(convergedCond.Message).To(ContainSubstring("1/1 updated"))
			Expect(convergedCond.Message).To(ContainSubstring("1 unavailable"))
			Expect(convergedCond.Message).To(ContainSubstring("ProgressDeadlineExceeded"))

			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].ContainerName).To(Equal("test-resource"))
			Expect(resource.Status.LastFailureDetails[0].LastTerminationReason).To(Equal("ImagePullBackOff"))
		})
	})

	Context("when restart is requested", func() {
		It("should process restart and return resultStop", func() {
			now := metav1.Now()
			resource.Spec.RestartRequest = &now

			setupCreateOrUpdate(mockClient, notReadyDeployment("1"))

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

			deploy := availableDeployment("1")
			setupCreateOrUpdate(mockClient, deploy)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultNil))
		})
	})

	Context("when new revision after settled", func() {
		It("should re-enter the capture loop for new revision", func() {
			resource.Status.LastFailureDeploymentRevision = "1"
			resource.Status.LastFailureDetails = []v1alpha1.LastFailureDetail{
				{ContainerName: "old-crash"},
			}

			setupCreateOrUpdate(mockClient, notReadyDeployment("2"))
			setupCrashingPods(mockUncached, "2")

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.ResultRequeueAfter).NotTo(BeNil())
			Expect(resource.Status.LastFailureDeploymentRevision).To(Equal("2"))
			Expect(resource.Status.LastFailureDetails[0].ContainerName).To(Equal("test-resource"))
		})
	})

	Context("grace period after NewReplicaSetAvailable", func() {
		It("should keep requeuing if within grace period", func() {
			deploy := deploymentWithConditions("1", []appsv1.DeploymentCondition{
				{
					Type:   appsv1.DeploymentAvailable,
					Status: corev1.ConditionFalse,
				},
				{
					Type:               appsv1.DeploymentProgressing,
					Status:             corev1.ConditionTrue,
					Reason:             "NewReplicaSetAvailable",
					LastTransitionTime: metav1.Now(), // just now — within grace period
				},
			})

			setupCreateOrUpdate(mockClient, deploy)
			setupEmptyUncachedList(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.ResultRequeueAfter).NotTo(BeNil())
			Expect(*result.ResultRequeueAfter).To(Equal(10 * time.Second))
		})

		It("should stop after grace period expires", func() {
			deploy := deploymentWithConditions("1", []appsv1.DeploymentCondition{
				{
					Type:   appsv1.DeploymentAvailable,
					Status: corev1.ConditionFalse,
				},
				{
					Type:               appsv1.DeploymentProgressing,
					Status:             corev1.ConditionTrue,
					Reason:             "NewReplicaSetAvailable",
					LastTransitionTime: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
				},
			})

			setupCreateOrUpdate(mockClient, deploy)
			setupEmptyUncachedList(mockUncached)

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(controller.ResultStop))
		})
	})

	Context("resolveImagePullPolicy", func() {
		DescribeTable("returns the correct pull policy",
			func(imageSpec *v1alpha1.ImageSpec, image string, expected corev1.PullPolicy) {
				r := &v1alpha1.StackResource{
					Spec: v1alpha1.StackResourceSpec{ImageSpec: imageSpec},
				}
				Expect(resolveImagePullPolicy(r, image)).To(Equal(expected))
			},
			Entry("explicit Always overrides tag",
				&v1alpha1.ImageSpec{Image: "app:v1.0", ImagePullPolicy: corev1.PullAlways},
				"app:v1.0", corev1.PullAlways),
			Entry("explicit Never overrides latest",
				&v1alpha1.ImageSpec{Image: "app:latest", ImagePullPolicy: corev1.PullNever},
				"app:latest", corev1.PullNever),
			Entry("latest tag defaults to Always",
				&v1alpha1.ImageSpec{Image: "app:latest"},
				"app:latest", corev1.PullAlways),
			Entry("no tag defaults to Always",
				&v1alpha1.ImageSpec{Image: "nginx"},
				"nginx", corev1.PullAlways),
			Entry("specific tag defaults to IfNotPresent",
				&v1alpha1.ImageSpec{Image: "app:v1.2.3"},
				"app:v1.2.3", corev1.PullIfNotPresent),
			Entry("sha digest defaults to IfNotPresent",
				&v1alpha1.ImageSpec{Image: "app:sha-abc123"},
				"app:sha-abc123", corev1.PullIfNotPresent),
			Entry("nil imageSpec (build spec) defaults to IfNotPresent for tagged",
				nil,
				"registry.local/app:build-abc", corev1.PullIfNotPresent),
			Entry("nil imageSpec with no tag defaults to Always",
				nil,
				"registry.local/app", corev1.PullAlways),
		)
	})

	Context("sanitization of failure messages", func() {
		It("should strip ANSI escape codes from termination messages", func() {
			rs := appsv1.ReplicaSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource-abc123",
					Namespace: "test-ns",
					Annotations: map[string]string{
						deploymentRevisionAnnotation: "1",
					},
					Labels: map[string]string{
						"pod-template-hash": "abc123",
					},
				},
				Spec: appsv1.ReplicaSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"resource": "test-resource"},
					},
				},
			}

			mockUncached.EXPECT().
				List(gomock.Any(), gomock.AssignableToTypeOf(&appsv1.ReplicaSetList{}), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
					list.(*appsv1.ReplicaSetList).Items = []appsv1.ReplicaSet{rs}
					return nil
				})

			pod := corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-pod",
					Namespace:         "test-ns",
					CreationTimestamp: metav1.Now(),
				},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name:         "test-resource",
							RestartCount: 1,
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason: "CrashLoopBackOff",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									ExitCode: 1,
									Reason:   "Error",
									Message:  "\x1b[36mINFO\x1b[0m error building image\rfailed",
								},
							},
						},
					},
				},
			}

			mockUncached.EXPECT().
				List(gomock.Any(), gomock.AssignableToTypeOf(&corev1.PodList{}), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
					list.(*corev1.PodList).Items = []corev1.Pod{pod}
					return nil
				})

			setupCreateOrUpdate(mockClient, notReadyDeployment("1"))

			result, err := reconciler.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.ResultRequeueAfter).NotTo(BeNil())
			Expect(resource.Status.LastFailureDetails).To(HaveLen(1))
			Expect(resource.Status.LastFailureDetails[0].LastTerminationMessage).To(Equal("INFO error building imagefailed"))
		})
	})
})
