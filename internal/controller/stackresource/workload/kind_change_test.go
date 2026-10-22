package workload

import (
	"context"
	"reflect"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"stackdome.io/cluster-agent/api/core/v1alpha1"
	"stackdome.io/cluster-agent/internal/controller/mocks"
)

// allowNoStaleWorkloads answers the stale-workload lookups with NotFound for
// suites that mock the client. The suite's own kind is skipped: gomock picks
// the first matching expectation, so a catch-all would shadow the suite's Gets.
func allowNoStaleWorkloads(mockClient *mocks.MockClient, current client.Object) {
	for _, kind := range workloadKinds() {
		if reflect.TypeOf(kind) == reflect.TypeOf(current) {
			continue
		}
		mockClient.EXPECT().
			Get(gomock.Any(), gomock.Any(), gomock.AssignableToTypeOf(kind)).
			Return(apierrors.NewNotFound(schema.GroupResource{}, "")).
			AnyTimes()
	}
}

var _ = Describe("changing a StackResource's workload type", func() {
	var (
		mockCtrl *gomock.Controller
		scheme   *runtime.Scheme
		resource *v1alpha1.StackResource
		ctx      context.Context
		key      client.ObjectKey
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		ctx, _ = testCtx()

		scheme = runtime.NewScheme()
		Expect(v1alpha1.AddToScheme(scheme)).To(Succeed())
		Expect(appsv1.AddToScheme(scheme)).To(Succeed())
		Expect(batchv1.AddToScheme(scheme)).To(Succeed())
		Expect(corev1.AddToScheme(scheme)).To(Succeed())

		resource = newTestResource()
		// Only the CronJob type reads it.
		resource.Spec.Schedule = "*/15 * * * *"
		key = client.ObjectKey{Name: resource.Name, Namespace: resource.Namespace}
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	// reconcilerOver runs against a fake API server already holding `existing`.
	reconcilerOver := func(existing ...client.Object) (*Reconciler, client.Client) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing...).Build()
		depCheck := mocks.NewMockDependencyChecker(mockCtrl)
		depCheck.EXPECT().DependenciesAvailable(gomock.Any(), gomock.Any()).Return(true, "", nil).AnyTimes()
		depCheck.EXPECT().VolumeMountsReadyForUse(gomock.Any(), gomock.Any()).Return(true, "", nil).AnyTimes()
		return &Reconciler{
			Client:            c,
			Scheme:            scheme,
			DependencyChecker: depCheck,
			UncachedClient:    fake.NewClientBuilder().WithScheme(scheme).Build(),
			Status:            testStatusReporter{},
		}, c
	}

	// previousWorkload is what the resource ran as before its type changed.
	previousWorkload := func(obj client.Object) client.Object {
		obj = obj.DeepCopyObject().(client.Object)
		obj.SetName(resource.Name)
		obj.SetNamespace(resource.Namespace)
		Expect(controllerutil.SetControllerReference(resource, obj, scheme)).To(Succeed())
		return obj
	}

	expectGone := func(c client.Client, obj client.Object) {
		err := c.Get(ctx, key, obj.DeepCopyObject().(client.Object))
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected %T to be deleted, got err=%v", obj, err)
	}

	DescribeTable("retires the previous workload once the new one exists",
		func(previous client.Object, workloadType v1alpha1.WorkloadType, current client.Object) {
			r, c := reconcilerOver(previousWorkload(previous))
			resource.Spec.WorkloadType = workloadType

			_, err := r.Reconcile(ctx, resource)

			Expect(err).NotTo(HaveOccurred())
			Expect(c.Get(ctx, key, current)).To(Succeed())
			expectGone(c, previous)
		},
		Entry("Service → CronJob", &appsv1.Deployment{}, v1alpha1.WorkloadTypeCronJob, &batchv1.CronJob{}),
		Entry("CronJob → Service", &batchv1.CronJob{}, v1alpha1.WorkloadTypeService, &appsv1.Deployment{}),
		Entry("Service → StatefulService", &appsv1.Deployment{}, v1alpha1.WorkloadTypeStatefulService, &appsv1.StatefulSet{}),
		Entry("Service → Job", &appsv1.Deployment{}, v1alpha1.WorkloadTypeJob, &batchv1.Job{}),
	)

	// The state an agent upgrade finds on clusters that switched before this fix.
	It("cleans up a previous workload already running next to the current one", func() {
		r, c := reconcilerOver(previousWorkload(&appsv1.Deployment{}), previousWorkload(&batchv1.CronJob{}))
		resource.Spec.WorkloadType = v1alpha1.WorkloadTypeCronJob

		_, err := r.Reconcile(ctx, resource)

		Expect(err).NotTo(HaveOccurred())
		Expect(c.Get(ctx, key, &batchv1.CronJob{})).To(Succeed())
		expectGone(c, &appsv1.Deployment{})
	})

	It("leaves a same-named workload it does not control", func() {
		foreign := &appsv1.Deployment{}
		foreign.SetName(resource.Name)
		foreign.SetNamespace(resource.Namespace)
		r, c := reconcilerOver(foreign)
		resource.Spec.WorkloadType = v1alpha1.WorkloadTypeCronJob

		_, err := r.Reconcile(ctx, resource)

		Expect(err).NotTo(HaveOccurred())
		Expect(c.Get(ctx, key, &appsv1.Deployment{})).To(Succeed())
	})

	It("keeps the previous workload when the new one is never created", func() {
		r, c := reconcilerOver(previousWorkload(&appsv1.Deployment{}))
		resource.Spec.WorkloadType = v1alpha1.WorkloadTypeStatefulService
		// A probe on an undeclared port is an invalid spec: no StatefulSet is applied.
		resource.Spec.HealthChecks = &v1alpha1.HealthChecks{
			Readiness: &v1alpha1.Probe{TCPSocket: &v1alpha1.TCPSocketProbe{PortName: "missing"}},
		}

		_, err := r.Reconcile(ctx, resource)

		Expect(err).NotTo(HaveOccurred())
		Expect(apierrors.IsNotFound(c.Get(ctx, key, &appsv1.StatefulSet{}))).To(BeTrue())
		Expect(c.Get(ctx, key, &appsv1.Deployment{})).To(Succeed())
	})
})
