package integration

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "stackdome.io/cluster-agent/api/core/v1alpha1"
	"stackdome.io/cluster-agent/test/integration/fixtures"
	"stackdome.io/cluster-agent/test/integration/helpers"
)

var _ = Describe("Stack mutations and deletion", func() {

	Context("Restart mechanism", Ordered, func() {
		var stack *corev1alpha1.Stack

		BeforeAll(func() {
			swr := fixtures.StackForRestart("restart-stack")
			Expect(fixtures.CreateStackWithResources(ctx, c, swr)).To(Succeed())
			stack = swr.Stack
			_, err := helpers.WaitForStackReady(ctx, c, client.ObjectKeyFromObject(stack), readyTimeout)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should restart the Deployment when RestartRequest is set", func() {
			srName := stack.Spec.ResourceNames[0]

			By("Verifying LastRestartRequestProcessedAt is nil before restart")
			sr := &corev1alpha1.StackResource{}
			Expect(c.Get(ctx, client.ObjectKey{Name: srName, Namespace: stack.Namespace}, sr)).To(Succeed())
			Expect(sr.Status.LastRestartRequestProcessedAt).To(BeNil())

			By("Setting RestartRequest on the StackResource")
			restartRequestedAt := metav1.Now()
			sr.Spec.RestartRequest = &restartRequestedAt
			Expect(c.Update(ctx, sr)).To(Succeed())

			By("Waiting for LastRestartRequestProcessedAt to be set")
			Eventually(func() bool {
				sr, err := helpers.GetStackResource(ctx, c, client.ObjectKey{
					Name:      srName,
					Namespace: stack.Namespace,
				})
				if err != nil {
					return false
				}
				return sr.Status.LastRestartRequestProcessedAt != nil
			}, readyTimeout, 5*time.Second).Should(BeTrue())
		})

		AfterAll(func() { helpers.CleanupStack(ctx, c, stack) })
	})

	Context("Spec mutation - image update", Ordered, func() {
		var stack *corev1alpha1.Stack

		BeforeAll(func() {
			swr := fixtures.StackForMutation("mut-image-stack")
			Expect(fixtures.CreateStackWithResources(ctx, c, swr)).To(Succeed())
			stack = swr.Stack
			_, err := helpers.WaitForStackReady(ctx, c, client.ObjectKeyFromObject(stack), readyTimeout)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should update the Deployment image when the StackResource image is changed", func() {
			newImage := "nginx:1.24-alpine"
			srName := stack.Spec.ResourceNames[0]

			By("Updating the image on the StackResource")
			sr := &corev1alpha1.StackResource{}
			Expect(c.Get(ctx, client.ObjectKey{Name: srName, Namespace: stack.Namespace}, sr)).To(Succeed())
			sr.Spec.ImageSpec.Image = newImage
			Expect(c.Update(ctx, sr)).To(Succeed())

			By("Waiting for Deployment to use the new image")
			_, err := helpers.WaitForDeploymentUpdated(ctx, c, stack.Namespace, srName, func(d *appsv1.Deployment) bool {
				if len(d.Spec.Template.Spec.Containers) == 0 {
					return false
				}
				return d.Spec.Template.Spec.Containers[0].Image == newImage
			}, readyTimeout)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterAll(func() { helpers.CleanupStack(ctx, c, stack) })
	})

	Context("Spec mutation - workload type change", Ordered, func() {
		var stack *corev1alpha1.Stack

		BeforeAll(func() {
			swr := fixtures.StackForMutation("mut-type-stack")
			Expect(fixtures.CreateStackWithResources(ctx, c, swr)).To(Succeed())
			stack = swr.Stack
			_, err := helpers.WaitForStackReady(ctx, c, client.ObjectKeyFromObject(stack), readyTimeout)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should retire the Deployment and its pods when a Service becomes a CronJob", func() {
			srName := stack.Spec.ResourceNames[0]
			key := client.ObjectKey{Name: srName, Namespace: stack.Namespace}

			By("Switching the StackResource to a CronJob")
			sr := &corev1alpha1.StackResource{}
			Expect(c.Get(ctx, key, sr)).To(Succeed())
			sr.Spec.WorkloadType = corev1alpha1.WorkloadTypeCronJob
			sr.Spec.Schedule = "*/15 * * * *"
			// CronJobs cannot declare ports.
			sr.Spec.Ports = nil
			Expect(c.Update(ctx, sr)).To(Succeed())

			By("Waiting for the CronJob")
			Eventually(func() error {
				_, err := helpers.GetCronJobForResource(ctx, c, stack.Namespace, srName)
				return err
			}, readyTimeout, 2*time.Second).Should(Succeed())

			By("Waiting for the old Deployment to be deleted")
			Expect(helpers.WaitForDeleted(ctx, c, key, &appsv1.Deployment{}, deleteTimeout)).To(Succeed())

			By("Waiting for the old Deployment's pods to be gone")
			Eventually(func() (int, error) {
				pods := &corev1.PodList{}
				if err := c.List(ctx, pods, client.InNamespace(stack.Namespace), client.MatchingLabels{"resource": srName}); err != nil {
					return 0, err
				}
				fromReplicaSet := 0
				for _, p := range pods.Items {
					if owner := metav1.GetControllerOf(&p); owner != nil && owner.Kind == "ReplicaSet" {
						fromReplicaSet++
					}
				}
				return fromReplicaSet, nil
			}, deleteTimeout, 2*time.Second).Should(BeZero())

			By("Verifying the CronJob is still there")
			cj, err := helpers.GetCronJobForResource(ctx, c, stack.Namespace, srName)
			Expect(err).NotTo(HaveOccurred())
			Expect(cj.Spec.Schedule).To(Equal("*/15 * * * *"))
			Expect(metav1.IsControlledBy(cj, sr)).To(BeTrue())
		})

		AfterAll(func() { helpers.CleanupStack(ctx, c, stack) })
	})

	Context("Spec mutation - add env vars", Ordered, func() {
		var stack *corev1alpha1.Stack

		BeforeAll(func() {
			swr := fixtures.StackForMutation("mut-env-stack")
			Expect(fixtures.CreateStackWithResources(ctx, c, swr)).To(Succeed())
			stack = swr.Stack
			_, err := helpers.WaitForStackReady(ctx, c, client.ObjectKeyFromObject(stack), readyTimeout)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should update the Deployment env vars when added to the StackResource", func() {
			srName := stack.Spec.ResourceNames[0]

			By("Adding environment variables to the StackResource")
			sr := &corev1alpha1.StackResource{}
			Expect(c.Get(ctx, client.ObjectKey{Name: srName, Namespace: stack.Namespace}, sr)).To(Succeed())
			sr.Spec.EnvironmentVariables = []corev1alpha1.EnvironmentVariable{
				{Name: "NEW_VAR", Value: "new-value"},
				{Name: "ANOTHER_VAR", Value: "another-value"},
			}
			Expect(c.Update(ctx, sr)).To(Succeed())

			By("Waiting for Deployment to have the new env vars")
			dep, err := helpers.WaitForDeploymentUpdated(ctx, c, stack.Namespace, srName, func(d *appsv1.Deployment) bool {
				_, found := helpers.GetContainerEnvVar(d, "NEW_VAR")
				return found
			}, readyTimeout)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying both env var values")
			val, found := helpers.GetContainerEnvVar(dep, "NEW_VAR")
			Expect(found).To(BeTrue())
			Expect(val).To(Equal("new-value"))

			val, found = helpers.GetContainerEnvVar(dep, "ANOTHER_VAR")
			Expect(found).To(BeTrue())
			Expect(val).To(Equal("another-value"))
		})

		AfterAll(func() { helpers.CleanupStack(ctx, c, stack) })
	})

	Context("Spec mutation - add resource to Stack", Ordered, func() {
		var stack *corev1alpha1.Stack

		BeforeAll(func() {
			swr := fixtures.StackForMutation("mut-addres-stack")
			Expect(fixtures.CreateStackWithResources(ctx, c, swr)).To(Succeed())
			stack = swr.Stack
			_, err := helpers.WaitForStackReady(ctx, c, client.ObjectKeyFromObject(stack), readyTimeout)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should create a new StackResource when a resource is added to the Stack", func() {
			secondResourceName := "mut-addres-stack-api"

			By("Re-getting the Stack from the API server")
			Expect(c.Get(ctx, client.ObjectKeyFromObject(stack), stack)).To(Succeed())

			By("Adding the new resource name to the Stack")
			stack.Spec.ResourceNames = append(stack.Spec.ResourceNames, secondResourceName)
			Expect(c.Update(ctx, stack)).To(Succeed())

			By("Creating the new StackResource")
			newSR := &corev1alpha1.StackResource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secondResourceName,
					Namespace: stack.Namespace,
					Labels: map[string]string{
						corev1alpha1.LabelManagedBy:    corev1alpha1.ManagedByStackdome,
						corev1alpha1.LabelStackName:    stack.Name,
						corev1alpha1.LabelResourceName: secondResourceName,
					},
					Annotations: map[string]string{
						corev1alpha1.RevisionAnnotation: fixtures.TestRevision,
					},
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						// nginx:1.25-alpine serves :80 only; no spec here asserts
						// on the number, so declare the port it actually listens
						// on or the port readiness gate keeps it unavailable.
						{Name: "http", Number: 80, Protocol: "http", FQDN: secondResourceName + ".local"},
					},
				},
			}
			newSR.OwnerReferences = []metav1.OwnerReference{{
				APIVersion:         "core.stackdome.io/v1alpha1",
				Kind:               "Stack",
				Name:               stack.Name,
				UID:                stack.UID,
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			}}
			Expect(c.Create(ctx, newSR)).To(Succeed())

			By("Waiting for the second StackResource to become Available")
			_, err := helpers.WaitForStackResourceAvailable(ctx, c, client.ObjectKey{
				Name:      secondResourceName,
				Namespace: stack.Namespace,
			}, readyTimeout)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying Deployment exists for the second resource")
			Expect(helpers.DeploymentExists(ctx, c, stack.Namespace, secondResourceName)).To(BeTrue())

			By("Waiting for Stack to reach Ready")
			_, err = helpers.WaitForStackReady(ctx, c, client.ObjectKeyFromObject(stack), readyTimeout)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterAll(func() { helpers.CleanupStack(ctx, c, stack) })
	})

	Context("StackResource cascade deletion", Ordered, func() {
		var stack *corev1alpha1.Stack

		BeforeAll(func() {
			swr := fixtures.SimpleStack("cascade-del-stack")
			Expect(fixtures.CreateStackWithResources(ctx, c, swr)).To(Succeed())
			stack = swr.Stack
			_, err := helpers.WaitForStackReady(ctx, c, client.ObjectKeyFromObject(stack), readyTimeout)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should delete owned Deployment and Service when StackResource is deleted", func() {
			srName := stack.Spec.ResourceNames[0]

			By("Verifying Deployment and Service exist")
			Expect(helpers.DeploymentExists(ctx, c, stack.Namespace, srName)).To(BeTrue())
			_, err := helpers.GetServiceForResource(ctx, c, stack.Namespace, srName)
			Expect(err).NotTo(HaveOccurred())

			By("Removing the resource from the Stack ResourceNames")
			Expect(c.Get(ctx, client.ObjectKeyFromObject(stack), stack)).To(Succeed())
			stack.Spec.ResourceNames = []string{}
			Expect(c.Update(ctx, stack)).To(Succeed())

			By("Deleting the StackResource")
			sr := &corev1alpha1.StackResource{}
			Expect(c.Get(ctx, client.ObjectKey{Name: srName, Namespace: stack.Namespace}, sr)).To(Succeed())
			Expect(c.Delete(ctx, sr)).To(Succeed())

			By("Waiting for StackResource to be deleted")
			Expect(helpers.WaitForStackResourceDeleted(ctx, c, client.ObjectKey{
				Name:      srName,
				Namespace: stack.Namespace,
			}, deleteTimeout)).To(Succeed())

			By("Verifying Deployment is deleted")
			Eventually(func() bool {
				return !helpers.DeploymentExists(ctx, c, stack.Namespace, srName)
			}, deleteTimeout, 5*time.Second).Should(BeTrue())

			By("Verifying Service is deleted")
			Eventually(func() bool {
				_, err := helpers.GetServiceForResource(ctx, c, stack.Namespace, srName)
				return errors.IsNotFound(err)
			}, deleteTimeout, 5*time.Second).Should(BeTrue())
		})

		AfterAll(func() { helpers.CleanupStack(ctx, c, stack) })
	})

	Context("Stack deletion", Ordered, func() {
		It("should clean up all owned resources on deletion", func() {
			swr := fixtures.StackForDeletion("del-stack")
			Expect(fixtures.CreateStackWithResources(ctx, c, swr)).To(Succeed())
			stack := swr.Stack
			srName := stack.Spec.ResourceNames[0]

			By("Waiting for Stack to become Ready")
			_, err := helpers.WaitForStackReady(ctx, c, client.ObjectKeyFromObject(stack), readyTimeout)
			Expect(err).NotTo(HaveOccurred())

			By("Verifying StackResource and Deployment exist")
			Expect(helpers.DeploymentExists(ctx, c, stack.Namespace, srName)).To(BeTrue())

			By("Deleting the Stack CR")
			Expect(c.Delete(ctx, stack)).To(Succeed())

			By("Waiting for Stack to be deleted")
			Expect(helpers.WaitForStackDeleted(ctx, c, client.ObjectKeyFromObject(stack), deleteTimeout)).To(Succeed())

			By("Verifying the child StackResource is also deleted")
			sr := &corev1alpha1.StackResource{}
			err = c.Get(ctx, client.ObjectKey{
				Name:      srName,
				Namespace: stack.Namespace,
			}, sr)
			Expect(errors.IsNotFound(err)).To(BeTrue(), "StackResource should be deleted")

			By("Verifying the Deployment is also deleted")
			dep := &appsv1.Deployment{}
			err = c.Get(ctx, client.ObjectKey{
				Name:      srName,
				Namespace: stack.Namespace,
			}, dep)
			Expect(errors.IsNotFound(err)).To(BeTrue(), "Deployment should be deleted")
		})
	})
})
