package fixtures

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "stackdome.io/cluster-agent/api/core/v1alpha1"
)

const (
	BuildSourceRepoURL         = "https://github.com/ashishmax31/test-private-repo.git"
	BuildSourceBranch          = "main"
	BuildSourceCommit          = "0e7a32d8abb53958e725529595209ffb6841a333"
	BuildSourceResourceName    = "todo-app"
	RegistryDockerConfigSecret = "registry-docker-config"
	// Requires: main branch, root Dockerfile, docker/Dockerfile.prod, docker/Dockerfile.broken
	PublicTestRepoURL = "https://github.com/axernel-labs/test-repo.git"

	TestRevision = "test-rev-1"
)

// StackWithResources groups a Stack with the StackResource objects that belong to it.
type StackWithResources struct {
	Stack     *corev1alpha1.Stack
	Resources []*corev1alpha1.StackResource
}

// CreateStackWithResources creates the Stack, re-reads it to obtain the UID,
// then creates each StackResource with an owner reference pointing to the Stack.
func CreateStackWithResources(ctx context.Context, c client.Client, swr *StackWithResources) error {
	setRevisionAnnotation(swr.Stack)
	if err := c.Create(ctx, swr.Stack); err != nil {
		return err
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(swr.Stack), swr.Stack); err != nil {
		return err
	}
	for _, sr := range swr.Resources {
		setRevisionAnnotation(sr)
		sr.OwnerReferences = []metav1.OwnerReference{OwnerRefTo(swr.Stack)}
		if err := c.Create(ctx, sr); err != nil {
			return err
		}
	}
	return nil
}

func setRevisionAnnotation(obj metav1.Object) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[corev1alpha1.RevisionAnnotation] = TestRevision
	obj.SetAnnotations(annotations)
}

func stackLabels(stackName string) map[string]string {
	return map[string]string{
		corev1alpha1.LabelManagedBy: corev1alpha1.ManagedByStackdome,
		corev1alpha1.LabelStackName: stackName,
	}
}

func resourceLabels(stackName, resourceName string) map[string]string {
	return map[string]string{
		corev1alpha1.LabelManagedBy:    corev1alpha1.ManagedByStackdome,
		corev1alpha1.LabelStackName:    stackName,
		corev1alpha1.LabelResourceName: resourceName,
	}
}

// SimpleStack creates a Stack with a single nginx-based StackResource.
func SimpleStack(name string) *StackWithResources {
	resourceName := name + "-web"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceName + ".local"},
					},
				},
			},
		},
	}
}

// StackWithMismatchedPort creates a Stack whose single resource declares a port
// nothing inside the container listens on. crccheck/hello-world serves :8000,
// so the declared :80 is provably dead. This reproduces the live incident where
// a wrong port number left a public resource permanently unavailable behind a
// 502, and is the negative case for the port readiness gate.
func StackWithMismatchedPort(name string) *StackWithResources {
	resourceName := name + "-mismatch"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						// Listens on :8000 only.
						Image: "crccheck/hello-world:latest",
					},
					Ports: []corev1alpha1.Port{
						// Deliberately wrong: nothing answers on :80.
						{Name: "http", Number: 80, Protocol: "http", ExposeToPublic: true, FQDN: resourceName + ".example.com"},
					},
				},
			},
		},
	}
}

// MultiResourceStack creates a Stack with a backend and frontend resource.
// The frontend has a plain BACKEND_URL env var (no interpolation).
func MultiResourceStack(name string) *StackWithResources {
	backendName := name + "-backend"
	frontendName := name + "-frontend"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{backendName, frontendName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      backendName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, backendName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						// nginx:1.25-alpine serves :80 only, so the declared
						// port must be 80 for the readiness gate to pass.
						{Name: "http", Number: 80, Protocol: "http", FQDN: backendName + ".local"},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      frontendName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, frontendName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: frontendName + ".local"},
					},
					EnvironmentVariables: []corev1alpha1.EnvironmentVariable{
						{Name: "BACKEND_URL", Value: backendName},
					},
				},
			},
		},
	}
}

// StackWithDependencies creates a Stack where resource B depends on resource A.
func StackWithDependencies(name string) *StackWithResources {
	resourceA := name + "-dep-a"
	resourceB := name + "-dep-b"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceA, resourceB},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceA,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceA),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceA + ".local"},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceB,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceB),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceB + ".local"},
					},
					DependsOn: []string{resourceA},
				},
			},
		},
	}
}

// StackWithEnvAndPorts creates a Stack with explicit environment variables
// and multiple ports on a single resource.
func StackWithEnvAndPorts(name string) *StackWithResources {
	resourceName := name + "-app"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					// This fixture exercises multi-port Service/Deployment
					// shape, so both declared ports are kept. nginx serves
					// :80 out of the box; teach it to listen on 8080 and 9090
					// as well so the declaration is honest.
					Command: []string{"/bin/sh", "-c"},
					Args: []string{
						"printf 'server{listen 8080;}\\nserver{listen 9090;}\\n' > /etc/nginx/conf.d/extra.conf && exec nginx -g 'daemon off;'",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 8080, Protocol: "http", FQDN: resourceName + "-http.local"},
						{Name: "metrics", Number: 9090, Protocol: "http", FQDN: resourceName + "-metrics.local"},
					},
					EnvironmentVariables: []corev1alpha1.EnvironmentVariable{
						{Name: "APP_ENV", Value: "integration-test"},
						{Name: "APP_PORT", Value: "8080"},
						{Name: "LOG_LEVEL", Value: "debug"},
					},
				},
			},
		},
	}
}

// StackWithInitContainer creates a Stack whose resource has an init container.
func StackWithInitContainer(name string) *StackWithResources {
	resourceName := name + "-init"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Init: &corev1alpha1.InitSpec{
						Command: []string{"sh"},
						Args:    []string{"-c", "echo 'init done'"},
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceName + ".local"},
					},
				},
			},
		},
	}
}

// StackForDeletion creates a simple Stack used to test cascade deletion.
func StackForDeletion(name string) *StackWithResources {
	return SimpleStack(name)
}

// StackWithBuildArgs creates a Stack with a single resource that builds from a
// private git repo and uses build arguments (both inline and secret-backed).
func StackWithBuildArgs(name, registryURL, gitSecretName, buildArgSecretName string) *StackWithResources {
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{BuildSourceResourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      BuildSourceResourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, BuildSourceResourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					BuildSpec: &corev1alpha1.StackResourceBuildSpec{
						SourceContext: corev1alpha1.BuildContextSource{
							Git: &corev1alpha1.GitRepoSource{
								RepoUrl: BuildSourceRepoURL,
								Auth: &corev1alpha1.GitAuth{
									PersonalAccessTokenRef: &corev1alpha1.CredentialSecretKeyPair{
										SecretRef: corev1.SecretReference{
											Name:      gitSecretName,
											Namespace: defaultNamespace,
										},
										UsernameKey: "username",
										PasswordKey: "token",
									},
								},
							},
						},
						BuildContext:   ".",
						DockerFilePath: "Dockerfile",
						SourceRevision: corev1alpha1.SourceRevisionSpec{
							GitRepo: &corev1alpha1.GitRepoRevision{
								Branch: BuildSourceBranch,
								Commit: BuildSourceCommit,
							},
						},
						Repository: RepositorySpecWithAuth(registryURL),
						BuildArgs: []corev1alpha1.BuildArg{
							{
								Name:  "APP_ENV",
								Value: "integration-test",
							},
							{
								Name: "BUILD_TOKEN",
								ValueFrom: &corev1alpha1.BuildArgValueSource{
									SecretKeyRef: corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: buildArgSecretName,
										},
										Key: "token",
									},
								},
							},
						},
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 3000, Protocol: "http", FQDN: name + ".local"},
					},
				},
			},
		},
	}
}

// StackWithCommandArgs creates a Stack with a single resource that has
// explicit Command and Args set.
func StackWithCommandArgs(name string) *StackWithResources {
	resourceName := name + "-cmd"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Command: []string{"nginx"},
					Args:    []string{"-g", "daemon off;"},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceName + ".local"},
					},
				},
			},
		},
	}
}

// StackWithPublicPorts creates a Stack with a resource that has one public
// port and one internal port.
func StackWithPublicPorts(name string) *StackWithResources {
	resourceName := name + "-pub"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					// The public/metrics split is the point of this fixture,
					// so both ports stay. nginx already serves :80; add a
					// listener on 9090 so the metrics port is real too.
					Command: []string{"/bin/sh", "-c"},
					Args: []string{
						"printf 'server{listen 9090;}\\n' > /etc/nginx/conf.d/extra.conf && exec nginx -g 'daemon off;'",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", ExposeToPublic: true, FQDN: name + "-pub.example.com"},
						{Name: "metrics", Number: 9090, Protocol: "http", FQDN: resourceName + "-metrics.local"},
					},
				},
			},
		},
	}
}

// StackWithTLSPort creates a Stack whose single resource exposes one public port with
// TLS enabled, for exercising certificate issuance gating. The ClusterIssuer annotation
// goes on the StackResource, which is where the svc reconciler reads it.
func StackWithTLSPort(name, issuerName string) *StackWithResources {
	resourceName := name + "-tls-app"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:        resourceName,
					Namespace:   defaultNamespace,
					Labels:      resourceLabels(name, resourceName),
					Annotations: map[string]string{corev1alpha1.ClusterIssuerAnnotation: issuerName},
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", ExposeToPublic: true, FQDN: resourceName + ".example.com", TLS: true},
					},
				},
			},
		},
	}
}

// StackWithNoPorts creates a Stack with a single resource that has no ports.
func StackWithNoPorts(name string) *StackWithResources {
	resourceName := name + "-noport"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
				},
			},
		},
	}
}

// StackWithDependencyChain creates a Stack with 3 resources forming a linear
// dependency chain: A -> B -> C.
func StackWithDependencyChain(name string) *StackWithResources {
	resourceA := name + "-chain-a"
	resourceB := name + "-chain-b"
	resourceC := name + "-chain-c"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceA, resourceB, resourceC},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceA,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceA),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceA + ".local"},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceB,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceB),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceB + ".local"},
					},
					DependsOn: []string{resourceA},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceC,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceC),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceC + ".local"},
					},
					DependsOn: []string{resourceB},
				},
			},
		},
	}
}

// StackWithFanInDependencies creates a Stack with 3 resources where C depends
// on both A and B (fan-in pattern).
func StackWithFanInDependencies(name string) *StackWithResources {
	resourceA := name + "-fan-a"
	resourceB := name + "-fan-b"
	resourceC := name + "-fan-c"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceA, resourceB, resourceC},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceA,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceA),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceA + ".local"},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceB,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceB),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceB + ".local"},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceC,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceC),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceC + ".local"},
					},
					DependsOn: []string{resourceA, resourceB},
				},
			},
		},
	}
}

// StackWithInitCustomImage creates a Stack whose resource has an init container
// with a custom image (busybox).
func StackWithInitCustomImage(name string) *StackWithResources {
	resourceName := name + "-init-img"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Init: &corev1alpha1.InitSpec{
						Command: []string{"sh"},
						Args:    []string{"-c", "echo 'init with custom image'"},
						ImageSpec: &corev1alpha1.ImageSpec{
							Image: "busybox:1.36",
						},
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceName + ".local"},
					},
				},
			},
		},
	}
}

// StackWithThreeResources creates a Stack with 3 independent resources:
// web, api, and worker.
func StackWithThreeResources(name string) *StackWithResources {
	webName := name + "-web"
	apiName := name + "-api"
	workerName := name + "-worker"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{webName, apiName, workerName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      webName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, webName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: webName + ".local"},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      apiName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, apiName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						// nginx serves :80 only; no test asserts on this
						// number, so declare the port it actually listens on.
						{Name: "http", Number: 80, Protocol: "http", FQDN: apiName + ".local"},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      workerName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, workerName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						// nginx serves :80 only; no test asserts on this
						// number, so declare the port it actually listens on.
						{Name: "http", Number: 80, Protocol: "http", FQDN: workerName + ".local"},
					},
				},
			},
		},
	}
}

// StackForMutation creates a simple Stack used for mutation/update tests.
func StackForMutation(name string) *StackWithResources {
	return SimpleStack(name)
}

// StackForRestart creates a simple Stack used for restart tests.
func StackForRestart(name string) *StackWithResources {
	return SimpleStack(name)
}

// SimpleBuildStack creates a Stack with a single resource that builds from a
// public git repo with no authentication, using the root Dockerfile.
func SimpleBuildStack(name, registryURL string) *StackWithResources {
	resourceName := name + "-build"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					BuildSpec: &corev1alpha1.StackResourceBuildSpec{
						SourceContext: corev1alpha1.BuildContextSource{
							Git: &corev1alpha1.GitRepoSource{
								RepoUrl: PublicTestRepoURL,
							},
						},
						BuildContext:   ".",
						DockerFilePath: "Dockerfile",
						SourceRevision: corev1alpha1.SourceRevisionSpec{
							GitRepo: &corev1alpha1.GitRepoRevision{
								Branch: "main",
								Commit: BuildSourceCommit,
							},
						},
						Repository: RepositorySpecWithAuth(registryURL),
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 3000, Protocol: "http", FQDN: name + ".local"},
					},
				},
			},
		},
	}
}

// BuildStackCustomPaths creates a Stack that builds from a public git repo
// with a custom Dockerfile path.
func BuildStackCustomPaths(name, registryURL string) *StackWithResources {
	resourceName := name + "-build"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					BuildSpec: &corev1alpha1.StackResourceBuildSpec{
						SourceContext: corev1alpha1.BuildContextSource{
							Git: &corev1alpha1.GitRepoSource{
								RepoUrl: PublicTestRepoURL,
							},
						},
						BuildContext:   ".",
						DockerFilePath: "docker/Dockerfile.prod",
						SourceRevision: corev1alpha1.SourceRevisionSpec{
							GitRepo: &corev1alpha1.GitRepoRevision{
								Branch: "main",
								Commit: BuildSourceCommit,
							},
						},
						Repository: RepositorySpecWithAuth(registryURL),
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 3000, Protocol: "http", FQDN: name + ".local"},
					},
				},
			},
		},
	}
}

// BuildStackBrokenDockerfile creates a Stack that builds from a public git repo
// but references a broken Dockerfile path, intended to test build failure handling.
func BuildStackBrokenDockerfile(name, registryURL string) *StackWithResources {
	resourceName := name + "-build"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					BuildSpec: &corev1alpha1.StackResourceBuildSpec{
						SourceContext: corev1alpha1.BuildContextSource{
							Git: &corev1alpha1.GitRepoSource{
								RepoUrl: PublicTestRepoURL,
							},
						},
						BuildContext:   ".",
						DockerFilePath: "docker/Dockerfile.broken",
						SourceRevision: corev1alpha1.SourceRevisionSpec{
							GitRepo: &corev1alpha1.GitRepoRevision{
								Branch: "main",
								Commit: BuildSourceCommit,
							},
						},
						Repository: RepositorySpecWithAuth(registryURL),
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 3000, Protocol: "http", FQDN: name + ".local"},
					},
				},
			},
		},
	}
}

func RepositorySpecWithAuth(registryURL string) corev1alpha1.ImageRepositorySpec {
	return corev1alpha1.ImageRepositorySpec{
		External: &corev1alpha1.ExternalRegistrySpec{
			Host: registryURL,
			TLS:  &corev1alpha1.RegistryTLSSpec{Insecure: true},
		},
		Repository: "integration-test/app",
		Auth: &corev1alpha1.RegistryCredentialsSpec{
			DockerConfig: &corev1alpha1.DockerConfigAuth{
				SecretKey: ".dockerconfigjson",
				SecretRef: &corev1.SecretReference{
					Name:      RegistryDockerConfigSecret,
					Namespace: defaultNamespace,
				},
			},
		},
	}
}

func CrashingStack(name string) *StackWithResources {
	resourceName := name + "-crash"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "busybox:1.36",
					},
					Command: []string{"sh"},
					Args:    []string{"-c", "echo 'app starting'; echo 'connecting to database'; echo 'ERROR: connection refused'; exit 1"},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 8080, Protocol: "http", FQDN: resourceName + ".local"},
					},
				},
			},
		},
	}
}

func ImagePullFailStack(name string) *StackWithResources {
	resourceName := name + "-pullfail"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nonexistent-registry.example.com/fake-image:v999",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 8080, Protocol: "http", FQDN: resourceName + ".local"},
					},
				},
			},
		},
	}
}

// SecretEnvStack creates a Stack with a single resource that has a valueFrom
// env var referencing a Kubernetes Secret.
func SecretEnvStack(name string) *StackWithResources {
	resourceName := name + "-secret-env"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{ResourceNames: []string{resourceName}},
		},
		Resources: []*corev1alpha1.StackResource{{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resourceName,
				Namespace: defaultNamespace,
				Labels:    resourceLabels(name, resourceName),
			},
			Spec: corev1alpha1.StackResourceSpec{
				ImageSpec: &corev1alpha1.ImageSpec{Image: "nginx:1.25-alpine"},
				Ports:     []corev1alpha1.Port{{Name: "http", Number: 80, Protocol: "http", FQDN: resourceName + ".local"}},
				EnvironmentVariables: []corev1alpha1.EnvironmentVariable{{
					Name: "API_KEY",
					ValueFrom: &corev1alpha1.EnvVarSource{
						SecretKeyRef: corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "test-app-secret"},
							Key:                  "API_KEY",
						},
					},
				}},
			},
		}},
	}
}

// ProbedStack creates a Stack with a single resource that has readiness and
// startup health probes configured.
func ProbedStack(name string) *StackWithResources {
	resourceName := name + "-probed"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{ResourceNames: []string{resourceName}},
		},
		Resources: []*corev1alpha1.StackResource{{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resourceName,
				Namespace: defaultNamespace,
				Labels:    resourceLabels(name, resourceName),
			},
			Spec: corev1alpha1.StackResourceSpec{
				ImageSpec: &corev1alpha1.ImageSpec{Image: "nginx:1.25-alpine"},
				Ports:     []corev1alpha1.Port{{Name: "http", Number: 80, Protocol: "http", FQDN: resourceName + ".local"}},
				HealthChecks: &corev1alpha1.HealthChecks{
					Readiness: &corev1alpha1.Probe{
						HTTPGet: &corev1alpha1.HTTPGetProbe{Path: "/", PortName: "http"},
					},
					Startup: &corev1alpha1.Probe{
						HTTPGet:          &corev1alpha1.HTTPGetProbe{Path: "/", PortName: "http"},
						FailureThreshold: 30,
					},
				},
			},
		}},
	}
}

// WorkerStack creates a Stack with a single Worker-type resource (no ports, no Service).
func WorkerStack(name string) *StackWithResources {
	resourceName := name + "-worker"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{ResourceNames: []string{resourceName}},
		},
		Resources: []*corev1alpha1.StackResource{{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resourceName,
				Namespace: defaultNamespace,
				Labels:    resourceLabels(name, resourceName),
			},
			Spec: corev1alpha1.StackResourceSpec{
				WorkloadType: corev1alpha1.WorkloadTypeWorker,
				ImageSpec:    &corev1alpha1.ImageSpec{Image: "busybox:1.36"},
				Command:      []string{"sh", "-c", "sleep 3600"},
			},
		}},
	}
}

// StackWithAllOptionalFields populates env, ports, command, args, and init —
// used by field-removal specs as the fully-populated starting point.
func StackWithAllOptionalFields(name string) *StackWithResources {
	resourceName := name + "-mig"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "nginx:1.25-alpine",
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 80, Protocol: "http", FQDN: resourceName + ".local"},
					},
					EnvironmentVariables: []corev1alpha1.EnvironmentVariable{
						{Name: "MIGRATION_TEST", Value: "true"},
						{Name: "DEBUG", Value: "1"},
					},
					Command: []string{"nginx", "-g", "daemon off;"},
					Args:    []string{"-c", "/etc/nginx/nginx.conf"},
					Init: &corev1alpha1.InitSpec{
						Command: []string{"sh"},
						Args:    []string{"-c", "echo init"},
					},
				},
			},
		},
	}
}

// BuildStackWithClusterRegistry creates a Stack with a single resource that
// builds from a public git repo and pushes to a ClusterRegistry using BasicAuth
// credentials instead of a DockerConfig secret.
func BuildStackWithClusterRegistry(name, registryName, credsSecretName, testNamespace string) *StackWithResources {
	resourceName := name + "-build"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: testNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: testNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					BuildSpec: &corev1alpha1.StackResourceBuildSpec{
						SourceContext: corev1alpha1.BuildContextSource{
							Git: &corev1alpha1.GitRepoSource{
								RepoUrl: PublicTestRepoURL,
							},
						},
						BuildContext:   ".",
						DockerFilePath: "Dockerfile",
						SourceRevision: corev1alpha1.SourceRevisionSpec{
							GitRepo: &corev1alpha1.GitRepoRevision{
								Branch: BuildSourceBranch,
								Commit: BuildSourceCommit,
							},
						},
						Repository: corev1alpha1.ImageRepositorySpec{
							ClusterRegistryRef: &corev1.LocalObjectReference{Name: registryName},
							Repository:         "integration-test/cluster-reg-app",
							Auth: &corev1alpha1.RegistryCredentialsSpec{
								Basic: &corev1alpha1.BasicAuthCredentials{
									SecretRef:   corev1.SecretReference{Name: credsSecretName, Namespace: testNamespace},
									UsernameKey: "username",
									PasswordKey: "password",
								},
							},
						},
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 3000, Protocol: "http"},
					},
				},
			},
		},
	}
}

func BuildStackWithDockerHub(name, repository, credsSecretName, testNamespace string) *StackWithResources {
	resourceName := name + "-build"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: testNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: testNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					BuildSpec: &corev1alpha1.StackResourceBuildSpec{
						SourceContext: corev1alpha1.BuildContextSource{
							Git: &corev1alpha1.GitRepoSource{
								RepoUrl: PublicTestRepoURL,
							},
						},
						BuildContext:   ".",
						DockerFilePath: "Dockerfile",
						SourceRevision: corev1alpha1.SourceRevisionSpec{
							GitRepo: &corev1alpha1.GitRepoRevision{
								Branch: BuildSourceBranch,
								Commit: BuildSourceCommit,
							},
						},
						Repository: corev1alpha1.ImageRepositorySpec{
							External: &corev1alpha1.ExternalRegistrySpec{
								Host: "docker.io",
							},
							Repository: repository,
							Auth: &corev1alpha1.RegistryCredentialsSpec{
								Basic: &corev1alpha1.BasicAuthCredentials{
									SecretRef:   corev1.SecretReference{Name: credsSecretName, Namespace: testNamespace},
									UsernameKey: "username",
									PasswordKey: "password",
								},
							},
						},
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 3000, Protocol: "http"},
					},
				},
			},
		},
	}
}

func InitContainerFailStack(name string) *StackWithResources {
	resourceName := name + "-initfail"
	return &StackWithResources{
		Stack: &corev1alpha1.Stack{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: defaultNamespace,
				Labels:    stackLabels(name),
			},
			Spec: corev1alpha1.StackSpec{
				ResourceNames: []string{resourceName},
			},
		},
		Resources: []*corev1alpha1.StackResource{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: defaultNamespace,
					Labels:    resourceLabels(name, resourceName),
				},
				Spec: corev1alpha1.StackResourceSpec{
					ImageSpec: &corev1alpha1.ImageSpec{
						Image: "busybox:1.36",
					},
					Init: &corev1alpha1.InitSpec{
						Command: []string{"sh"},
						Args:    []string{"-c", "echo 'init: checking dependencies'; echo 'init: FATAL: missing required config'; exit 1"},
					},
					Ports: []corev1alpha1.Port{
						{Name: "http", Number: 8080, Protocol: "http", FQDN: resourceName + ".local"},
					},
				},
			},
		},
	}
}
