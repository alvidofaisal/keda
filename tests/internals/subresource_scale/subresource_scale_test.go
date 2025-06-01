//go:build e2e
// +build e2e

package subresource_scale_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"

	. "github.com/kedacore/keda/v2/tests/helper"
)

const (
	testName = "subresource-scale-test"
)

var (
	testNamespace           = fmt.Sprintf("%s-ns", testName)
	argoNamespace             = "argo-rollouts"
	monitoredDeploymentName   = fmt.Sprintf("%s-monitored", testName)
	argoRolloutName           = fmt.Sprintf("%s-rollout", testName)
	scaledObjectName          = fmt.Sprintf("%s-so", testName)
	clusterCRDName            = "clusterscalers.testing.keda.sh"
	clusterCRName             = fmt.Sprintf("%s-cr", testName)
	clusterScaledObjectName = fmt.Sprintf("%s-cluster-so", testName)
)

type templateData struct {
	TestNamespace           string
	MonitoredDeploymentName string
	ArgoRolloutName         string
	ScaledObjectName        string
	ClusterCRName           string
	ClusterScaledObjectName string
}

const (
	monitoredDeploymentTemplate = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{.MonitoredDeploymentName}}
  namespace: {{.TestNamespace}}
  labels:
    app: {{.MonitoredDeploymentName}}
spec:
  replicas: 0
  selector:
    matchLabels:
      app: {{.MonitoredDeploymentName}}
  template:
    metadata:
      labels:
        app: {{.MonitoredDeploymentName}}
    spec:
      containers:
        - name: nginx
          image: 'ghcr.io/nginx/nginx-unprivileged:1.26'`

	argoRolloutTemplate = `apiVersion: argoproj.io/v1alpha1
kind: Rollout
metadata:
  name: {{.ArgoRolloutName}}
  namespace: {{.TestNamespace}}
  labels:
    app: {{.ArgoRolloutName}}
spec:
  replicas: 0
  strategy:
    canary:
      steps:
        - setWeight: 50
        - pause: {duration: 10}
  selector:
    matchLabels:
      app: {{.ArgoRolloutName}}
  template:
    metadata:
      labels:
        app: {{.ArgoRolloutName}}
    spec:
      containers:
        - name: nginx
          image: ghcr.io/nginx/nginx-unprivileged:1.26
`

	scaledObjectTemplate = `apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: {{.ScaledObjectName}}
  namespace: {{.TestNamespace}}
spec:
  scaleTargetRef:
    apiVersion: argoproj.io/v1alpha1
    kind: Rollout
    name: {{.ArgoRolloutName}}
  pollingInterval: 5
  cooldownPeriod: 5
  minReplicaCount: 0
  maxReplicaCount: 10
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 5
  triggers:
  - type: kubernetes-workload
    metadata:
      podSelector: 'app={{.MonitoredDeploymentName}}'
      value: '1'
`

	clusterScalerCRDTemplate = `
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: {{.ClusterCRDName}}
spec:
  group: testing.keda.sh
  versions:
    - name: v1alpha1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                replicas:
                  type: integer
                  format: int32
            status:
              type: object
              properties:
                replicas:
                  type: integer
                  format: int32
      subresources:
        scale:
          specReplicasPath: .spec.replicas
          statusReplicasPath: .status.replicas
  scope: Cluster
  names:
    plural: clusterscalers
    singular: clusterscaler
    kind: ClusterScaler
`
	clusterScalerCRTemplate = `
apiVersion: testing.keda.sh/v1alpha1
kind: ClusterScaler
metadata:
  name: {{.ClusterCRName}}
spec:
  replicas: 0
`

	clusterScaledObjectTemplate = `
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: {{.ClusterScaledObjectName}}
  namespace: {{.TestNamespace}}
spec:
  scaleTargetRef:
    apiVersion: testing.keda.sh/v1alpha1
    kind: ClusterScaler
    name: {{.ClusterCRName}}
  pollingInterval: 1
  cooldownPeriod: 1
  minReplicaCount: 0
  maxReplicaCount: 2
  triggers:
  - type: cpu
    metricType: Utilization
    metadata:
      value: "10"
`
)

func TestScaler(t *testing.T) {
	// setup
	t.Log("--- setting up ---")
	// Create kubernetes resources
	kc := GetKubernetesClient(t)
	data, templates := getTemplateData()
	t.Cleanup(func() {
		// cleanup
		DeleteKubernetesResources(t, testNamespace, data, templates)
		cleanupArgo(t)
	})
	setupArgo(t, kc)

	CreateKubernetesResources(t, kc, testNamespace, data, templates)
	assert.True(t, waitForArgoRolloutReplicaCount(t, argoRolloutName, testNamespace, 0),
		"replica count should be 0 after 1 minute")

	// test scaling
	testScaleOut(t, kc)
	testScaleIn(t, kc)

	// Test cluster-scoped CRD scaling
	testClusterScopedCRDScale(t, kc)
}

func setupArgo(t *testing.T, kc *kubernetes.Clientset) {
	CreateNamespace(t, kc, argoNamespace)
	cmdWithNamespace := fmt.Sprintf("kubectl apply -n %s -f https://github.com/argoproj/argo-rollouts/releases/latest/download/install.yaml",
		argoNamespace)
	_, err := ExecuteCommand(cmdWithNamespace)

	require.NoErrorf(t, err, "cannot install argo resources - %s", err)
}

func cleanupArgo(t *testing.T) {
	cmdWithNamespace := fmt.Sprintf("kubectl delete -n %s -f https://github.com/argoproj/argo-rollouts/releases/latest/download/install.yaml",
		argoNamespace)
	_, err := ExecuteCommand(cmdWithNamespace)

	assert.NoErrorf(t, err, "cannot delete argo resources - %s", err)
	DeleteNamespace(t, argoNamespace)
}

func testScaleOut(t *testing.T, kc *kubernetes.Clientset) {
	t.Log("--- testing scale out ---")

	// scale monitored deployment to 5 replicas
	KubernetesScaleDeployment(t, kc, monitoredDeploymentName, 5, testNamespace)
	assert.True(t, waitForArgoRolloutReplicaCount(t, argoRolloutName, testNamespace, 5),
		"replica count should be 5 after 1 minute")

	// scale monitored deployment to 10 replicas
	KubernetesScaleDeployment(t, kc, monitoredDeploymentName, 10, testNamespace)
	assert.True(t, waitForArgoRolloutReplicaCount(t, argoRolloutName, testNamespace, 10),
		"replica count should be 10 after 1 minute")
}

func testScaleIn(t *testing.T, kc *kubernetes.Clientset) {
	t.Log("--- testing scale in ---")

	// scale monitored deployment to 5 replicas
	KubernetesScaleDeployment(t, kc, monitoredDeploymentName, 5, testNamespace)
	assert.True(t, waitForArgoRolloutReplicaCount(t, argoRolloutName, testNamespace, 5),
		"replica count should be 5 after 1 minute")

	// scale monitored deployment to 0 replicas
	KubernetesScaleDeployment(t, kc, monitoredDeploymentName, 0, testNamespace)
	assert.True(t, waitForArgoRolloutReplicaCount(t, argoRolloutName, testNamespace, 0),
		"replica count should be 0 after 1 minute")
}

func getTemplateData() (templateData, []Template) {
	return templateData{
			TestNamespace:           testNamespace,
			MonitoredDeploymentName: monitoredDeploymentName,
			ArgoRolloutName:         argoRolloutName,
			TestNamespace:           testNamespace,
			MonitoredDeploymentName: monitoredDeploymentName,
			ArgoRolloutName:         argoRolloutName,
			ScaledObjectName:        scaledObjectName,
			ClusterCRName:           clusterCRName,
			ClusterScaledObjectName: clusterScaledObjectName,
		}, []Template{
			{Name: "monitoredDeploymentTemplate", Config: monitoredDeploymentTemplate},
			{Name: "argoRolloutTemplate", Config: argoRolloutTemplate},
			{Name: "scaledObjectTemplate", Config: scaledObjectTemplate},
		}
}

func getClusterTemplateData() (templateData, []Template) {
	return templateData{
			TestNamespace:           testNamespace,
			ClusterCRName:           clusterCRName,
			ClusterScaledObjectName: clusterScaledObjectName,
		}, []Template{
			{Name: "clusterScalerCRDTemplate", Config: clusterScalerCRDTemplate, AdditionalData: map[string]string{"ClusterCRDName": clusterCRDName}},
			{Name: "clusterScalerCRTemplate", Config: clusterScalerCRTemplate},
			{Name: "clusterScaledObjectTemplate", Config: clusterScaledObjectTemplate},
		}
}

func waitForArgoRolloutReplicaCount(t *testing.T, name, namespace string, target int) bool {
	for i := 0; i < 60; i++ {
		kctlGetCmd := fmt.Sprintf(`kubectl get rollouts.argoproj.io/%s -n %s -o jsonpath="{.spec.replicas}"`, argoRolloutName, namespace)
		output, err := ExecuteCommand(kctlGetCmd)

		assert.NoErrorf(t, err, "cannot get rollout info - %s", err)

		unqoutedOutput := strings.ReplaceAll(string(output), "\"", "")
		replicas, err := strconv.ParseInt(unqoutedOutput, 10, 64)
		assert.NoErrorf(t, err, "cannot convert rollout count to int - %s", err)

		t.Logf("Waiting for rollout replicas to hit target. Name - %s, Current  - %d, Target - %d",
			name, replicas, target)

		if replicas == int64(target) {
			return true
		}

		time.Sleep(time.Second)
	}

	return false
}

func testClusterScopedCRDScale(t *testing.T, kc *kubernetes.Clientset) {
	t.Log("--- testing cluster-scoped CRD scale ---")
	data, templates := getClusterTemplateData()

	// Create CRD
	KubectlApplyWithTemplate(t, data, "clusterScalerCRDTemplate", templates)
	t.Cleanup(func() {
		KubectlDeleteWithTemplate(t, data, "clusterScalerCRDTemplate", templates)
	})

	// Create CR and ScaledObject
	CreateKubernetesResources(t, kc, testNamespace, data, templates[1:]) // Skip CRD template
	t.Cleanup(func() {
		DeleteKubernetesResources(t, testNamespace, data, templates[1:])
	})

	assert.True(t, waitForClusterCRReplicaCount(t, clusterCRName, 0, 60),
		"replica count should be 0 after 1 minute")

	// Create a stress deployment to trigger CPU scaling
	stressReplicas := 2
	stressData := struct{ TestNamespace, Name string }{TestNamespace: testNamespace, Name: "stress"}
	KubectlApplyWithTemplate(t, stressData, stressDeploymentTemplate, GetStressTemplates(testNamespace, stressReplicas))
	t.Cleanup(func() {
		KubectlDeleteWithTemplate(t, stressData, stressDeploymentTemplate, GetStressTemplates(testNamespace, stressReplicas))
	})

	// Check if CR scaled out
	t.Log("--- checking scale out for cluster CRD ---")
	assert.True(t, waitForClusterCRReplicaCount(t, clusterCRName, 2, 180), // Increased timeout for scaling
		"replica count should be 2 after 3 minutes")

	// Check KEDA operator logs for errors
	kedaOperatorLogs, err := GetPodLogs(t, kc, KedaNamespace, "keda-operator", "")
	require.NoErrorf(t, err, "cannot get keda operator logs - %s", err)
	assert.NotContains(t, kedaOperatorLogs, "meta.k8s.io", "KEDA operator logs should not contain errors related to incorrect API group querying")

	// Remove stress deployment
	KubectlDeleteWithTemplate(t, stressData, stressDeploymentTemplate, GetStressTemplates(testNamespace, stressReplicas))

	// Check if CR scaled in
	t.Log("--- checking scale in for cluster CRD ---")
	assert.True(t, waitForClusterCRReplicaCount(t, clusterCRName, 0, 180), // Increased timeout for scaling
		"replica count should be 0 after 3 minutes")
}

func waitForClusterCRReplicaCount(t *testing.T, name string, targetReplicas, timeout int) bool {
	for i := 0; i < timeout; i++ {
		// Note: No namespace for cluster-scoped CRs
		kctlGetCmd := fmt.Sprintf(`kubectl get clusterscaler %s -o jsonpath="{.spec.replicas}"`, name)
		output, err := ExecuteCommand(kctlGetCmd)
		if err != nil {
			// It might take a moment for the CR to be available after creation or deletion
			t.Logf("Error getting ClusterScaler %s: %v. Retrying...", name, err)
			time.Sleep(time.Second)
			continue
		}

		unquotedOutput := strings.ReplaceAll(string(output), "\"", "")
		if unquotedOutput == "" { // Handle case where replicas might not be set initially
			t.Logf("ClusterScaler %s replicas not yet set. Retrying...", name)
			time.Sleep(time.Second)
			continue
		}
		replicas, err := strconv.ParseInt(unquotedOutput, 10, 64)
		if err != nil {
			t.Logf("Error converting replica count for %s: %v. Output: '%s'. Retrying...", name, err, output)
			time.Sleep(time.Second)
			continue
		}

		t.Logf("Waiting for ClusterScaler replicas. Name - %s, Current - %d, Target - %d",
			name, replicas, targetReplicas)

		if replicas == int64(targetReplicas) {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

const stressDeploymentTemplate = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{.Name}}
  namespace: {{.TestNamespace}}
  labels:
    app: {{.Name}}
spec:
  replicas: {{.Replicas}}
  selector:
    matchLabels:
      app: {{.Name}}
  template:
    metadata:
      labels:
        app: {{.Name}}
    spec:
      containers:
      - name: stress
        image: polinux/stress
        args:
        - --cpu
        - "1"
        resources:
          requests:
            cpu: "0.5"
          limits:
            cpu: "1"
`

func GetStressTemplates(namespace string, replicas int) []Template {
	return []Template{
		{
			Name:   "stressDeploymentTemplate",
			Config: stressDeploymentTemplate,
			AdditionalData: map[string]string{
				"TestNamespace": namespace,
				"Name":          "stress",
				"Replicas":      fmt.Sprintf("%d", replicas),
			},
		},
	}
}
