package networkpolicysoak

import (
	"context"
	"embed"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	api_corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/perf-tests/clusterloader2/pkg/framework"
	"k8s.io/perf-tests/clusterloader2/pkg/framework/client"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement"
	measurementutil "k8s.io/perf-tests/clusterloader2/pkg/measurement/util"
	"k8s.io/perf-tests/clusterloader2/pkg/measurement/util/gatherers"
	"k8s.io/perf-tests/clusterloader2/pkg/util"
)

const (
	// file paths
	networkPolicySoakMeasurementName = "NetworkPolicySoakMeasurement"
	serviceAccountFilePath           = "manifests/serviceaccount.yaml"
	clusterRoleFilePath              = "manifests/clusterrole.yaml"
	clusterRoleBindingFilePath       = "manifests/clusterrolebinding.yaml"
	netPolFilePath                   = "manifests/network_policy.yaml"
	clientFilePath                   = "manifests/client_deploy.yaml"
	targetFilePath                   = "manifests/target_deploy.yaml"
	APIserverFilePath                = "manifests/allow_apiserver_np.yaml"

	// variables
	clientNamespace = "np-soak-client"
	clientName      = "np-soak-client" // all the k8s resources will be prefixed with this name
	targetName      = "np-soak-target" // all the deployments in target namespaces will be named with np-soak-target
	apiserverNPName = "allow-egress-apiserver"
)

//go:embed manifests
var manifestsFS embed.FS

type NetworkPolicySoakMeasurement struct {
	isRunning            bool
	testDuration         time.Duration
	k8sClient            kubernetes.Interface
	framework            *framework.Framework
	targetNamespaces     []string
	targetLabelKey       string
	targetLabelVal       string
	clientLabelKey       string
	clientLabelVal       string
	targetReplicasPerNs  int
	clientReplicasPerDep int
	targetPort           int
	targetPath           string
	testEndTime          time.Time
	workerPerClient      int
	npType               string
	// gatherers
	gatherers *gatherers.ContainerResourceGatherer
}

func createNetworkPolicySoakMeasurement() measurement.Measurement {
	return &NetworkPolicySoakMeasurement{}
}

func init() {
	measurement.Register(networkPolicySoakMeasurementName, createNetworkPolicySoakMeasurement)
}

func (m *NetworkPolicySoakMeasurement) Execute(config *measurement.Config) ([]measurement.Summary, error) {
	action, err := util.GetString(config.Params, "action")
	if err != nil {
		return nil, err
	}

	switch action {
	case "start":
		return m.start(config)
	case "gather":
		return m.gather()
	default:
		return nil, fmt.Errorf("unknown action: %s", action)
	}
}

func (m *NetworkPolicySoakMeasurement) start(config *measurement.Config) ([]measurement.Summary, error) {
	if m.isRunning {
		return nil, fmt.Errorf("phase: start, %s: measurement already running", m.String())
	}

	if err := m.initialize(config); err != nil {
		return nil, err
	}

	// create the client namespace
	if err := client.CreateNamespace(m.k8sClient, clientNamespace); err != nil {
		return nil, fmt.Errorf("phase: start, %s: failed to create namespace %s: %v", m.String(), clientNamespace, err)
	}

	// deploy the RBAC resources
	if err := m.deployRBACResources(); err != nil {
		return nil, err
	}

	// deploy the target pods
	if err := m.deployTargetPods(); err != nil {
		return nil, err
	}

	// deploy the network policy to allow traffic from client to API server
	if err := m.deployAPIServerNetworkPolicy(); err != nil {
		return nil, err
	}

	// deploy the network policy to allow traffic from client to target pods
	if err := m.deployNetworkPolicy(); err != nil {
		return nil, err
	}

	// deploy the client pods
	if err := m.deployClientPods(); err != nil {
		return nil, err
	}

	// start envoy resource gatherer
	if err := m.envoyResourceGather(); err != nil {
		return nil, err
	}

	m.isRunning = true
	return nil, nil
}

func (m *NetworkPolicySoakMeasurement) initialize(config *measurement.Config) error {
	// initialization
	m.k8sClient = config.ClusterFramework.GetClientSets().GetClient()
	m.framework = config.ClusterFramework

	namespaceList, err := m.k8sClient.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("phase: start, %s: failed to list namespaces: %v", m.String(), err)
	}

	// target namespaces are automanagered by the framework
	// capture all the target namespaces
	targetNamespacePrefix := m.framework.GetAutomanagedNamespacePrefix()
	for _, ns := range namespaceList.Items {
		if strings.HasPrefix(ns.Name, targetNamespacePrefix) {
			m.targetNamespaces = append(m.targetNamespaces, ns.Name)
		}
	}

	if len(m.targetNamespaces) == 0 {
		return fmt.Errorf("phase: start, %s: no target namespaces found, verify config", m.String())
	}

	// parse the config params
	if m.targetLabelKey, err = util.GetString(config.Params, "targetLabelKey"); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get target label key: %v", m.String(), err)
	}

	if m.targetLabelVal, err = util.GetString(config.Params, "targetLabelValue"); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get target label value: %v", m.String(), err)
	}

	if m.clientLabelKey, err = util.GetString(config.Params, "clientLabelKey"); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get client label key: %v", m.String(), err)
	}

	if m.clientLabelVal, err = util.GetString(config.Params, "clientLabelValue"); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get client label value: %v", m.String(), err)
	}

	if m.targetReplicasPerNs, err = util.GetIntOrDefault(config.Params, "targetReplicasPerNs", 1); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get target replicas per namespace: %v", m.String(), err)
	}

	if m.clientReplicasPerDep, err = util.GetIntOrDefault(config.Params, "clientReplicasPerDep", 1); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get client replicas per deployment: %v", m.String(), err)
	}

	if m.targetPort, err = util.GetIntOrDefault(config.Params, "targetPort", 80); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get target port: %v", m.String(), err)
	}

	if m.targetPath, err = util.GetStringOrDefault(config.Params, "targetPath", "/"); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get target path: %v", m.String(), err)
	}

	if m.testDuration, err = util.GetDuration(config.Params, "testDuration"); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get test duration: %v", m.String(), err)
	}

	if m.workerPerClient, err = util.GetIntOrDefault(config.Params, "workerPerClient", 1); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get worker per client: %v", m.String(), err)
	}

	if m.npType, err = util.GetStringOrDefault(config.Params, "npType", "k8s"); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get network policy type: %v", m.String(), err)
	}

	return nil
}

func (m *NetworkPolicySoakMeasurement) deployRBACResources() error {
	templateMap := map[string]interface{}{
		"Name":      clientName,
		"Namespace": clientNamespace,
	}

	// create the service account
	if err := m.framework.ApplyTemplatedManifests(manifestsFS, serviceAccountFilePath, templateMap); err != nil {
		return fmt.Errorf("phase: start, %s: failed to apply service account manifest: %v", m.String(), err)
	}

	// create the cluster role
	if err := m.framework.ApplyTemplatedManifests(manifestsFS, clusterRoleFilePath, templateMap); err != nil {
		return fmt.Errorf("phase: start, %s: failed to apply cluster role manifest: %v", m.String(), err)
	}

	// create the cluster role binding
	if err := m.framework.ApplyTemplatedManifests(manifestsFS, clusterRoleBindingFilePath, templateMap); err != nil {
		return fmt.Errorf("phase: start, %s: failed to apply cluster role binding manifest: %v", m.String(), err)
	}

	return nil
}

func (m *NetworkPolicySoakMeasurement) deployTargetPods() error {
	templateMap := map[string]interface{}{
		"TargetName":       targetName,
		"TargetLabelKey":   m.targetLabelKey,
		"TargetLabelValue": m.targetLabelVal,
		"Replicas":         m.targetReplicasPerNs,
		"TargetPort":       m.targetPort,
	}

	depBatchSize := 50
	for i := 0; i < len(m.targetNamespaces); i += depBatchSize {
		end := i + depBatchSize
		if end > len(m.targetNamespaces) {
			end = len(m.targetNamespaces)
		}
		for _, ns := range m.targetNamespaces[i:end] {
			templateMap["TargetNamespace"] = ns
			if err := m.framework.ApplyTemplatedManifests(manifestsFS, targetFilePath, templateMap); err != nil {
				return fmt.Errorf("phase: start, %s NS: %s, failed to apply target deployment manifest: %v", m.String(), ns, err)
			}
		}

		// Wait for the current batch deployments to be ready.
		labelSelector := fmt.Sprintf("%s=%s", m.targetLabelKey, m.targetLabelVal)
		batchPodCount := (end - i) * m.targetReplicasPerNs
		waitDuration := math.Max(60.0, float64(batchPodCount)*0.5)
		targetWaitCtx, targetWaitCancel := context.WithTimeout(context.TODO(), time.Duration(waitDuration)*time.Second)
		// desired pod count is the number of deployments until now * replicas per deployment
		desiredPodCount := end * m.targetReplicasPerNs
		if err := m.waitForDeploymentPodsReady(targetWaitCtx, desiredPodCount, labelSelector); err != nil {
			klog.Warningf("phase: start, %s: failed to wait for target pods to be ready: %v", m.String(), err)
		}
		targetWaitCancel() // Explicitly cancel the context immediately after waiting.
	}

	return nil
}

func (m *NetworkPolicySoakMeasurement) deployAPIServerNetworkPolicy() error {

	if policy, err := m.k8sClient.NetworkingV1().NetworkPolicies(clientNamespace).Get(context.TODO(), apiserverNPName, metav1.GetOptions{}); err == nil && policy != nil {
		// network policy already exists
		klog.Warningf("Network policy %s already exists, skipping deployment", apiserverNPName)
		return nil
	}

	// get the API server IP address
	var kubeAPIServerIP string
	if endpoints, err := m.k8sClient.CoreV1().Endpoints(api_corev1.NamespaceDefault).Get(context.TODO(), "kubernetes", metav1.GetOptions{}); err != nil {
		return fmt.Errorf("phase: start, %s: failed to get API server endpoint: %v", m.String(), err)
	} else {
		if len(endpoints.Subsets) == 0 || len(endpoints.Subsets[0].Addresses) == 0 {
			return fmt.Errorf("phase: start, %s: failed to get API server IP address", m.String())
		}
		kubeAPIServerIP = endpoints.Subsets[0].Addresses[0].IP
	}

	templateMap := map[string]interface{}{
		"Name":             apiserverNPName,
		"Namespace":        clientNamespace,
		"ClientLabelKey":   m.clientLabelKey,
		"ClientLabelValue": m.clientLabelVal,
		"KubeAPIServerIP":  kubeAPIServerIP,
	}

	if err := m.framework.ApplyTemplatedManifests(manifestsFS, APIserverFilePath, templateMap); err != nil {
		return fmt.Errorf("phase: start, %s: failed to apply API server network policy manifest: %v", m.String(), err)
	}
	return nil
}

func (m *NetworkPolicySoakMeasurement) deployNetworkPolicy() error {

	templateMap := map[string]interface{}{
		"ClientNamespace":    clientNamespace,
		"ClientLabelKey":     m.clientLabelKey,
		"ClientLabelValue":   m.clientLabelVal,
		"TargetLabelKey":     m.targetLabelKey,
		"TargetLabelValue":   m.targetLabelVal,
		"TargetPort":         strconv.Itoa(m.targetPort),
		"TargetPath":         m.targetPath,
		"NetworkPolicy_Type": m.npType,
	}

	for _, ns := range m.targetNamespaces {
		templateMap["TargetNamespace"] = ns
		templateMap["Name"] = ns // use the target namespace name as the network policy name

		if err := m.framework.ApplyTemplatedManifests(manifestsFS, netPolFilePath, templateMap); err != nil {
			return fmt.Errorf("phase: start, %s NS: %s, failed to apply network policy manifest: %v", m.String(), err, ns)
		}
	}
	return nil
}

func (m *NetworkPolicySoakMeasurement) deployClientPods() error {
	// Usually server/target pods replicas are not large, so they should be up and running in a short time
	klog.Infof("Deploying client pods")

	// convert the test duration to seconds
	duration := int(m.testDuration.Seconds())

	templateMap := map[string]interface{}{
		"ClientName":       clientName,
		"ClientNamespace":  clientNamespace,
		"ClientLabelKey":   m.clientLabelKey,
		"ClientLabelValue": m.clientLabelVal,
		"TargetLabelKey":   m.targetLabelKey,
		"TargetLabelValue": m.targetLabelVal,
		"TargetPort":       m.targetPort,
		"TargetPath":       m.targetPath,
		"Duration":         duration,
		"Replicas":         m.clientReplicasPerDep,
		"Workers":          m.workerPerClient,
	}

	clientBatchSize := 50
	for i := 0; i < len(m.targetNamespaces); i += clientBatchSize {
		end := i + clientBatchSize
		if end > len(m.targetNamespaces) {
			end = len(m.targetNamespaces)
		}
		for _, ns := range m.targetNamespaces[i:end] {
			templateMap["TargetNamespace"] = ns
			templateMap["UniqueName"] = ns // use target namespace name as unique name
			if err := m.framework.ApplyTemplatedManifests(manifestsFS, clientFilePath, templateMap); err != nil {
				return fmt.Errorf("phase: start, %s NS: %s, failed to apply client deployment manifest: %v", m.String(), ns, err)
			}
		}

		// Wait for the current batch client pods to be ready.
		labelSelector := fmt.Sprintf("%s=%s", m.clientLabelKey, m.clientLabelVal)
		batchPodCount := (end - i) * m.clientReplicasPerDep
		waitDuration := math.Max(60.0, float64(batchPodCount)*0.5)
		clientWaitCtx, clientWaitCancel := context.WithTimeout(context.TODO(), time.Duration(waitDuration)*time.Second)
		// desired pod count is the number of deployments until now * replicas per deployment
		desiredPodCount := end * m.clientReplicasPerDep
		if err := m.waitForDeploymentPodsReady(clientWaitCtx, desiredPodCount, labelSelector); err != nil {
			klog.Warningf("phase: start, %s: failed to wait for client pods to be ready: %v", m.String(), err)
		}
		clientWaitCancel() // cancel context immediately after waiting
	}

	m.testEndTime = time.Now().Add(m.testDuration)
	return nil
}

// Wait for the deployment pods be to be ready
func (m *NetworkPolicySoakMeasurement) waitForDeploymentPodsReady(ctx context.Context, desiredPodCount int, labelSelector string) error {
	// get the selector for the pods
	selector := util.NewObjectSelector()
	if labelSelector == "" {
		return fmt.Errorf("label selector is empty")
	}
	selector.LabelSelector = labelSelector

	options := &measurementutil.WaitForPodOptions{
		DesiredPodCount:     func() int { return desiredPodCount },
		CallerName:          m.String(),
		WaitForPodsInterval: 2 * time.Second,
	}

	podStore, err := measurementutil.NewPodStore(m.k8sClient, selector)
	if err != nil {
		return err
	}

	_, err = measurementutil.WaitForPods(ctx, podStore, options)
	if err != nil {
		return err
	}

	return nil
}

func (m *NetworkPolicySoakMeasurement) gather() ([]measurement.Summary, error) {
	if !m.isRunning {
		return nil, fmt.Errorf("phase: gather, %s: measurement not running", m.String())
	}

	// wait for the test to complete
	klog.Infof("phase: gather, %s: waiting for the test run to complete...", m.String())
	time.Sleep(time.Until(m.testEndTime))
	klog.Infof("phase: gather, %s: test run completed", m.String())

	// stop gathering resource usage
	if m.gatherers == nil {
		klog.Warningf("phase: gather, %s: gatherer not initialized. Envoy resource usage not collected", m.String())
	}

	// stop gathering resource usage
	summary, err := m.gatherers.StopAndSummarize([]int{50, 90, 99, 100})
	if err != nil {
		return nil, fmt.Errorf("phase: gather, %s: failed to stop gathering resource usage: %v", m.String(), err)
	}

	content, err := util.PrettyPrintJSON(summary)
	if err != nil {
		return nil, fmt.Errorf("phase: gather, %s: failed to pretty print resource usage summary: %v", m.String(), err)
	}

	resourceSummary := measurement.CreateSummary(networkPolicySoakMeasurementName, "json", content)
	return []measurement.Summary{resourceSummary}, nil
}

func (m *NetworkPolicySoakMeasurement) envoyResourceGather() error {
	if m.gatherers != nil {
		return fmt.Errorf("phase: gather, %s: resource gatherer already initialized, not expected", m.String())
	}

	// api server IP address
	host := m.framework.GetClusterConfig().GetMasterIP()

	// namespace
	namespace := "kube-system"

	// label selector
	labelSelector := "name=cilium-envoy"

	// resource gatherer options
	options := gatherers.ResourceGathererOptions{
		InKubemark:                        m.framework.GetClusterConfig().Provider.Features().IsKubemarkProvider,
		ResourceDataGatheringPeriod:       60 * time.Second,
		MasterResourceDataGatheringPeriod: 60 * time.Second,
		Nodes:                             gatherers.AllNodes,
	}

	gatherers, err := gatherers.NewResourceUsageGatherer(m.k8sClient,
		host,
		m.framework.GetClusterConfig().KubeletPort,
		m.framework.GetClusterConfig().Provider,
		options,
		namespace,
		labelSelector)
	if err != nil {
		return fmt.Errorf("phase: gather, %s: failed to create resource gatherer: %v", m.String(), err)
	}
	m.gatherers = gatherers

	// start gathering resource usage
	go m.gatherers.StartGatheringData()

	return nil
}

func (m *NetworkPolicySoakMeasurement) Dispose() {
	// delete RBAC resources
	if err := m.k8sClient.RbacV1().ClusterRoleBindings().Delete(context.TODO(), fmt.Sprintf("%s-crb", clientName), metav1.DeleteOptions{}); err != nil {
		klog.Errorf("phase: gather, %s: failed to delete cluster role binding: %v", m.String(), err)
	}

	if err := m.k8sClient.RbacV1().ClusterRoles().Delete(context.TODO(), fmt.Sprintf("%s-cr", clientName), metav1.DeleteOptions{}); err != nil {
		klog.Errorf("phase: gather, %s: failed to delete cluster role: %v", m.String(), err)
	}

	if err := m.k8sClient.CoreV1().ServiceAccounts(clientNamespace).Delete(context.TODO(), fmt.Sprintf("%s-sa", clientName), metav1.DeleteOptions{}); err != nil {
		klog.Errorf("phase: gather, %s: failed to delete service account: %v", m.String(), err)
	}

	// Define the GVR for CiliumClusterwideNetworkPolicy
	cnpGVR := schema.GroupVersionResource{
		Group:    "cilium.io",
		Version:  "v2",
		Resource: "ciliumnetworkpolicies",
	}
	// delete cilium network policies in all the client namespaces
	dynamicClient := m.framework.GetDynamicClients().GetClient()
	if err := dynamicClient.Resource(cnpGVR).Namespace(clientNamespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{}); err != nil {
		klog.Errorf("phase: gather, %s: failed to delete cilium network policies: %v", m.String(), err)
	}

	// delete client pods
	if err := m.k8sClient.AppsV1().Deployments(clientNamespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{}); err != nil {
		klog.Errorf("phase: gather, %s: failed to delete client deployments: %v", m.String(), err)
	}

	// delte client namespace
	if err := m.k8sClient.CoreV1().Namespaces().Delete(context.TODO(), clientNamespace, metav1.DeleteOptions{}); err != nil {
		klog.Errorf("phase: gather, %s: failed to delete namespace %s: %v", m.String(), clientNamespace, err)
	}

	// clear target deployments from all the target namespaces using label selector
	labelSelector := fmt.Sprintf("%s=%s", m.targetLabelKey, m.targetLabelVal)
	for _, ns := range m.targetNamespaces {
		if err := m.k8sClient.AppsV1().Deployments(ns).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: labelSelector}); err != nil {
			klog.Errorf("phase: gather, %s NS: %s, failed to delete target deployments: %v", m.String(), ns, err)
		}
	}

	// stop gatherers
	if m.gatherers != nil {
		m.gatherers.Dispose()
	}
}

func (m *NetworkPolicySoakMeasurement) String() string {
	return networkPolicySoakMeasurementName
}
