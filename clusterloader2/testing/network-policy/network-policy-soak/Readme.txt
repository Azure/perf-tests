# test setup details - 
# All the client pods will be deployed in single test namespace
# Server pods will be spread accross different namespaces
# clients, servers, NPs are grouped
# each group will have
    * M clients - 1 deployment yaml, M replicas, in test namespace
    * N servers - 1 deployment yaml, M replicas,  in Server custom namespace
    * Network policy - 1 egress Network policy to allow M clients to N servers
# Number of groups = Number of Network Policies = Number of Server NS's   

# Command to run cilium l7 network policy soak, change the paths to config files below accordingly.
./clusterloader --provider=aks --kubeconfig=/home/sarathsa/.kube/config --testconfig=/home/sarathsa/work/perf-tests/clusterloader2/testing/network-policy/network-policy-soak/config.yaml --v=5 --enable-prometheus-server=True --prometheus-storage-class-provisioner=disk.csi.azure.com --prometheus-pvc-storage-class=default --report-dir=/tmp/load-test/network-policy/http-soak1 --testoverrides=/home/sarathsa/work/perf-tests/clusterloader2/testing/network-policy/network-policy-soak/overrides.yaml 2>&1 | tee /tmp/load-test/network-policy/http-soak1/logs.txt


# Test setup parameters are defined in overrides.yaml and the config for the test setup are defined in config.yaml

# Test code path - perf-tests/clusterloader2/pkg/measurement/common/network-policy/network-policy-soak/manifests
# yaml files for test objects are defined - perf-tests/clusterloader2/pkg/measurement/common/network-policy/network-policy-soak/manifests