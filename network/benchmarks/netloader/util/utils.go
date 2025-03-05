package util

import (
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog"
)

// Iplookup - thread safe ip address lookup
type Iplookup struct {
	IpList []string
	lock   sync.Mutex
}

// NewIplookup
func NewIplookup(IpList []string) *Iplookup {
	return &Iplookup{
		IpList: IpList,
	}
}

// GetIp
func (i *Iplookup) GetIp() string {
	// get the lock to make this thread safe
	i.lock.Lock()
	defer i.lock.Unlock()
	// get the first ip from the list
	// and move it to the end
	ip := i.IpList[0]
	i.IpList = append(i.IpList[1:], ip)
	return ip
}

// get pod ip addresses from list of pods
func GetPodIPs(podList *corev1.PodList) []string {
	var podIPs []string
	for _, pod := range podList.Items {
		// if pod is not ready, or ip is empty, skip
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			klog.Warningf("Pod %s is not ready, skipping", pod.Name)
			continue
		}
		podIPs = append(podIPs, pod.Status.PodIP)
	}
	return podIPs
}
