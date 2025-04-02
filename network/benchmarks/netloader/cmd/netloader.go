package main

// simple http load gen tool - takes arguements for target label selectors and namespace, duration of the run, number of concurrent threads, dest port number please

import (
	"os"
	"os/signal"
	"syscall"

	"k8s.io/klog"
	client "k8s.io/perf-tests/network/benchmarks/netloader/pkg/test-client"
)

// main function
func main() {
	klog.Info("Starting netloader")
	defer klog.Info("Shutting down netloader")

	// main stop channel
	mainStopCh := make(chan os.Signal, 1)
	testClient, err := client.NewTestClient(mainStopCh)
	if err != nil {
		klog.Fatalf("Failed to create test client: %v", err)
	}

	signal.Notify(mainStopCh, syscall.SIGINT, syscall.SIGTERM)
	testClient.Run()
}
