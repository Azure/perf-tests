package client

import (
	"context"
	"crypto/tls"
	"flag"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/http2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
	"k8s.io/perf-tests/network/benchmarks/netloader/pkg/metrics"
	utils "k8s.io/perf-tests/network/benchmarks/netloader/util"
)

type TestClient struct {
	dest_labelSelector string
	namespace          string
	duration           time.Duration
	interval           time.Duration
	concurrentThreads  int
	destPort           int
	destPath           string
	stopChan           chan os.Signal
	HttpMetrics        *HttpMetrics
	iplookup           *utils.Iplookup
	httpClient         *http.Client
}

func NewTestClient(stopCh chan os.Signal) (*TestClient, error) {
	client := &TestClient{
		stopChan:    stopCh,
		HttpMetrics: NewHttpMetrics(),
		httpClient: &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
		},
	}
	return client, nil
}

// parse command line arguments
func (c *TestClient) parse() error {
	var dest_labelSelector string
	var namespace string
	var duration int
	var interval int
	var concurrentThreads int
	var destPort int
	var destPath string

	flag.StringVar(&dest_labelSelector, "dest_labelSelector", "app=target", "destination label selector")
	flag.StringVar(&namespace, "namespace", "default", "namespace")
	flag.IntVar(&duration, "duration", 60, "duration of the run in seconds")
	flag.IntVar(&interval, "interval", 1, "interval between requests")
	flag.IntVar(&concurrentThreads, "workers", 10, "number of concurrent threads")
	flag.IntVar(&destPort, "destPort", 80, "destination port number")
	flag.StringVar(&destPath, "destPath", "/", "destination path")

	flag.Parse()

	c.dest_labelSelector = dest_labelSelector
	c.namespace = namespace
	c.duration = time.Duration(duration) * time.Second
	c.interval = time.Duration(interval) * time.Second
	c.concurrentThreads = concurrentThreads
	c.destPort = destPort
	c.destPath = destPath

	klog.Infof("dest_labelSelector: %s, namespace: %s, duration: %v, interval: %v, concurrentThreads: %d, destPort: %d, destPath: %s", c.dest_labelSelector, c.namespace, c.duration, c.interval, c.concurrentThreads, c.destPort, c.destPath)
	return nil
}

func (c *TestClient) Run() {
	// parse command line arguments
	_ = c.parse()

	// start metrics server
	go func() {
		klog.Info("Starting metrics server")

		if err := metrics.StartMetricsServer("8080", "/metrics"); err != nil {
			klog.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	// start the test
	c.startTest()

	// Wait for the stop signal
	klog.Info("Going to Idlestate")
	<-c.stopChan
	klog.Info("Received stop signal")
}

func (c *TestClient) startTest() {
	// get k8s config
	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("Failed to get k8s config: %v", err)
	}

	// create k8s client
	k8sClient, err := clientset.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create k8s client: %v", err)
	}

	// get the list of pods
	podList, err := k8sClient.CoreV1().Pods(c.namespace).List(context.Background(), metav1.ListOptions{LabelSelector: c.dest_labelSelector, Limit: 128})
	if err != nil {
		klog.Fatalf("Failed to get pod list: %v", err)
	}

	// get the list of pod IPs
	podIps := utils.GetPodIPs(podList)
	if len(podIps) == 0 {
		klog.Fatalf("No pods found with label selector %s", c.dest_labelSelector)
	}

	c.iplookup = utils.NewIplookup(podIps)
	ctx, cancel := context.WithTimeout(context.Background(), c.duration)
	defer cancel()

	// new common channel to send ip addresses to workers
	ipChan := make(chan string, c.concurrentThreads)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(c.interval).C
		for {
			select {
			case <-ticker:
				ip := c.iplookup.GetIp()
				// broadcast ip to all workers
				for i := 0; i < c.concurrentThreads; i++ {
					select {
					case ipChan <- ip:
					default:
						// skip if channel is full
					}
				}
			case <-ctx.Done():
				klog.Info("Load duration expired, stopping ticker")
				return
			}
		}
	}()

	for i := 0; i < c.concurrentThreads; i++ {
		wg.Add(1)
		// pass ipChan directly to worker
		go c.worker(&wg, ctx, ipChan)
	}

	wg.Wait()
}

// update worker signature to receive string
func (c *TestClient) worker(wg *sync.WaitGroup, ctx context.Context, ipChan <-chan string) {
	defer wg.Done()
	destPort := strconv.Itoa(c.destPort)
	for {
		select {
		case <-ctx.Done():
			klog.Info("Load duration expired, stopping worker")
			return
		case ip := <-ipChan:
			// url
			url := "http://" + ip + ":" + destPort + c.destPath

			// start time
			start := time.Now()

			// make the http request
			resp, err := c.httpClient.Get(url)
			if err != nil {
				klog.Errorf("http request failed: %v", err)
				// Inc total and fail counters
				c.HttpMetrics.requestsTotal.WithLabelValues("total").Inc()
				c.HttpMetrics.requestsFail.WithLabelValues("fail").Inc()
				continue
			}

			// record the latency - optional via env var
			if os.Getenv("RECORD_LATENCY") == "true" {
				latency := time.Since(start).Seconds()
				c.HttpMetrics.latencies.WithLabelValues(ip).Observe(latency)
			}

			// Inc total and success counters
			c.HttpMetrics.requestsTotal.WithLabelValues("total").Inc()
			c.HttpMetrics.requestsSuccess.WithLabelValues("success").Inc()
			resp.Body.Close()
		}
	}
}
