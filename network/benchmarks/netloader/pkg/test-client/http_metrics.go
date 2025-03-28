package client

import (
	"github.com/prometheus/client_golang/prometheus"
)

// HttpMetrics
type HttpMetrics struct {
	requestsTotal   *prometheus.CounterVec
	requestsSuccess *prometheus.CounterVec
	requestsFail    *prometheus.CounterVec
	latencies       *prometheus.HistogramVec
}

// NewHttpMetrics
func NewHttpMetrics() *HttpMetrics {
	requestsTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests",
	}, []string{"count"})

	requestsSuccess := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_success",
		Help: "Total number of successful HTTP requests",
	}, []string{"count"})

	requestsFail := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_fail",
		Help: "Total number of failed HTTP requests",
	}, []string{"count"})

	latencies := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_latency_seconds",
		Help:    "HTTP request latency in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"endpoint"})

	// Register the metrics with Prometheus
	prometheus.MustRegister(requestsTotal, requestsSuccess, requestsFail, latencies)

	return &HttpMetrics{
		requestsTotal:   requestsTotal,
		requestsSuccess: requestsSuccess,
		requestsFail:    requestsFail,
		latencies:       latencies,
	}
}
