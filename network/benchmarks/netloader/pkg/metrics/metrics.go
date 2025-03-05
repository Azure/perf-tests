package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// start metrics server
func StartMetricsServer(port, path string) error {
	// start metrics server
	http.Handle(path, promhttp.Handler())
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		return fmt.Errorf("failed to start metrics server: %v", err)
	}
	return nil
}
