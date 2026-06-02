package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetClusterMetrics(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/metrics", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("# HELP my_metric A sample metric\n# TYPE my_metric counter\nmy_metric 123"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	metrics, err := client.GetClusterMetrics(context.Background())

	assert.NoError(t, err)
	assert.Contains(t, metrics.RawData, "my_metric 123")
}

func TestGetClusterMetrics_Error(t *testing.T) {
	// Mock server returning error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	_, err := client.GetClusterMetrics(context.Background())

	assert.Error(t, err)
}
