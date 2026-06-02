package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"

	"github.com/mirstar13/go-map-reduce/pkg/metrics"
)

func TestMetricsEndpoint(t *testing.T) {
	app := fiber.New()
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	// Increment a metric to ensure it shows up with a non-zero value
	metrics.JobsTotal.WithLabelValues("COMPLETED").Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	bodyStr := string(body)
	require.Contains(t, bodyStr, "mapreduce_manager_jobs_total")
	require.Contains(t, bodyStr, `mapreduce_manager_jobs_total{status="COMPLETED"} 1`)
}

func TestJobDurationMetric(t *testing.T) {
	app := fiber.New()
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	// Observe a duration
	metrics.JobDuration.WithLabelValues("FAILED").Observe(42.5)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	bodyStr := string(body)
	require.Contains(t, bodyStr, "mapreduce_manager_job_duration_seconds")
	require.Contains(t, bodyStr, `mapreduce_manager_job_duration_seconds_count{status="FAILED"} 1`)
	require.Contains(t, bodyStr, `mapreduce_manager_job_duration_seconds_sum{status="FAILED"} 42.5`)
}
