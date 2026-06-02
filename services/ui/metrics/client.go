package metrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

type ClusterMetrics struct {
	ActiveNodes     int
	ActiveTasks     int
	ShuffleProgress float64
	MemoryUsage     float64
	RawData         string
}

type Client interface {
	GetClusterMetrics(ctx context.Context) (ClusterMetrics, error)
}

type metricsClient struct {
	httpClient *http.Client
	baseUrl    string
}

func NewClient(baseUrl string) Client {
	return &metricsClient{
		httpClient: &http.Client{},
		baseUrl:    baseUrl,
	}
}

func (c *metricsClient) GetClusterMetrics(ctx context.Context) (ClusterMetrics, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/metrics", c.baseUrl), nil)
	if err != nil {
		return ClusterMetrics{}, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return ClusterMetrics{}, fmt.Errorf("failed to perform request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ClusterMetrics{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ClusterMetrics{}, fmt.Errorf("failed to read response body: %w", err)
	}

	return ClusterMetrics{RawData: string(body)}, nil
}
