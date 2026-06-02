package main

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/services/ui/metrics"
	"github.com/stretchr/testify/assert"
)

type mockMetricsClient struct{}

func (m *mockMetricsClient) GetClusterMetrics(ctx context.Context) (metrics.ClusterMetrics, error) {
	return metrics.ClusterMetrics{}, nil
}

func TestInitialModel(t *testing.T) {
	c := client.New("http://localhost:8080", "test-token")
	mc := &mockMetricsClient{}
	m := initialModel(c, mc)
	assert.Equal(t, 0, len(m.jobs))
	assert.False(t, m.quitting)
	assert.NotNil(t, m.client)
	assert.NotNil(t, m.MetricsClient) 
	assert.Equal(t, globalStatusPanel, m.focused) 
}

func TestUpdateJobs(t *testing.T) {
	c := client.New("http://localhost:8080", "test-token")
	mc := &mockMetricsClient{}
	m := initialModel(c, mc)


	jobID := uuid.New()
	jobs := []db.Job{
		{JobID: jobID, Status: "running"},
	}

	newModel, cmd := m.Update(jobsMsg(jobs))
	m = newModel.(model)

	assert.Nil(t, cmd)
	assert.Equal(t, 1, len(m.jobs))
	assert.Equal(t, jobID, m.jobs[0].JobID)
	assert.Equal(t, 1, len(m.jobList.Items()))
	assert.Equal(t, jobID.String(), m.jobList.Items()[0].(item).job.JobID.String())
}
