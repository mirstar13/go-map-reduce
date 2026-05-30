package main

import (
	"testing"

	"github.com/google/uuid"
	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/mirstar13/go-map-reduce/db"
	"github.com/stretchr/testify/assert"
)

func TestInitialModel(t *testing.T) {
	c := client.New("http://localhost:8080", "test-token")
	m := initialModel(c)
	assert.Equal(t, 0, len(m.jobs))
	assert.False(t, m.quitting)
	assert.NotNil(t, m.client)
	assert.Equal(t, jobsPanel, m.focused)
}

func TestUpdateJobs(t *testing.T) {
	c := client.New("http://localhost:8080", "test-token")
	m := initialModel(c)

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
