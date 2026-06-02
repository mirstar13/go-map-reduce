package shuffle

import (
	"sync"

	"github.com/google/uuid"
)

type Tracker struct {
	mu sync.RWMutex
	// jobID -> taskIndex -> nodeIP
	locations map[uuid.UUID]map[int32]string
}

func NewTracker() *Tracker {
	return &Tracker{
		locations: make(map[uuid.UUID]map[int32]string),
	}
}

func (t *Tracker) Register(jobID uuid.UUID, taskIndex int32, nodeIP string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.locations[jobID] == nil {
		t.locations[jobID] = make(map[int32]string)
	}
	t.locations[jobID][taskIndex] = nodeIP
}

func (t *Tracker) GetSourceNodes(jobID uuid.UUID) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	nodesMap := make(map[string]struct{})
	for _, nodeIP := range t.locations[jobID] {
		nodesMap[nodeIP] = struct{}{}
	}
	var nodes []string
	for nodeIP := range nodesMap {
		nodes = append(nodes, nodeIP)
	}
	return nodes
}
