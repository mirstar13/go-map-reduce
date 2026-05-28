//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// MapperImpl implements the Mapper interface for graph preprocessing.
// It parses edge lists (SRC DST [TIMESTAMP]) and emits (SRC, DST).
type MapperImpl struct {
	seenNodes map[string]struct{}
}

func (m *MapperImpl) Combine(key string, values []string) ([]plugin.Record, error) {
	neighborMap := make(map[string]struct{})
	hasEmpty := false
	for _, v := range values {
		if v != "" {
			neighborMap[v] = struct{}{}
		} else {
			hasEmpty = true
		}
	}

	results := make([]plugin.Record, 0, len(neighborMap)+1)
	if hasEmpty || len(neighborMap) == 0 {
		results = append(results, plugin.Record{Key: key, Value: ""})
	}
	for n := range neighborMap {
		results = append(results, plugin.Record{Key: key, Value: n})
	}
	return results, nil
}

func (m *MapperImpl) Map(key, value string) ([]plugin.Record, error) {
	// Skip comments or empty lines
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return nil, nil
	}

	// sx-stackoverflow and gplus edges format: SRC DST [TIMESTAMP]
	// Using strings.Fields handles multiple spaces or tabs
	fields := strings.Fields(trimmed)
	if len(fields) < 2 {
		return nil, nil
	}

	u, v := fields[0], fields[1]
	if u == "" || v == "" {
		return nil, nil
	}

	if m.seenNodes == nil {
		m.seenNodes = make(map[string]struct{})
	}

	records := []plugin.Record{
		{Key: u, Value: v},
	}

	// Only emit the empty destination node record if we haven't seen it yet
	// in this task. This prevents millions of redundant records.
	if _, seen := m.seenNodes[v]; !seen {
		records = append(records, plugin.Record{Key: v, Value: ""})
		m.seenNodes[v] = struct{}{}
	}
	
	// Also mark u as seen so if it appears as a destination later, we don't
	// need to emit the empty record (since we already have edges for it).
	m.seenNodes[u] = struct{}{}

	return records, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper interface {
	plugin.Mapper
	plugin.Combiner
} = &MapperImpl{}
