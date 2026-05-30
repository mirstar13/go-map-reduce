//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// Mapper1Impl implements the first stage of Triangle Counting.
// It emits (u, v) and (v, u) for every edge to build a full adjacency list.
type Mapper1Impl struct{}

func (m *Mapper1Impl) Combine(key string, values []string) ([]plugin.Record, error) {
	neighborMap := make(map[string]struct{})
	for _, v := range values {
		if v != "" {
			neighborMap[v] = struct{}{}
		}
	}

	results := make([]plugin.Record, 0, len(neighborMap))
	for n := range neighborMap {
		results = append(results, plugin.Record{Key: key, Value: n})
	}
	return results, nil
}

func (m *Mapper1Impl) Map(inputs []plugin.MapInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		trimmed := strings.TrimSpace(input.Value)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		fields := strings.Fields(trimmed)
		if len(fields) < 2 {
			continue
		}

		u, v := fields[0], fields[1]
		if u == v {
			continue // Ignore self-loops
		}

		records = append(records, plugin.Record{Key: u, Value: v})
		records = append(records, plugin.Record{Key: v, Value: u})
	}

	return records, nil
}

var Mapper interface {
	plugin.Mapper
	plugin.Combiner
} = &Mapper1Impl{}
