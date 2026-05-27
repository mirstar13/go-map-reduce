//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// ReducerImpl implements the Reducer interface for graph preprocessing.
// It groups neighbors for each node and assigns an initial rank and distance.
type ReducerImpl struct{}

func (r *ReducerImpl) Reduce(key string, values []string) ([]plugin.Record, error) {
	// Use a map to ensure unique neighbors
	neighborMap := make(map[string]struct{})
	for _, v := range values {
		if v != "" {
			neighborMap[v] = struct{}{}
		}
	}

	neighborList := make([]string, 0, len(neighborMap))
	for n := range neighborMap {
		neighborList = append(neighborList, n)
	}

	// Format: rank|distance|label|neighbors
	// Initial rank: 1.0
	// Initial distance: INF
	// Initial label: key
	//
	// This format is compatible with PageRank, BFS, and Connected Components algorithms.
	val := "1.0|INF|" + key + "|" + strings.Join(neighborList, ",")

	return []plugin.Record{
		{Key: key, Value: val},
	}, nil
}

// Reducer is the exported symbol that the worker loads.
var Reducer plugin.Reducer = &ReducerImpl{}
