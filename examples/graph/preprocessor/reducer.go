//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// ReducerImpl implements the Reducer interface for graph preprocessing.
// It groups neighbors for each node and assigns an initial rank and distance.
type ReducerImpl struct{}

func (r *ReducerImpl) Reduce(inputs []plugin.ReduceInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		// Use a map to ensure unique neighbors
		neighborMap := make(map[string]struct{})
		for _, v := range input.Values {
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
		val := "1.0|INF|" + input.Key + "|" + strings.Join(neighborList, ",")

		records = append(records, plugin.Record{
			Key:   input.Key,
			Value: val,
		})
	}

	return records, nil
}

// Reducer is the exported symbol that the worker loads.
var Reducer plugin.Reducer = &ReducerImpl{}
