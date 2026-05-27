//go:build plugin

package main

import (
	"fmt"
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// ReducerImpl implements the Reducer interface for PageRank.
type ReducerImpl struct{}

const damping = 0.85

func (r *ReducerImpl) Reduce(key string, values []string) ([]plugin.Record, error) {
	var totalRank float64
	var structure string

	for _, v := range values {
		if strings.HasPrefix(v, "S:") {
			structure = v[2:]
		} else {
			var rank float64
			_, err := fmt.Sscanf(v, "%f", &rank)
			if err == nil {
				totalRank += rank
			}
		}
	}

	// PageRank formula: (1-d) + d * sum(incoming_ranks)
	newRank := (1.0 - damping) + damping*totalRank

	// Output format: rank|distance|neighbors
	// structure already contains "distance|neighbors"
	return []plugin.Record{
		{Key: key, Value: fmt.Sprintf("%f|%s", newRank, structure)},
	}, nil
}

// Reducer is the exported symbol that the worker loads.
var Reducer plugin.Reducer = &ReducerImpl{}
