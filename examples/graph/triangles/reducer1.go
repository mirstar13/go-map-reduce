//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// Reducer1Impl groups all neighbors for a node.
type Reducer1Impl struct{}

func (r *Reducer1Impl) Reduce(key string, values []string) ([]plugin.Record, error) {
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

	return []plugin.Record{
		{Key: key, Value: strings.Join(neighborList, ",")},
	}, nil
}

var Reducer plugin.Reducer = &Reducer1Impl{}
