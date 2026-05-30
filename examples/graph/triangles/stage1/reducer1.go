//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// Reducer1Impl groups all neighbors for a node.
type Reducer1Impl struct{}

func (r *Reducer1Impl) Reduce(inputs []plugin.ReduceInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
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

		records = append(records, plugin.Record{
			Key:   input.Key,
			Value: strings.Join(neighborList, ","),
		})
	}

	return records, nil
}

var Reducer plugin.Reducer = &Reducer1Impl{}
