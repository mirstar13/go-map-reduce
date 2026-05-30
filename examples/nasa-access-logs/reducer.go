//go:build plugin

package main

import (
	"strconv"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// ReducerImpl implements the Reducer interface for NASA access logs.
type ReducerImpl struct{}

// Reduce sums the counts for each hour.
func (r *ReducerImpl) Reduce(inputs []plugin.ReduceInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		total := 0
		for _, v := range input.Values {
			count, err := strconv.Atoi(v)
			if err != nil {
				count = 1
			}
			total += count
		}
		
		records = append(records, plugin.Record{
			Key:   input.Key,
			Value: strconv.Itoa(total),
		})
	}

	return records, nil
}

// Reducer is the exported symbol that the worker loads.
var Reducer plugin.Reducer = &ReducerImpl{}
