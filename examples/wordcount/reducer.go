// Package main implements a WordCount reducer plugin.
// This reducer sums up all the counts for each word.
//
//go:build plugin

package main

import (
	"strconv"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// ReducerImpl implements the Reducer interface for word counting.
type ReducerImpl struct{}

// Reduce sums all values for the given key (word) and returns the total count.
func (r *ReducerImpl) Reduce(key string, values []string) ([]plugin.Record, error) {
	total := 0
	for _, v := range values {
		count, err := strconv.Atoi(v)
		if err != nil {
			count = 1 // Treat invalid values as 1
		}
		total += count
	}

	return []plugin.Record{
		{Key: key, Value: strconv.Itoa(total)},
	}, nil
}

// Reducer is the exported symbol that the worker loads.
var Reducer plugin.Reducer = &ReducerImpl{}
