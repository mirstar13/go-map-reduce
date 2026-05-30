// Package main implements an Inverted Index reducer plugin.
// This reducer collects all document IDs for each word.
//
//go:build plugin

package main

import (
	"sort"
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// ReducerImpl implements the Reducer interface for inverted index.
type ReducerImpl struct{}

// Reduce collects all document IDs for each word and returns sorted lists.
func (r *ReducerImpl) Reduce(inputs []plugin.ReduceInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		// Remove duplicates
		seen := make(map[string]bool)
		var uniqueDocs []string
		for _, docID := range input.Values {
			if !seen[docID] {
				seen[docID] = true
				uniqueDocs = append(uniqueDocs, docID)
			}
		}

		// Sort document IDs for consistent output
		sort.Strings(uniqueDocs)

		records = append(records, plugin.Record{
			Key:   input.Key,
			Value: strings.Join(uniqueDocs, ","),
		})
	}

	return records, nil
}

// Reducer is the exported symbol that the worker loads.
var Reducer plugin.Reducer = &ReducerImpl{}
