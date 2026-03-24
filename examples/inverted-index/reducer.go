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

// Reduce collects all document IDs for a word and returns a sorted list.
func (r *ReducerImpl) Reduce(key string, values []string) ([]plugin.Record, error) {
	// Remove duplicates
	seen := make(map[string]bool)
	var uniqueDocs []string
	for _, docID := range values {
		if !seen[docID] {
			seen[docID] = true
			uniqueDocs = append(uniqueDocs, docID)
		}
	}

	// Sort document IDs for consistent output
	sort.Strings(uniqueDocs)

	return []plugin.Record{
		{Key: key, Value: strings.Join(uniqueDocs, ",")},
	}, nil
}

// Reducer is the exported symbol that the worker loads.
var Reducer plugin.Reducer = &ReducerImpl{}
