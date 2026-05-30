//go:build plugin

package main

import (
	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// Reducer2Impl detects triangles.
type Reducer2Impl struct{}

func (r *Reducer2Impl) Reduce(inputs []plugin.ReduceInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		exists := false
		var potentialNeighbors []string

		for _, v := range input.Values {
			if v == "EXIST" {
				exists = true
			} else {
				potentialNeighbors = append(potentialNeighbors, v)
			}
		}

		// A triangle exists only if the third edge (the key) is confirmed to EXIST
		if !exists || len(potentialNeighbors) == 0 {
			continue
		}

		// Key is "u,v". For every node 'w' in potentialNeighbors, (u, v, w) is a triangle.
		// Emit the confirmed triangles.
		for _, w := range potentialNeighbors {
			// Output triangle: u,v,w
			records = append(records, plugin.Record{
				Key:   input.Key + "," + w,
				Value: "1",
			})
		}
	}

	return records, nil
}

var Reducer plugin.Reducer = &Reducer2Impl{}
