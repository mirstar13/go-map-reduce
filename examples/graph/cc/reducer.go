//go:build plugin

package main

import (
	"fmt"
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// ReducerImpl implements the Reducer interface for Connected Components.
type ReducerImpl struct{}

func (r *ReducerImpl) Reduce(inputs []plugin.ReduceInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		minLabel := ""
		var structure string

		for _, v := range input.Values {
			if strings.HasPrefix(v, "S:") {
				structure = v[2:]
			} else {
				label := v
				if strings.HasPrefix(v, "L:") {
					label = v[2:]
				}

				// Numeric string comparison: shorter strings are smaller numbers,
				// same length strings compared lexicographically.
				if minLabel == "" || len(label) < len(minLabel) || (len(label) == len(minLabel) && label < minLabel) {
					minLabel = label
				}
			}
		}

		// Output format: rank|distance|label|neighbors
		// structure: "rank|distance|neighbors"
		parts := strings.SplitN(structure, "|", 3)
		if len(parts) < 3 {
			continue
		}

		val := fmt.Sprintf("%s|%s|%s|%s", parts[0], parts[1], minLabel, parts[2])

		records = append(records, plugin.Record{
			Key:   input.Key,
			Value: val,
		})
	}

	return records, nil
}

// Reducer is the exported symbol that the worker loads.
var Reducer plugin.Reducer = &ReducerImpl{}
