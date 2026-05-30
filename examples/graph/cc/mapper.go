//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// MapperImpl implements the Mapper interface for Connected Components (Label Propagation).
type MapperImpl struct{}

func (m *MapperImpl) Map(inputs []plugin.MapInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		// Value format: "rank|distance|label|neighbor1,neighbor2,..."
		parts := strings.SplitN(input.Value, "|", 4)
		if len(parts) < 4 {
			continue
		}

		rankStr := parts[0]
		distStr := parts[1]
		labelStr := parts[2]
		neighborsStr := parts[3]

		neighbors := strings.Split(neighborsStr, ",")
		if neighborsStr == "" {
			neighbors = []string{}
		}

		// 1. Emit current label to all neighbors
		for _, n := range neighbors {
			if n != "" {
				records = append(records, plugin.Record{
					Key:   n,
					Value: labelStr,
				})
			}
		}

		// 2. Preserve structure and current label
		// Prefix "S:" for rank, distance, and neighbors, "L:" for current label
		records = append(records, plugin.Record{
			Key:   input.Key,
			Value: "S:" + rankStr + "|" + distStr + "|" + neighborsStr,
		})
		records = append(records, plugin.Record{
			Key:   input.Key,
			Value: "L:" + labelStr,
		})
	}

	return records, nil
}

func (m *MapperImpl) Combine(key string, values []string) ([]plugin.Record, error) {
	minLabel := ""
	var structure string

	for _, v := range values {
		if strings.HasPrefix(v, "S:") {
			structure = v
		} else {
			label := v
			if strings.HasPrefix(v, "L:") {
				label = v[2:]
			}

			if minLabel == "" || len(label) < len(minLabel) || (len(label) == len(minLabel) && label < minLabel) {
				minLabel = label
			}
		}
	}

	var records []plugin.Record
	if structure != "" {
		records = append(records, plugin.Record{Key: key, Value: structure})
	}
	if minLabel != "" {
		records = append(records, plugin.Record{Key: key, Value: "L:" + minLabel})
	}
	return records, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper interface {
	plugin.Mapper
	plugin.Combiner
} = &MapperImpl{}
