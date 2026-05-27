//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// MapperImpl implements the Mapper interface for Connected Components (Label Propagation).
type MapperImpl struct{}

func (m *MapperImpl) Map(key, value string) ([]plugin.Record, error) {
	// Value format: "rank|distance|label|neighbor1,neighbor2,..."
	parts := strings.SplitN(value, "|", 4)
	if len(parts) < 4 {
		return nil, nil
	}

	rankStr := parts[0]
	distStr := parts[1]
	labelStr := parts[2]
	neighborsStr := parts[3]

	neighbors := strings.Split(neighborsStr, ",")
	if neighborsStr == "" {
		neighbors = []string{}
	}

	var records []plugin.Record

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
		Key:   key,
		Value: "S:" + rankStr + "|" + distStr + "|" + neighborsStr,
	})
	records = append(records, plugin.Record{
		Key:   key,
		Value: "L:" + labelStr,
	})

	return records, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper plugin.Mapper = &MapperImpl{}
