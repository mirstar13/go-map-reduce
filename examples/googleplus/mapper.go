//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// MapperImpl implements the Mapper interface for Google+ network.
type MapperImpl struct{}

// Map parses ego-network edges and emits (node, "1") for both ends of an edge.
func (m *MapperImpl) Map(inputs []plugin.MapInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		fields := strings.Fields(input.Value)
		if len(fields) < 2 {
			continue
		}

		u, v := fields[0], fields[1]
		records = append(records, plugin.Record{Key: u, Value: "1"})
		records = append(records, plugin.Record{Key: v, Value: "1"})
	}

	return records, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper interface {
	plugin.Mapper
} = &MapperImpl{}
