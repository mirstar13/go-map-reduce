//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// MapperImpl implements the Mapper interface for Stack Overflow interactions.
type MapperImpl struct{}

// Map parses directed edges (SRC DST TIMESTAMP) and emits the destination (receiver of help).
// This treats each interaction as a "reputation point" for the receiver.
func (m *MapperImpl) Map(inputs []plugin.MapInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		// Format: source_id target_id timestamp
		fields := strings.Fields(input.Value)
		if len(fields) < 2 {
			continue
		}

		target := fields[1]
		records = append(records, plugin.Record{
			Key:   target,
			Value: "1",
		})
	}

	return records, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper interface {
	plugin.Mapper
} = &MapperImpl{}
