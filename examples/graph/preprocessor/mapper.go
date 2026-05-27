//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// MapperImpl implements the Mapper interface for graph preprocessing.
// It parses edge lists (SRC DST [TIMESTAMP]) and emits (SRC, DST).
type MapperImpl struct{}

func (m *MapperImpl) Map(key, value string) ([]plugin.Record, error) {
	// Skip comments or empty lines
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return nil, nil
	}

	// sx-stackoverflow and gplus edges format: SRC DST [TIMESTAMP]
	// Using strings.Fields handles multiple spaces or tabs
	fields := strings.Fields(trimmed)
	if len(fields) < 2 {
		return nil, nil
	}

	u, v := fields[0], fields[1]
	if u == "" || v == "" {
		return nil, nil
	}

	return []plugin.Record{
		{Key: u, Value: v},
		{Key: v, Value: ""}, // Ensure DST nodes are captured
	}, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper plugin.Mapper = &MapperImpl{}
