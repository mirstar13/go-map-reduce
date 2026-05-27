//go:build plugin

package main

import (
	"fmt"
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// Mapper1Impl implements the first stage of Triangle Counting.
// It emits (u, v) and (v, u) for every edge to build a full adjacency list.
type Mapper1Impl struct{}

func (m *Mapper1Impl) Map(key, value string) ([]plugin.Record, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return nil, nil
	}

	fields := strings.Fields(trimmed)
	if len(fields) < 2 {
		return nil, nil
	}

	u, v := fields[0], fields[1]
	if u == v {
		return nil, nil // Ignore self-loops
	}

	return []plugin.Record{
		{Key: u, Value: v},
		{Key: v, Value: u},
	}, nil
}

var Mapper plugin.Mapper = &Mapper1Impl{}
