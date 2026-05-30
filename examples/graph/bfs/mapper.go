//go:build plugin

package main

import (
	"fmt"
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// MapperImpl implements the Mapper interface for Breadth-First Search (BFS).
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

		// 1. Distribute distance to neighbors if the node has been visited
		if distStr != "INF" {
			var dist int
			_, err := fmt.Sscanf(distStr, "%d", &dist)
			if err == nil {
				for _, n := range neighbors {
					if n != "" {
						records = append(records, plugin.Record{
							Key:   n,
							Value: fmt.Sprintf("%d", dist+1),
						})
					}
				}
			}
		}

		// 2. Preserve structure and current properties
		// Prefix "S:" for rank, label, and neighbors, "D:" for current distance
		records = append(records, plugin.Record{
			Key:   input.Key,
			Value: "S:" + rankStr + "|" + labelStr + "|" + neighborsStr,
		})
		records = append(records, plugin.Record{
			Key:   input.Key,
			Value: "D:" + distStr,
		})
	}

	return records, nil
}

func (m *MapperImpl) Combine(key string, values []string) ([]plugin.Record, error) {
	minDist := -1
	var structure string

	for _, v := range values {
		if strings.HasPrefix(v, "S:") {
			structure = v
		} else {
			var dStr string
			if strings.HasPrefix(v, "D:") {
				dStr = v[2:]
			} else {
				dStr = v
			}

			if dStr == "INF" {
				continue
			}

			var d int
			_, err := fmt.Sscanf(dStr, "%d", &d)
			if err == nil {
				if minDist == -1 || d < minDist {
					minDist = d
				}
			}
		}
	}

	var records []plugin.Record
	if structure != "" {
		records = append(records, plugin.Record{Key: key, Value: structure})
	}
	if minDist != -1 {
		records = append(records, plugin.Record{Key: key, Value: fmt.Sprintf("D:%d", minDist)})
	}
	return records, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper interface {
	plugin.Mapper
	plugin.Combiner
} = &MapperImpl{}
