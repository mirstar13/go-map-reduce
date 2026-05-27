//go:build plugin

package main

import (
	"fmt"
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// MapperImpl implements the Mapper interface for PageRank.
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

	var rank float64
	_, err := fmt.Sscanf(rankStr, "%f", &rank)
	if err != nil {
		rank = 1.0
	}

	neighbors := strings.Split(neighborsStr, ",")
	if neighborsStr == "" {
		neighbors = []string{}
	}

	var records []plugin.Record

	// 1. Distribute rank to neighbors
	if len(neighbors) > 0 {
		share := rank / float64(len(neighbors))
		for _, n := range neighbors {
			if n != "" {
				records = append(records, plugin.Record{
					Key:   n,
					Value: fmt.Sprintf("%f", share),
				})
			}
		}
	}

	// 2. Preserve graph structure and other properties
	// Prefix "S:" indicates structure (distance, label, and neighbors)
	records = append(records, plugin.Record{
		Key:   key,
		Value: "S:" + distStr + "|" + labelStr + "|" + neighborsStr,
	})

	return records, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper plugin.Mapper = &MapperImpl{}
