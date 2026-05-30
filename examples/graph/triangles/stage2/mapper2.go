//go:build plugin

package main

import (
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// Mapper2Impl processes adjacency lists and emits potential triangle edges.
type Mapper2Impl struct{}

func (m *Mapper2Impl) Combine(key string, values []string) ([]plugin.Record, error) {
	exists := false
	neighborMap := make(map[string]struct{})
	for _, v := range values {
		if v == "EXIST" {
			exists = true
		} else if v != "" {
			neighborMap[v] = struct{}{}
		}
	}

	var results []plugin.Record
	if exists {
		results = append(results, plugin.Record{Key: key, Value: "EXIST"})
	}
	for n := range neighborMap {
		results = append(results, plugin.Record{Key: key, Value: n})
	}
	return results, nil
}

func (m *Mapper2Impl) Map(inputs []plugin.MapInput) ([]plugin.Record, error) {
	var records []plugin.Record

	for _, input := range inputs {
		u := input.Key
		neighbors := strings.Split(input.Value, ",")
		if input.Value == "" {
			continue
		}

		// 1. Emit existing edges (normalize order to u < v)
		for _, v := range neighbors {
			if v == "" {
				continue
			}
			if u < v {
				records = append(records, plugin.Record{
					Key:   u + "," + v,
					Value: "EXIST",
				})
			}
		}

		// 2. Emit potential triangle edges from neighbor pairs (vi, vj) with u as middle
		// Normalize order so we only check each potential triangle once.
		for i := 0; i < len(neighbors); i++ {
			for j := i + 1; j < len(neighbors); j++ {
				vi, vj := neighbors[i], neighbors[j]
				if vi == "" || vj == "" {
					continue
				}

				// Key is the potential edge between neighbors
				pairKey := vi + "," + vj
				if vi > vj {
					pairKey = vj + "," + vi
				}

				records = append(records, plugin.Record{
					Key:   pairKey,
					Value: u,
				})
			}
		}
	}

	return records, nil
}

var Mapper interface {
	plugin.Mapper
	plugin.Combiner
} = &Mapper2Impl{}
