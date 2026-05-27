//go:build plugin

package main

import (
	"fmt"
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// Mapper2Impl processes adjacency lists and emits potential triangle edges.
type Mapper2Impl struct{}

func (m *Mapper2Impl) Map(key, value string) ([]plugin.Record, error) {
	u := key
	neighbors := strings.Split(value, ",")
	if value == "" {
		return nil, nil
	}

	var records []plugin.Record

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

	return records, nil
}

var Mapper plugin.Mapper = &Mapper2Impl{}
