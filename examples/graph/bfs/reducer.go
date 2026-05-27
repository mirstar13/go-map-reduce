//go:build plugin

package main

import (
	"fmt"
	"strings"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// ReducerImpl implements the Reducer interface for BFS.
type ReducerImpl struct{}

func (r *ReducerImpl) Reduce(key string, values []string) ([]plugin.Record, error) {
	minDist := -1 // -1 represents INF
	var structure string
	var rank string
	var label string

	for _, v := range values {
		if strings.HasPrefix(v, "S:") {
			parts := strings.SplitN(v[2:], "|", 3)
			if len(parts) == 3 {
				rank = parts[0]
				label = parts[1]
				structure = parts[2]
			}
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

	finalDist := "INF"
	if minDist != -1 {
		finalDist = fmt.Sprintf("%d", minDist)
	}

	// Output format: rank|distance|label|neighbors
	return []plugin.Record{
		{Key: key, Value: fmt.Sprintf("%s|%s|%s|%s", rank, finalDist, label, structure)},
	}, nil
}

// Reducer is the exported symbol that the worker loads.
var Reducer plugin.Reducer = &ReducerImpl{}
