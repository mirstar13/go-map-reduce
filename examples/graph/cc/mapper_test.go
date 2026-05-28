package main

import (
	"reflect"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestCCCombine(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name    string
		key     string
		values  []string
		want    []plugin.Record
	}{
		{
			name: "minimum label",
			key:  "node1",
			values: []string{"L:10", "L:5", "L:100"},
			want: []plugin.Record{
				{Key: "node1", Value: "L:5"},
			},
		},
		{
			name: "structure and label",
			key:  "node1",
			values: []string{"L:10", "S:1|INF|n2,n3", "L:5"},
			want: []plugin.Record{
				{Key: "node1", Value: "S:1|INF|n2,n3"},
				{Key: "node1", Value: "L:5"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := m.Combine(tt.key, tt.values)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Combine() = %v, want %v", got, tt.want)
			}
		})
	}
}
