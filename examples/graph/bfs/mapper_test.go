package main

import (
	"reflect"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestBFSCombine(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name    string
		key     string
		values  []string
		want    []plugin.Record
	}{
		{
			name: "minimum distance",
			key:  "node1",
			values: []string{"D:5", "D:3", "D:7"},
			want: []plugin.Record{
				{Key: "node1", Value: "D:3"},
			},
		},
		{
			name: "structure and distance",
			key:  "node1",
			values: []string{"D:5", "S:1|label1|n2,n3", "D:3"},
			want: []plugin.Record{
				{Key: "node1", Value: "S:1|label1|n2,n3"},
				{Key: "node1", Value: "D:3"},
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
