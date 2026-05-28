package main

import (
	"reflect"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestPageRankCombine(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name    string
		key     string
		values  []string
		want    []plugin.Record
	}{
		{
			name: "sum ranks",
			key:  "node1",
			values: []string{"0.5", "0.3", "0.2"},
			want: []plugin.Record{
				{Key: "node1", Value: "1.000000"},
			},
		},
		{
			name: "structure and rank",
			key:  "node1",
			values: []string{"0.5", "S:INF|label1|n2,n3", "0.5"},
			want: []plugin.Record{
				{Key: "node1", Value: "S:INF|label1|n2,n3"},
				{Key: "node1", Value: "1.000000"},
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
