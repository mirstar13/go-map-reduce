//go:build plugin

package main

import (
	"reflect"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestCC_Map(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		inputs []plugin.MapInput
		want   []plugin.Record
	}{
		{
			name: "propagate label",
			inputs: []plugin.MapInput{
				{Key: "n1", Value: "1.0|INF|n1|n2,n3"},
			},
			want: []plugin.Record{
				{Key: "n2", Value: "n1"},
				{Key: "n3", Value: "n1"},
				{Key: "n1", Value: "S:1.0|INF|n2,n3"},
				{Key: "n1", Value: "L:n1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := m.Map(tt.inputs)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Map() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCCCombine(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		key    string
		values []string
		want   []plugin.Record
	}{
		{
			name:   "minimum label",
			key:    "node1",
			values: []string{"L:10", "L:5", "L:100"},
			want: []plugin.Record{
				{Key: "node1", Value: "L:5"},
			},
		},
		{
			name:   "structure and label",
			key:    "node1",
			values: []string{"L:10", "S:1.0|INF|n2,n3", "L:5"},
			want: []plugin.Record{
				{Key: "node1", Value: "S:1.0|INF|n2,n3"},
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

func TestCCReducer_Reduce(t *testing.T) {
	r := &ReducerImpl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "pick minimum label",
			inputs: []plugin.ReduceInput{
				{Key: "n1", Values: []string{"n2", "n3", "S:1.0|INF|n2,n3", "L:n1"}},
			},
			want: []plugin.Record{
				{Key: "n1", Value: "1.0|INF|n1|n2,n3"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := r.Reduce(tt.inputs)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Reduce() = %v, want %v", got, tt.want)
			}
		})
	}
}
