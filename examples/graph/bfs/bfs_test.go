//go:build plugin

package main

import (
	"reflect"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestBFS_Map(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		inputs []plugin.MapInput
		want   []plugin.Record
	}{
		{
			name: "distribute distance",
			inputs: []plugin.MapInput{
				{Key: "n1", Value: "1.0|5|label1|n2,n3"},
			},
			want: []plugin.Record{
				{Key: "n2", Value: "6"},
				{Key: "n3", Value: "6"},
				{Key: "n1", Value: "S:1.0|label1|n2,n3"},
				{Key: "n1", Value: "D:5"},
			},
		},
		{
			name: "infinite distance",
			inputs: []plugin.MapInput{
				{Key: "n1", Value: "1.0|INF|label1|n2,n3"},
			},
			want: []plugin.Record{
				{Key: "n1", Value: "S:1.0|label1|n2,n3"},
				{Key: "n1", Value: "D:INF"},
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

func TestBFSCombine(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		key    string
		values []string
		want   []plugin.Record
	}{
		{
			name:   "minimum distance",
			key:    "node1",
			values: []string{"D:5", "D:3", "D:7"},
			want: []plugin.Record{
				{Key: "node1", Value: "D:3"},
			},
		},
		{
			name:   "structure and distance",
			key:    "node1",
			values: []string{"D:5", "S:1.0|label1|n2,n3", "D:3"},
			want: []plugin.Record{
				{Key: "node1", Value: "S:1.0|label1|n2,n3"},
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

func TestBFSReducer_Reduce(t *testing.T) {
	r := &ReducerImpl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "pick minimum distance",
			inputs: []plugin.ReduceInput{
				{Key: "n1", Values: []string{"6", "8", "S:1.0|label1|n2,n3", "D:10"}},
			},
			want: []plugin.Record{
				{Key: "n1", Value: "1.0|6|label1|n2,n3"},
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
