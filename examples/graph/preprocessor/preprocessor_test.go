//go:build plugin

package main

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestPreprocessorMapper_Map(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		inputs []plugin.MapInput
		want   []plugin.Record
	}{
		{
			name: "edge",
			inputs: []plugin.MapInput{
				{Key: "0", Value: "1 2"},
			},
			want: []plugin.Record{
				{Key: "1", Value: "2"},
				{Key: "2", Value: ""},
			},
		},
		{
			name: "edge with timestamp",
			inputs: []plugin.MapInput{
				{Key: "0", Value: "3 4 123456789"},
			},
			want: []plugin.Record{
				{Key: "3", Value: "4"},
				{Key: "4", Value: ""},
			},
		},
		{
			name: "comment",
			inputs: []plugin.MapInput{
				{Key: "0", Value: "# this is a comment"},
			},
			want: nil,
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

func TestPreprocessorReducer_Reduce(t *testing.T) {
	r := &ReducerImpl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "group neighbors",
			inputs: []plugin.ReduceInput{
				{Key: "1", Values: []string{"2", "3", "2", ""}},
			},
			want: []plugin.Record{
				{Key: "1", Value: "1.0|INF|1|2,3"}, // Order of 2,3 might vary
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := r.Reduce(tt.inputs)
			if len(got) != 1 {
				t.Fatalf("Reduce() returned %d records, want 1", len(got))
			}
			if got[0].Key != tt.inputs[0].Key {
				t.Errorf("Reduce() Key = %s, want %s", got[0].Key, tt.inputs[0].Key)
			}
			
			// Check parts since neighbor order is non-deterministic
			parts := strings.Split(got[0].Value, "|")
			if len(parts) != 4 {
				t.Fatalf("Value has %d parts, want 4", len(parts))
			}
			if parts[0] != "1.0" || parts[1] != "INF" || parts[2] != tt.inputs[0].Key {
				t.Errorf("Metadata parts = %v, want [1.0 INF %s]", parts[:3], tt.inputs[0].Key)
			}
			
			neighbors := strings.Split(parts[3], ",")
			sort.Strings(neighbors)
			wantNeighbors := []string{"2", "3"}
			if !reflect.DeepEqual(neighbors, wantNeighbors) {
				t.Errorf("Neighbors = %v, want %v", neighbors, wantNeighbors)
			}
		})
	}
}
