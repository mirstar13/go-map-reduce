//go:build plugin

package main

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestTriangleMapper1_Map(t *testing.T) {
	m := &Mapper1Impl{}
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
				{Key: "2", Value: "1"},
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

func TestTriangleReducer1_Reduce(t *testing.T) {
	r := &Reducer1Impl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "group neighbors",
			inputs: []plugin.ReduceInput{
				{Key: "1", Values: []string{"2", "3", "2"}},
			},
			want: []plugin.Record{
				{Key: "1", Value: "2,3"}, // Order might vary
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := r.Reduce(tt.inputs)
			if len(got) != 1 {
				t.Fatalf("Reduce() returned %d records, want 1", len(got))
			}
			
			neighbors := strings.Split(got[0].Value, ",")
			sort.Strings(neighbors)
			wantNeighbors := []string{"2", "3"}
			if !reflect.DeepEqual(neighbors, wantNeighbors) {
				t.Errorf("Neighbors = %v, want %v", neighbors, wantNeighbors)
			}
		})
	}
}
