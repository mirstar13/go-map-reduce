//go:build plugin

package main

import (
	"reflect"
	"sort"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestTriangleMapper2_Map(t *testing.T) {
	m := &Mapper2Impl{}
	tests := []struct {
		name   string
		inputs []plugin.MapInput
		want   []plugin.Record
	}{
		{
			name: "neighbors",
			inputs: []plugin.MapInput{
				{Key: "1", Value: "2,3"},
			},
			want: []plugin.Record{
				{Key: "1,2", Value: "EXIST"},
				{Key: "1,3", Value: "EXIST"},
				{Key: "2,3", Value: "1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := m.Map(tt.inputs)
			// Normalize got for comparison
			sort.Slice(got, func(i, j int) bool {
				if got[i].Key != got[j].Key {
					return got[i].Key < got[j].Key
				}
				return got[i].Value < got[j].Value
			})
			sort.Slice(tt.want, func(i, j int) bool {
				if tt.want[i].Key != tt.want[j].Key {
					return tt.want[i].Key < tt.want[j].Key
				}
				return tt.want[i].Value < tt.want[j].Value
			})
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Map() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTriangleReducer2_Reduce(t *testing.T) {
	r := &Reducer2Impl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "triangle detected",
			inputs: []plugin.ReduceInput{
				{Key: "2,3", Values: []string{"EXIST", "1"}},
			},
			want: []plugin.Record{
				{Key: "2,3,1", Value: "1"},
			},
		},
		{
			name: "no triangle - missing EXIST",
			inputs: []plugin.ReduceInput{
				{Key: "2,3", Values: []string{"1"}},
			},
			want: nil,
		},
		{
			name: "no triangle - missing potential",
			inputs: []plugin.ReduceInput{
				{Key: "2,3", Values: []string{"EXIST"}},
			},
			want: nil,
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
