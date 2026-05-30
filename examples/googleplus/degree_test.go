//go:build plugin

package main

import (
	"reflect"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestDegreeMapper_Map(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		inputs []plugin.MapInput
		want   []plugin.Record
	}{
		{
			name: "edge",
			inputs: []plugin.MapInput{
				{Key: "0", Value: "123 456"},
			},
			want: []plugin.Record{
				{Key: "123", Value: "1"},
				{Key: "456", Value: "1"},
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

func TestDegreeReducer_Reduce(t *testing.T) {
	r := &ReducerImpl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "sum degree",
			inputs: []plugin.ReduceInput{
				{Key: "123", Values: []string{"1", "1", "1"}},
			},
			want: []plugin.Record{
				{Key: "123", Value: "3"},
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
