//go:build plugin

package main

import (
	"reflect"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestReputationMapper_Map(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		inputs []plugin.MapInput
		want   []plugin.Record
	}{
		{
			name: "interaction",
			inputs: []plugin.MapInput{
				{Key: "0", Value: "9 8 1217567877"},
			},
			want: []plugin.Record{
				{Key: "8", Value: "1"},
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

func TestReputationReducer_Reduce(t *testing.T) {
	r := &ReducerImpl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "sum reputation",
			inputs: []plugin.ReduceInput{
				{Key: "8", Values: []string{"1", "1", "10"}},
			},
			want: []plugin.Record{
				{Key: "8", Value: "12"},
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
