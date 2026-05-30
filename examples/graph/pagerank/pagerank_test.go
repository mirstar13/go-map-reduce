//go:build plugin

package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestPageRankMapper_Map(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		inputs []plugin.MapInput
		want   []plugin.Record
	}{
		{
			name: "distribute rank",
			inputs: []plugin.MapInput{
				{Key: "n1", Value: "1.0|INF|label1|n2,n3"},
			},
			want: []plugin.Record{
				{Key: "n2", Value: "0.500000"},
				{Key: "n3", Value: "0.500000"},
				{Key: "n1", Value: "S:INF|label1|n2,n3"},
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

func TestPageRankCombine(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		key    string
		values []string
		want   []plugin.Record
	}{
		{
			name:   "sum ranks",
			key:    "node1",
			values: []string{"0.5", "0.3", "0.2"},
			want: []plugin.Record{
				{Key: "node1", Value: "1.000000"},
			},
		},
		{
			name:   "structure and rank",
			key:    "node1",
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

func TestPageRankReducer_Reduce(t *testing.T) {
	r := &ReducerImpl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "calculate new rank",
			inputs: []plugin.ReduceInput{
				{Key: "n1", Values: []string{"0.5", "0.5", "S:INF|label1|n2,n3"}},
			},
			// newRank = (1-0.85) + 0.85*(0.5+0.5) = 0.15 + 0.85 = 1.0
			want: []plugin.Record{
				{Key: "n1", Value: "1.000000|INF|label1|n2,n3"},
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
				t.Errorf("Key = %s, want %s", got[0].Key, tt.inputs[0].Key)
			}
			
			if !strings.HasPrefix(got[0].Value, "1.000000|") {
				t.Errorf("Value = %s, want starting with 1.000000|", got[0].Value)
			}
			if got[0].Value != tt.want[0].Value {
				t.Errorf("Value = %s, want %s", got[0].Value, tt.want[0].Value)
			}
		})
	}
}
