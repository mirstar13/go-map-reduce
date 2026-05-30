//go:build plugin

package main

import (
	"reflect"
	"sort"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestInvertedIndexMapper_Map(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		inputs []plugin.MapInput
		want   []plugin.Record
	}{
		{
			name: "json document",
			inputs: []plugin.MapInput{
				{Key: "0", Value: `{"doc_id": "doc1", "content": "Hello world hello"}`},
			},
			want: []plugin.Record{
				{Key: "hello", Value: "doc1"},
				{Key: "world", Value: "doc1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := m.Map(tt.inputs)
			// Sort got for comparison
			sort.Slice(got, func(i, j int) bool { return got[i].Key < got[j].Key })
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Map() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInvertedIndexCombine(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		key    string
		values []string
		want   []plugin.Record
	}{
		{
			name:   "deduplicate docs",
			key:    "hello",
			values: []string{"doc1", "doc2", "doc1"},
			want: []plugin.Record{
				{Key: "hello", Value: "doc1"},
				{Key: "hello", Value: "doc2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := m.Combine(tt.key, tt.values)
			sort.Slice(got, func(i, j int) bool { return got[i].Value < got[j].Value })
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Combine() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInvertedIndexReducer_Reduce(t *testing.T) {
	r := &ReducerImpl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "collect and sort docs",
			inputs: []plugin.ReduceInput{
				{Key: "hello", Values: []string{"doc2", "doc1", "doc2"}},
			},
			want: []plugin.Record{
				{Key: "hello", Value: "doc1,doc2"},
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
