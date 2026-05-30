//go:build plugin

package main

import (
	"reflect"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestWordCountMapper_Map(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		inputs []plugin.MapInput
		want   []plugin.Record
	}{
		{
			name: "simple words",
			inputs: []plugin.MapInput{
				{Key: "0", Value: "Hello world hello"},
			},
			want: []plugin.Record{
				{Key: "hello", Value: "1"},
				{Key: "world", Value: "1"},
				{Key: "hello", Value: "1"},
			},
		},
		{
			name: "with punctuation",
			inputs: []plugin.MapInput{
				{Key: "0", Value: "Hello, world! 123."},
			},
			want: []plugin.Record{
				{Key: "hello", Value: "1"},
				{Key: "world", Value: "1"},
				{Key: "123", Value: "1"},
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

func TestWordCountMapper_Combine(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		key    string
		values []string
		want   []plugin.Record
	}{
		{
			name:   "sum multiple values",
			key:    "hello",
			values: []string{"1", "1", "1"},
			want: []plugin.Record{
				{Key: "hello", Value: "3"},
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

func TestWordCountReducer_Reduce(t *testing.T) {
	r := &ReducerImpl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "sum multiple values",
			inputs: []plugin.ReduceInput{
				{Key: "hello", Values: []string{"3", "2", "5"}},
			},
			want: []plugin.Record{
				{Key: "hello", Value: "10"},
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
