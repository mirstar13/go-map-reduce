//go:build plugin

package main

import (
	"reflect"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestTrafficMapper_Map(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name   string
		inputs []plugin.MapInput
		want   []plugin.Record
	}{
		{
			name: "single line",
			inputs: []plugin.MapInput{
				{Key: "0", Value: `in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839`},
			},
			want: []plugin.Record{
				{Key: "00", Value: "1"},
			},
		},
		{
			name: "different hour",
			inputs: []plugin.MapInput{
				{Key: "0", Value: `uplherc.upl.com - - [01/Aug/1995:23:59:59 -0400] "GET / HTTP/1.0" 304 0`},
			},
			want: []plugin.Record{
				{Key: "23", Value: "1"},
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

func TestTrafficReducer_Reduce(t *testing.T) {
	r := &ReducerImpl{}
	tests := []struct {
		name   string
		inputs []plugin.ReduceInput
		want   []plugin.Record
	}{
		{
			name: "sum counts",
			inputs: []plugin.ReduceInput{
				{Key: "00", Values: []string{"1", "1", "5"}},
			},
			want: []plugin.Record{
				{Key: "00", Value: "7"},
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
