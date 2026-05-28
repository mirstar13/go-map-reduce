package main

import (
	"reflect"
	"sort"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func TestInvertedIndexCombine(t *testing.T) {
	m := &MapperImpl{}
	tests := []struct {
		name    string
		key     string
		values  []string
		want    []plugin.Record
	}{
		{
			name: "deduplicate docs",
			key:  "hello",
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
