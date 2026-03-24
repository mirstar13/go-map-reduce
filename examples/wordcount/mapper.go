// Package main implements a WordCount mapper plugin.
// This mapper splits each line into words and emits (word, "1") for each word.
//
//go:build plugin

package main

import (
	"strings"
	"unicode"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

// MapperImpl implements the Mapper interface for word counting.
type MapperImpl struct{}

// Map splits the input value into words and emits (word, "1") pairs.
func (m *MapperImpl) Map(key, value string) ([]plugin.Record, error) {
	var records []plugin.Record

	// Split line into words, normalize to lowercase
	words := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})

	for _, word := range words {
		word = strings.ToLower(word)
		if word != "" {
			records = append(records, plugin.Record{
				Key:   word,
				Value: "1",
			})
		}
	}

	return records, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper plugin.Mapper = &MapperImpl{}
