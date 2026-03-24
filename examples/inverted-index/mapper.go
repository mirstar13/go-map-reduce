// Package main implements an Inverted Index mapper plugin.
// This mapper parses JSONL documents and emits (word, docID) pairs.
//
//go:build plugin

package main

import (
	"encoding/json"
	"strings"
	"unicode"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
)

type document struct {
	DocID   string `json:"doc_id"`
	Content string `json:"content"`
}

// MapperImpl implements the Mapper interface for inverted index.
type MapperImpl struct{}

// Map parses a JSON document and emits (word, docID) pairs.
func (m *MapperImpl) Map(key, value string) ([]plugin.Record, error) {
	var doc document
	if err := json.Unmarshal([]byte(value), &doc); err != nil {
		// Skip invalid JSON lines
		return nil, nil
	}

	var records []plugin.Record
	seen := make(map[string]bool)

	// Split content into words
	words := strings.FieldsFunc(doc.Content, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})

	for _, word := range words {
		word = strings.ToLower(word)
		if word != "" && !seen[word] {
			seen[word] = true
			records = append(records, plugin.Record{
				Key:   word,
				Value: doc.DocID,
			})
		}
	}

	return records, nil
}

// Mapper is the exported symbol that the worker loads.
var Mapper plugin.Mapper = &MapperImpl{}
