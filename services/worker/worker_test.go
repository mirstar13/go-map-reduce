package main

import (
	"container/heap"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashKey_Deterministic(t *testing.T) {
	w := &worker{}

	hash1 := w.hashKey("test-key")
	hash2 := w.hashKey("test-key")

	assert.Equal(t, hash1, hash2)
}

func TestHashKey_DifferentKeys(t *testing.T) {
	w := &worker{}

	hash1 := w.hashKey("key1")
	hash2 := w.hashKey("key2")

	// Different keys may have same hash (collision), but usually won't
	assert.NotEqual(t, hash1, hash2)
}

func TestRecord_Parsing(t *testing.T) {
	input := "key1\tvalue1\nkey2\tvalue with spaces\nkey3\t\n"
	lines := strings.Split(input, "\n")

	type testRecord struct {
		Key   string
		Value string
	}

	var records []testRecord
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		r := testRecord{Key: parts[0]}
		if len(parts) > 1 {
			r.Value = parts[1]
		}
		records = append(records, r)
	}

	assert.Len(t, records, 3)
	assert.Equal(t, "key1", records[0].Key)
	assert.Equal(t, "value1", records[0].Value)
	assert.Equal(t, "key2", records[1].Key)
	assert.Equal(t, "value with spaces", records[1].Value)
	assert.Equal(t, "key3", records[2].Key)
	assert.Equal(t, "", records[2].Value)
}

func TestMergeHeap(t *testing.T) {
	// This tests the heap logic used in external sort
	h := &mergeHeap{}
	heap.Init(h)
	heap.Push(h, &mergeItem{line: "banana"})
	heap.Push(h, &mergeItem{line: "apple"})
	heap.Push(h, &mergeItem{line: "cherry"})

	assert.Equal(t, 3, h.Len())
	// Min-heap: the root is the smallest
	assert.Equal(t, "apple", (*h)[0].line)

	item := heap.Pop(h).(*mergeItem)
	assert.Equal(t, "apple", item.line)
	assert.Equal(t, "banana", (*h)[0].line)
}
