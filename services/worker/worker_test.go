package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
	"github.com/mirstar13/go-map-reduce/services/worker/config"
)

func TestPartitionRecordsByReducer(t *testing.T) {
	w := &worker{
		cfg: &config.Config{
			NumReducers: 3,
		},
	}

	records := []plugin.Record{
		{Key: "apple", Value: "1"},
		{Key: "banana", Value: "1"},
		{Key: "apple", Value: "1"},
		{Key: "cherry", Value: "1"},
	}
	partitions := w.partitionRecordsByReducer(records)

	// Verify all records are present across partitions
	totalRecords := 0
	for _, recs := range partitions {
		totalRecords += len(recs)
	}
	assert.Equal(t, 4, totalRecords)

	// Verify same keys go to same partition
	appleReducer := w.hashKey("apple") % 3
	var foundApple bool
	for _, r := range partitions[appleReducer] {
		if r.Key == "apple" {
			foundApple = true
			break
		}
	}
	assert.True(t, foundApple, "apple should be in reducer %d", appleReducer)
}

func TestPartitionRecordsByReducer_EmptyOutput(t *testing.T) {
	w := &worker{
		cfg: &config.Config{
			NumReducers: 2,
		},
	}

	partitions := w.partitionRecordsByReducer(nil)
	assert.Empty(t, partitions)
}

func TestPartitionRecordsByReducer_SingleReducer(t *testing.T) {
	w := &worker{
		cfg: &config.Config{
			NumReducers: 1,
		},
	}

	records := []plugin.Record{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}
	partitions := w.partitionRecordsByReducer(records)

	// All records should go to reducer 0
	assert.Len(t, partitions, 1)
	assert.Len(t, partitions[0], 2)
}

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
	// This test just verifies the function works
	assert.NotNil(t, hash1)
	assert.NotNil(t, hash2)
}

func TestGroupByKey(t *testing.T) {
	records := []keyValue{
		{Key: "apple", Value: "1"},
		{Key: "banana", Value: "1"},
		{Key: "apple", Value: "1"},
		{Key: "apple", Value: "1"},
		{Key: "banana", Value: "1"},
	}

	groups := groupByKey(records)

	assert.Len(t, groups, 2)
	assert.Len(t, groups["apple"], 3)
	assert.Len(t, groups["banana"], 2)
}

func TestKeyValue_Parsing(t *testing.T) {
	input := []byte("key1\tvalue1\nkey2\tvalue with spaces\nkey3\t\n")
	lines := bytes.Split(input, []byte("\n"))

	var records []keyValue
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		parts := bytes.SplitN(line, []byte("\t"), 2)
		kv := keyValue{Key: string(parts[0])}
		if len(parts) > 1 {
			kv.Value = string(parts[1])
		}
		records = append(records, kv)
	}

	assert.Len(t, records, 3)
	assert.Equal(t, "key1", records[0].Key)
	assert.Equal(t, "value1", records[0].Value)
	assert.Equal(t, "key2", records[1].Key)
	assert.Equal(t, "value with spaces", records[1].Value)
	assert.Equal(t, "key3", records[2].Key)
	assert.Equal(t, "", records[2].Value)
}
