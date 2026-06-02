package sdk_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
	"github.com/mirstar13/go-map-reduce/pkg/sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// WordCountMapper
type WordCountMapper struct{}

func (m *WordCountMapper) Map(inputs []plugin.MapInput) ([]plugin.Record, error) {
	var records []plugin.Record
	for _, input := range inputs {
		words := strings.Fields(input.Value)
		for _, word := range words {
			records = append(records, plugin.Record{Key: word, Value: "1"})
		}
	}
	return records, nil
}

// WordCountReducer
type WordCountReducer struct{}

func (r *WordCountReducer) Reduce(inputs []plugin.ReduceInput) ([]plugin.Record, error) {
	var records []plugin.Record
	for _, input := range inputs {
		records = append(records, plugin.Record{Key: input.Key, Value: fmt.Sprintf("%d", len(input.Values))})
	}
	return records, nil
}

func TestLocalRunner_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		inputs   []plugin.Record
		expected []plugin.Record
	}{
		{
			name: "word count simple",
			inputs: []plugin.Record{
				{Key: "doc1", Value: "hello world"},
				{Key: "doc2", Value: "hello golang"},
			},
			expected: []plugin.Record{
				{Key: "golang", Value: "1"},
				{Key: "hello", Value: "2"},
				{Key: "world", Value: "1"},
			},
		},
		{
			name:     "empty input",
			inputs:   []plugin.Record{},
			expected: []plugin.Record{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runner := sdk.LocalRunner{
				Mapper:  &WordCountMapper{},
				Reducer: &WordCountReducer{},
			}

			results, err := runner.Run(context.Background(), tt.inputs)
			require.NoError(t, err)

			// Sort results by key to ensure deterministic comparison
			sort.Slice(results, func(i, j int) bool {
				if results[i].Key == results[j].Key {
					return results[i].Value < results[j].Value
				}
				return results[i].Key < results[j].Key
			})
			sort.Slice(tt.expected, func(i, j int) bool {
				if tt.expected[i].Key == tt.expected[j].Key {
					return tt.expected[i].Value < tt.expected[j].Value
				}
				return tt.expected[i].Key < tt.expected[j].Key
			})

			if len(tt.expected) == 0 {
				assert.Empty(t, results)
			} else {
				assert.Equal(t, tt.expected, results)
			}
		})
	}
}

type ctxKey struct{}

type ContextAwareMapper struct {
	Ctx context.Context
}

func (m *ContextAwareMapper) Map(inputs []plugin.MapInput) ([]plugin.Record, error) {
	return []plugin.Record{{Key: "k", Value: "v"}}, nil
}
func (m *ContextAwareMapper) InjectContext(ctx context.Context) error {
	m.Ctx = ctx
	return nil
}

type ContextAwareReducer struct {
	Ctx context.Context
}

func (r *ContextAwareReducer) Reduce(inputs []plugin.ReduceInput) ([]plugin.Record, error) {
	return []plugin.Record{{Key: "k", Value: "v"}}, nil
}
func (r *ContextAwareReducer) InjectContext(ctx context.Context) error {
	r.Ctx = ctx
	return nil
}

func TestLocalRunner_ContextAware(t *testing.T) {
	t.Parallel()

	mapper := &ContextAwareMapper{}
	reducer := &ContextAwareReducer{}
	runner := sdk.LocalRunner{
		Mapper:  mapper,
		Reducer: reducer,
	}

	ctx := context.WithValue(context.Background(), ctxKey{}, "value")
	inputs := []plugin.Record{{Key: "k", Value: "v"}}

	_, err := runner.Run(ctx, inputs)
	require.NoError(t, err)

	assert.Equal(t, ctx, mapper.Ctx)
	assert.Equal(t, ctx, reducer.Ctx)
	assert.Equal(t, "value", mapper.Ctx.Value(ctxKey{}))
}
