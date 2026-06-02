package sdk

import (
	"context"
	"sync"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
	"golang.org/x/sync/errgroup"
)

// LocalRunner is an in-memory, multi-threaded runner for MapReduce plugins.
type LocalRunner struct {
	Mapper  plugin.Mapper
	Reducer plugin.Reducer
}

// Run executes the Map and Reduce phases locally using goroutines.
func (r *LocalRunner) Run(ctx context.Context, inputs []plugin.Record) ([]plugin.Record, error) {
	if len(inputs) == 0 {
		return []plugin.Record{}, nil
	}

	// Inject context into Mapper if supported
	if ca, ok := r.Mapper.(plugin.ContextAware); ok {
		if err := ca.InjectContext(ctx); err != nil {
			return nil, err
		}
	}

	// Inject context into Reducer if supported
	if ca, ok := r.Reducer.(plugin.ContextAware); ok {
		if err := ca.InjectContext(ctx); err != nil {
			return nil, err
		}
	}

	// 1. Map Phase
	var mapResults []plugin.Record
	var mapMutex sync.Mutex
	eg, mapCtx := errgroup.WithContext(ctx)

	for _, input := range inputs {
		input := input // capture loop variable
		eg.Go(func() error {
			// Check for context cancellation
			select {
			case <-mapCtx.Done():
				return mapCtx.Err()
			default:
			}

			// Map expects []plugin.MapInput, we pass a single item
			records, err := r.Mapper.Map([]plugin.MapInput{
				{Key: input.Key, Value: input.Value},
			})
			if err != nil {
				return err
			}

			mapMutex.Lock()
			mapResults = append(mapResults, records...)
			mapMutex.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// 2. Shuffle Phase
	grouped := make(map[string][]string)
	for _, rec := range mapResults {
		grouped[rec.Key] = append(grouped[rec.Key], rec.Value)
	}

	// 3. Reduce Phase
	var reduceResults []plugin.Record
	var reduceMutex sync.Mutex
	eg, reduceCtx := errgroup.WithContext(ctx)

	for key, values := range grouped {
		key := key       // capture variables
		values := values // capture variables
		eg.Go(func() error {
			select {
			case <-reduceCtx.Done():
				return reduceCtx.Err()
			default:
			}

			// Reduce expects []plugin.ReduceInput
			records, err := r.Reducer.Reduce([]plugin.ReduceInput{
				{Key: key, Values: values},
			})
			if err != nil {
				return err
			}

			reduceMutex.Lock()
			reduceResults = append(reduceResults, records...)
			reduceMutex.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	if reduceResults == nil {
		return []plugin.Record{}, nil
	}

	return reduceResults, nil
}
