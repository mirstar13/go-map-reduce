package interfaces

import (
	"context"
	"encoding/json"

	"github.com/mirstar13/go-map-reduce/services/manager/dispatcher"
	"github.com/mirstar13/go-map-reduce/services/manager/splitter"
)

// Splitter is satisfied by *splitter.Splitter.
// Extracted as an interface so the supervisor can be tested without MinIO.
type Splitter interface {
	Compute(ctx context.Context, objectKey string, numSplits int) ([]splitter.Split, error)
}

// Dispatcher is satisfied by *dispatcher.Dispatcher.
// Extracted as an interface so the supervisor can be tested without Kubernetes.
type Dispatcher interface {
	DispatchMap(ctx context.Context, spec dispatcher.MapTaskSpec) (string, error)
	DispatchReduce(ctx context.Context, spec dispatcher.ReduceTaskSpec) (string, error)
	DeleteJob(ctx context.Context, jobName string) error
}

// Ensure the concrete types still satisfy the interfaces at compile time.
var _ Splitter = (*splitter.Splitter)(nil)
var _ Dispatcher = (*dispatcher.Dispatcher)(nil)

// RawJSON is a convenience alias kept here so tests can build
// dispatcher.ReduceTaskSpec.InputLocations without importing dispatcher.
type RawJSON = json.RawMessage
