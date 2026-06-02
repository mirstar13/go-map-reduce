package plugin

// Record represents a key-value pair in MapReduce.
type Record struct {
	Key   string
	Value string
}

// MapInput represents a single input for the Mapper.
type MapInput struct {
	Key   string
	Value string
}

// Mapper is the interface that mapper plugins must implement.
// The Map function receives a batch of input records and emits intermediate key-value pairs.
type Mapper interface {
	// Map processes a batch of input records and returns output records.
	// This reduces RPC overhead by processing multiple lines in a single call.
	Map(inputs []MapInput) ([]Record, error)
}

// Combiner is an optional interface for local aggregation on the mapper node.
type Combiner interface {
	// Combine processes multiple values for a single key and returns zero or more output records.
	Combine(key string, values []string) ([]Record, error)
}

// Partitioner is an optional interface for custom key distribution.
type Partitioner interface {
	Partition(key string, numReducers int) (int, error)
}

// ReduceInput represents a single key and its associated values for the Reducer.
type ReduceInput struct {
	Key    string
	Values []string
}

// Reducer is the interface that reducer plugins must implement.
// The Reduce function receives a batch of keys, each with its associated values.
type Reducer interface {
	// Reduce processes a batch of keys and their values and returns output records.
	Reduce(inputs []ReduceInput) ([]Record, error)
}

// MapperFunc is a function type that implements the Mapper interface.
type MapperFunc func(inputs []MapInput) ([]Record, error)

// Map implements the Mapper interface.
func (f MapperFunc) Map(inputs []MapInput) ([]Record, error) {
	return f(inputs)
}

// ReducerFunc is a function type that implements the Reducer interface.
type ReducerFunc func(inputs []ReduceInput) ([]Record, error)

// Reduce implements the Reducer interface.
func (f ReducerFunc) Reduce(inputs []ReduceInput) ([]Record, error) {
	return f(inputs)
}
