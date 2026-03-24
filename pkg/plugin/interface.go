package plugin

// Record represents a key-value pair in MapReduce.
type Record struct {
	Key   string
	Value string
}

// Mapper is the interface that mapper plugins must implement.
// The Map function receives input records and emits intermediate key-value pairs.
type Mapper interface {
	// Map processes a single input record and returns zero or more output records.
	// The input key is typically a line number or offset, and the value is the record content.
	Map(key, value string) ([]Record, error)
}

// Reducer is the interface that reducer plugins must implement.
// The Reduce function receives a key and all values associated with that key.
type Reducer interface {
	// Reduce processes all values for a single key and returns zero or more output records.
	// The values slice contains all values emitted by mappers for this key.
	Reduce(key string, values []string) ([]Record, error)
}

// MapperFunc is a function type that implements the Mapper interface.
// This allows users to provide a simple function instead of a full struct.
type MapperFunc func(key, value string) ([]Record, error)

// Map implements the Mapper interface.
func (f MapperFunc) Map(key, value string) ([]Record, error) {
	return f(key, value)
}

// ReducerFunc is a function type that implements the Reducer interface.
type ReducerFunc func(key string, values []string) ([]Record, error)

// Reduce implements the Reducer interface.
func (f ReducerFunc) Reduce(key string, values []string) ([]Record, error) {
	return f(key, values)
}
