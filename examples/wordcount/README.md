# Word Count Example

A classic MapReduce example that counts word frequencies in text.

## Files

- `mapper.go` - Emits (word, "1") for each word in input
- `reducer.go` - Sums counts for each unique word  
- `input.txt` - Sample input text

## Plugin Interface

The mapper and reducer implement the `plugin.Mapper` and `plugin.Reducer` interfaces:

```go
// Mapper - emits (word, "1") for each word
func (m *MapperImpl) Map(key, value string) ([]plugin.Record, error)

// Reducer - sums counts for each word
func (r *ReducerImpl) Reduce(key string, values []string) ([]plugin.Record, error)
```

## Usage

```bash
# Login
mapreduce login --server http://localhost:8081 --username admin

# Submit the job
mapreduce jobs submit \
  --input ./examples/wordcount/input.txt \
  --mapper ./examples/wordcount/mapper.go \
  --reducer ./examples/wordcount/reducer.go \
  --mappers 2 \
  --reducers 2 \
  --format text
```

## Expected Output

```
a	2
and	1
big	1
black	1
box	1
boxing	1
brown	2
daft	1
dog	3
...
```

## How It Works

1. The **Builder** compiles `mapper.go` and `reducer.go` into plugin binaries
2. **Map phase**: Each mapper splits lines into words, emits `(word, "1")`
3. **Shuffle**: Records are partitioned by `hash(word) % numReducers`
4. **Reduce phase**: Each reducer sums counts for its assigned words
