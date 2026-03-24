# Inverted Index Example

Builds an inverted index mapping words to the documents that contain them.

## Files

- `mapper.go` - Emits (word, doc_id) for each unique word in document
- `reducer.go` - Collects all doc_ids containing each word
- `input.jsonl` - Sample documents in JSON Lines format

## Plugin Interface

```go
// Mapper - parses JSONL, emits (word, docID) for each unique word
func (m *MapperImpl) Map(key, value string) ([]plugin.Record, error)

// Reducer - collects document IDs for each word
func (r *ReducerImpl) Reduce(key string, values []string) ([]plugin.Record, error)
```

## Usage

```bash
# Login
mapreduce login --server http://localhost:8081 --username admin

# Submit the job
mapreduce jobs submit \
  --input ./examples/inverted-index/input.jsonl \
  --mapper ./examples/inverted-index/mapper.go \
  --reducer ./examples/inverted-index/reducer.go \
  --mappers 2 \
  --reducers 2 \
  --format jsonl
```

## Expected Output

```
a	doc2
and	doc3,doc4,doc5
animals	doc4
are	doc4,doc5
being	doc5
brown	doc1,doc2
clever	doc5
cunning	doc3
dog	doc1,doc2
dogs	doc4
fox	doc1,doc2,doc3
foxes	doc5
...
```

## How It Works

1. The **Builder** compiles `mapper.go` and `reducer.go` into plugin binaries
2. **Map phase**: Each mapper parses JSON, emits `(word, docID)` pairs
3. **Shuffle**: Records are partitioned by `hash(word) % numReducers`
4. **Reduce phase**: Each reducer collects document IDs for its words
