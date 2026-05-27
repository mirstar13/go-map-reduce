# Graph Benchmarking Examples

This directory contains MapReduce implementations of classic graph algorithms, optimized for benchmarking on large-scale datasets like Google+ circles and Stack Overflow temporal networks.

## Algorithms

### 1. PageRank (`examples/graph/pagerank`)
Calculates the relative importance of nodes in a directed graph.
- **Complexity**: Iterative (10-20 iterations typically).
- **Intensive**: Network and Shuffle intensive (distributes rank across all edges).

### 2. Breadth-First Search (BFS) / Shortest Path (`examples/graph/bfs`)
Finds the shortest distance from a source node to all other reachable nodes.
- **Complexity**: Iterative (proportional to the graph diameter).
- **Intensive**: Memory intensive (maintains the frontier of discovery).

### 3. Connected Components (`examples/graph/cc`)
Finds clusters of nodes that are reachable from each other using Label Propagation.
- **Complexity**: Iterative.
- **Intensive**: Shuffle intensive (propagates labels until convergence).

## Data Format

All algorithms use a unified 4-field adjacency list format:
`Key: NodeID`
`Value: rank|distance|label|neighbor1,neighbor2,...`

- **rank**: Float64 (default: 1.0)
- **distance**: Integer or "INF"
- **label**: String (initially set to NodeID)
- **neighbors**: Comma-separated list of target NodeIDs

## How to Run

### Step 1: Preprocessing
Convert raw SNAP data (`SRC DST TIMESTAMP`) into the unified format using the Preprocessor.
- **Mapper**: `examples/graph/preprocessor/mapper.go`
- **Reducer**: `examples/graph/preprocessor/reducer.go`

### Step 2: Iteration
Run the desired algorithm iteratively. The output of one MapReduce job should be used as the input for the next.

```bash
# Example for one iteration of PageRank
mapreduce jobs submit \
  --input graph_preprocessed.jsonl \
  --mapper examples/graph/pagerank/mapper.go \
  --reducer examples/graph/pagerank/reducer.go \
  --auto
```

## Benchmarking with SNAP Datasets

These algorithms are designed to be tested with:
1.  **Google+ Circles**: [ego-Gplus](https://snap.stanford.edu/data/ego-Gplus.html)
2.  **Stack Overflow**: [sx-stackoverflow](https://snap.stanford.edu/data/sx-stackoverflow.html)

For benchmarking, it is recommended to monitor:
- **Map Time**: How fast the system processes edges.
- **Shuffle Time**: How well the system handles the massive data movement required by PageRank.
- **Scaling**: Use the `--auto` flag to test how the system scales with different input sizes.
