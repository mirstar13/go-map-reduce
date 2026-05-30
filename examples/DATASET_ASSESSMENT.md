# Dataset Assessment and Algorithm Suggestions

This document provides an assessment of the datasets included in the `examples/` directory and suggests suitable MapReduce algorithms for each.

## 1. Google+ Social Network (`./examples/googleplus/`)

### Assessment
*   **Format**: SNAP ego-network format.
*   **Content**: Contains multiple files per "ego" user:
    *   `.edges`: Edge list of the ego user's network.
    *   `.feat`: Feature vectors for nodes.
    *   `.featnames`: Names of the features.
    *   `.circles`: User-defined groups (circles).
*   **Scale**: Distributed across thousands of small files, representing a large sparse graph when combined.

### Suggested MapReduce Algorithms
1.  **Triangle Counting**: 
    *   **Map**: Emit all pairs of neighbors for a node.
    *   **Reduce**: Identify where pairs of neighbors are also connected to each other.
2.  **Connected Components**:
    *   **Algorithm**: Label Propagation.
    *   **Map**: Emit (neighbor, current_label).
    *   **Reduce**: Update current_label to the minimum received label.
3.  **PageRank**:
    *   **Map**: Distribute a node's rank evenly among its neighbors.
    *   **Reduce**: Sum the received rank shares and apply the damping factor.
4.  **Degree Distribution**:
    *   **Map**: For each edge `(u, v)`, emit `(u, 1)` and `(v, 1)`.
    *   **Reduce**: Sum counts to get degree per node, then a second pass to count frequency of each degree.

---

## 2. NASA Access Logs (`./examples/nasa-access-logs/`)

### Assessment
*   **Format**: Common Log Format (CLF).
*   **Content**: HTTP request logs from 1995.
    *   Example: `in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /path HTTP/1.0" 200 1839`
*   **Scale**: A single large file (~160MB), ideal for line-by-line streaming.

### Suggested MapReduce Algorithms
1.  **Popular Pages (Top K)**:
    *   **Map**: Parse the request path and emit `(path, 1)`.
    *   **Reduce**: Sum counts per path.
2.  **Error Rate Analysis**:
    *   **Map**: Parse the HTTP status code. Emit `(status_code, 1)`.
    *   **Reduce**: Count occurrences of 4xx and 5xx errors.
3.  **Hourly Traffic Volume**:
    *   **Map**: Parse the timestamp, extract the hour, emit `(hour, 1)`.
    *   **Reduce**: Sum requests per hour to see peak usage times.
4.  **Total Bandwidth per Host**:
    *   **Map**: Parse host and bytes sent. Emit `(host, bytes)`.
    *   **Reduce**: Sum bytes per host.

---

## 3. Stack Overflow Temporal Network (`./examples/sx-stackoverflow/`)

### Assessment
*   **Format**: Directed graph with timestamps (`source_id target_id timestamp`).
*   **Content**: Interactions on Stack Overflow (answers, comments).
*   **Scale**: Very large text files (up to 1.6GB).

### Suggested MapReduce Algorithms
1.  **User Reputation (PageRank Variant)**:
    *   **Map**: Treat answers/comments as "votes" or links between users.
    *   **Reduce**: Calculate importance based on interaction density.
2.  **Activity Burst Detection**:
    *   **Map**: Emit `(timestamp_bin, 1)`.
    *   **Reduce**: Identify time windows with unusually high interaction counts.
3.  **Inverted User Index**:
    *   **Map**: For each interaction, emit `(target_id, source_id)`.
    *   **Reduce**: Create a list of all users who interacted with a specific user.
4.  **Reciprocity Measurement**:
    *   **Map**: For an edge `(u, v)`, emit `(sorted_pair(u, v), edge_direction)`.
    *   **Reduce**: Check if both `(u, v)` and `(v, u)` exist in the interaction history.
