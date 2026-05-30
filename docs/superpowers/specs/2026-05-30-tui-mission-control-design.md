# Design Spec: MapReduce Mission Control TUI

## 1. Overview
The **MapReduce Mission Control TUI** is a high-density terminal interface built with **Bubble Tea** and **Lip Gloss**. It provides real-time monitoring and management of the MapReduce cluster, replacing the basic CLI for daily operations.

## 2. Architecture
- **Language**: Go
- **Frameworks**: 
    - `charmbracelet/bubbletea`: For the TUI application lifecycle.
    - `charmbracelet/lipgloss`: For bento-grid layouts and "Tokyo Night" styling.
    - `charmbracelet/bubbles`: For progress bars, lists, and text inputs.
- **Data Source**: The existing UI API (`services/ui`).
- **Location**: `cmd/tui/`

## 3. Components (Bento Grid)

### A. Cluster Health (Top-Left)
- **Purpose**: Displays the operational status of core infrastructure.
- **Metrics**: 
    - PostgreSQL status (UP/DOWN)
    - MinIO status (UP/DOWN)
    - Active Worker count (derived from K8s Job API via Manager).
- **Style**: Subtle green/red dots for status.

### B. Active Job Progress (Top-Right)
- **Purpose**: Real-time visualization of the currently running (or most recent) job.
- **Features**: 
    - Status label (SPLITTING, MAP_PHASE, SHUFFLING, REDUCE_PHASE).
    - Large progress bar (0-100%).
    - Stats: Task count, elapsed time.

### C. Job List (Bottom-Left)
- **Purpose**: A scrollable list of historical and current jobs.
- **Interaction**: 
    - Arrow keys to select.
    - Status indicators (Icons for Success/Failure/Running).
    - Focusable via [TAB].

### D. Task Logs/Details (Bottom-Right)
- **Purpose**: Contextual information for the selected job.
- **Content**: 
    - Detailed task status breakdown.
    - Recent "Event" logs from the supervisor (e.g., "Worker-1 started MapTask #5").

### E. New Job Modal (Overlay)
- **Purpose**: A full-screen form triggered by the 'N' key.
- **Fields**: Input Path, Mapper Code Path, Reducer Code Path, Num Mappers/Reducers.

## 4. Data Flow
1. **Initial Load**: TUI fetches user config and authenticates with the UI API.
2. **Polling Loop**: Use `tea.Tick` to trigger `GET /jobs` and `GET /healthz` requests every 2 seconds.
3. **Internal State**: The `tea.Model` updates its internal state based on API responses.
4. **Commands**: Actions like `Cancel Job` or `Submit Job` are sent as HTTP requests; the UI updates on the next poll.

## 5. User Interface & Controls
- **[TAB]**: Cycle focus between Panels (Jobs -> Health -> Logs).
- **[UP/DOWN]**: Scroll selected list.
- **[N]**: New Job form.
- **[C]**: Cancel selected job (with confirmation prompt).
- **[Q / CTRL+C]**: Quit.

## 6. Error Handling
- **API Timeout**: Show a "reconnecting..." warning in the cluster health panel.
- **Auth Failure**: Prompt for re-login or exit.
- **Input Validation**: Highlight invalid fields in the "New Job" form.

## 7. Testing Strategy
- **Logic Tests**: Unit tests for the `Update` function to ensure API responses correctly transform the model state.
- **View Tests**: Use `tea.Model.View()` to verify rendered output strings for specific states.
- **API Mocking**: Use `httptest` to simulate cluster states (running jobs, failed workers).
