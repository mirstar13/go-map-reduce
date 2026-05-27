package command

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

var jobsCmd = &cobra.Command{
	Use:   "jobs",
	Short: "Submit and manage MapReduce jobs",
	Long: `Manage MapReduce jobs.

Sub-commands:
  list     List your jobs
  get      Get details of a single job
  submit   Upload files and submit a new job
  cancel   Cancel a running job
  output   Retrieve output file paths for a completed job`,
}

func init() {
	jobsCmd.AddCommand(jobsListCmd)
	jobsCmd.AddCommand(jobsGetCmd)
	jobsCmd.AddCommand(jobsSubmitCmd)
	jobsCmd.AddCommand(jobsCancelCmd)
	jobsCmd.AddCommand(jobsOutputCmd)
	jobsCmd.AddCommand(jobsWatchCmd)
	jobsCmd.AddCommand(jobsDeleteCmd)
	jobsCmd.AddCommand(jobsCatCmd)
}

var jobsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List your jobs",
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		var jobs []map[string]interface{}
		if err := c.Get("/jobs", &jobs); err != nil {
			return err
		}
		if flagJSON {
			printJSON(jobs)
			return nil
		}
		tw := newTabWriter()
		fmt.Fprintln(tw, "JOB ID\tSTATUS\tMAPPERS\tREDUCERS\tSUBMITTED")
		for _, j := range jobs {
			fmt.Fprintf(tw, "%s\t%s\t%.0f\t%.0f\t%s\n",
				strField(j, "job_id"),
				colourStatus(strField(j, "status")),
				numField(j, "num_mappers"),
				numField(j, "num_reducers"),
				fmtTime(strField(j, "submitted_at")),
			)
		}
		return tw.Flush()
	},
}

var jobsGetCmd = &cobra.Command{
	Use:   "get <job-id>",
	Short: "Get details of a single job",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		var job map[string]interface{}
		if err := c.Get("/jobs/"+args[0], &job); err != nil {
			return err
		}
		if flagJSON {
			printJSON(job)
			return nil
		}
		tw := newTabWriter()
		fields := []string{
			"job_id", "status", "owner_user_id", "owner_replica",
			"num_mappers", "num_reducers", "input_format",
			"mapper_path", "reducer_path", "input_path", "output_path",
			"submitted_at", "started_at", "completed_at", "error_message",
		}
		for _, f := range fields {
			v := strField(job, f)
			if v != "" {
				fmt.Fprintf(tw, "%s\t%s\n", f, v)
			}
		}
		return tw.Flush()
	},
}

var submitFlags struct {
	input       string
	mapper      string
	reducer     string
	numMappers  int
	numReducers int
	format      string
	auto        bool
}

var jobsSubmitCmd = &cobra.Command{
	Use:   "submit",
	Short: "Upload files and submit a new job",
	Long: `Submit a new MapReduce job.

This command performs three steps automatically:
  1. Uploads the input data file   → POST /files/input
  2. Uploads the mapper script     → POST /files/code
  3. Uploads the reducer script    → POST /files/code
  4. Submits the job               → POST /jobs

Examples:
  mapreduce jobs submit \
    --input   data.jsonl \
    --mapper  mapper.py  \
    --reducer reducer.py \
    --mappers 4 --reducers 2`,
	RunE: runSubmit,
}

func init() {
	jobsSubmitCmd.Flags().StringVar(&submitFlags.input, "input", "", "Input data file path (required)")
	jobsSubmitCmd.Flags().StringVar(&submitFlags.mapper, "mapper", "", "Mapper script path (required)")
	jobsSubmitCmd.Flags().StringVar(&submitFlags.reducer, "reducer", "", "Reducer script path (required)")
	jobsSubmitCmd.Flags().IntVar(&submitFlags.numMappers, "mappers", 4, "Number of map tasks")
	jobsSubmitCmd.Flags().IntVar(&submitFlags.numReducers, "reducers", 2, "Number of reduce tasks")
	jobsSubmitCmd.Flags().StringVar(&submitFlags.format, "format", "jsonl", "Input format: jsonl or text")
	jobsSubmitCmd.Flags().BoolVar(&submitFlags.auto, "auto", false, "Automatically set mapper/reducer counts based on input size")
	_ = jobsSubmitCmd.MarkFlagRequired("input")
	_ = jobsSubmitCmd.MarkFlagRequired("mapper")
	_ = jobsSubmitCmd.MarkFlagRequired("reducer")
}

func runSubmit(cmd *cobra.Command, args []string) error {
	c := newClient()

	// Validate local files exist before hitting the server.
	for _, f := range []string{submitFlags.input, submitFlags.mapper, submitFlags.reducer} {
		fi, err := os.Stat(f)
		if err != nil {
			return fmt.Errorf("file not found: %s", f)
		}

		// Calculate auto-scaling if enabled.
		if f == submitFlags.input && submitFlags.auto {
			sizeMB := float64(fi.Size()) / (1024 * 1024)
			submitFlags.numMappers = max(int(math.Ceil(sizeMB/float64(cfg.MapperThresholdMB))), 1)
			submitFlags.numReducers = max(submitFlags.numMappers/2, 1)
			fmt.Printf("Auto-scaling: using %d mappers and %d reducers for %.2f MB input (threshold: %d MB)\n",
				submitFlags.numMappers, submitFlags.numReducers, sizeMB, cfg.MapperThresholdMB)
		}
	}

	// Helper to upload a file with a progress bar.
	uploadWithProgress := func(endpoint, localPath, description string, resp any) error {
		fi, err := os.Stat(localPath)
		if err != nil {
			return err
		}
		bar := progressbar.NewOptions64(fi.Size(),
			progressbar.OptionSetDescription(description),
			progressbar.OptionSetWidth(30),
			progressbar.OptionShowBytes(true),
			progressbar.OptionShowCount(),
			progressbar.OptionOnCompletion(func() { fmt.Println() }),
			progressbar.OptionSetTheme(progressbar.Theme{
				Saucer:        "█",
				SaucerHead:    "█",
				SaucerPadding: "░",
				BarStart:      "[",
				BarEnd:        "]",
			}),
		)
		if err := c.UploadFileWithProgress(endpoint, localPath, resp, func(n int64) {
			bar.Add64(n) //nolint:errcheck
		}); err != nil {
			return err
		}
		bar.Finish() //nolint:errcheck
		return nil
	}

	// Step 1: upload input data.
	var inputResp struct {
		Path string `json:"path"`
	}
	if err := uploadWithProgress("/files/input", submitFlags.input, "Uploading input  ", &inputResp); err != nil {
		return fmt.Errorf("upload input: %w", err)
	}
	fmt.Printf("  → %s\n", inputResp.Path)

	// Step 2: upload mapper.
	var mapperResp struct {
		Path string `json:"path"`
	}
	if err := uploadWithProgress("/files/code", submitFlags.mapper, "Uploading mapper ", &mapperResp); err != nil {
		return fmt.Errorf("upload mapper: %w", err)
	}
	fmt.Printf("  → %s\n", mapperResp.Path)

	// Step 3: upload reducer.
	var reducerResp struct {
		Path string `json:"path"`
	}
	if err := uploadWithProgress("/files/code", submitFlags.reducer, "Uploading reducer", &reducerResp); err != nil {
		return fmt.Errorf("upload reducer: %w", err)
	}
	fmt.Printf("  → %s\n", reducerResp.Path)

	// Step 4: submit job.
	fmt.Println("Submitting job...")
	payload := map[string]interface{}{
		"mapper_path":  mapperResp.Path,
		"reducer_path": reducerResp.Path,
		"input_path":   inputResp.Path,
		"num_mappers":  submitFlags.numMappers,
		"num_reducers": submitFlags.numReducers,
		"input_format": submitFlags.format,
	}
	var job map[string]interface{}
	if err := c.Post("/jobs", payload, &job); err != nil {
		return fmt.Errorf("submit job: %w", err)
	}

	jobID := strField(job, "job_id")
	fmt.Printf("Job submitted successfully.\n")
	fmt.Printf("  Job ID : %s\n", jobID)
	fmt.Printf("  Status : %s\n", strField(job, "status"))
	fmt.Printf("\nTrack progress with:\n  mapreduce jobs watch %s\n", jobID)
	return nil
}

var jobsCancelCmd = &cobra.Command{
	Use:   "cancel <job-id>",
	Short: "Cancel a running job",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		var resp map[string]interface{}
		if err := c.Post("/jobs/"+args[0]+"/cancel", nil, &resp); err != nil {
			return err
		}
		fmt.Printf("Job %s cancelled.\n", args[0])
		return nil
	},
}

var jobsDeleteCmd = &cobra.Command{
	Use:   "delete <job-id>",
	Short: "Delete a job and its associated resources",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		if err := c.Delete("/jobs/"+args[0], nil); err != nil {
			return err
		}
		fmt.Printf("Job %s deleted.\n", args[0])
		return nil
	},
}

var jobsOutputCmd = &cobra.Command{
	Use:   "output <job-id>",
	Short: "Retrieve output file paths for a completed job",
	Long: `List the MinIO output paths produced by a completed job.

The paths can be downloaded via the MinIO console or by using the
mc (MinIO Client) tool:
  mc cp myminio/output/<path> ./local-file`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		var resp map[string]interface{}
		if err := c.Get("/jobs/"+args[0]+"/output", &resp); err != nil {
			return err
		}
		if flagJSON {
			printJSON(resp)
			return nil
		}

		// output_paths is a slice of objects with task_index and output_path.
		raw, _ := json.Marshal(resp["output_paths"])
		var paths []map[string]interface{}
		_ = json.Unmarshal(raw, &paths)

		if len(paths) == 0 {
			fmt.Println("No output paths found.")
			return nil
		}

		tw := newTabWriter()
		fmt.Fprintln(tw, "REDUCE INDEX\tOUTPUT PATH")
		for _, p := range paths {
			fmt.Fprintf(tw, "%.0f\t%s\n", numField(p, "task_index"), strField(p, "output_path"))
		}
		return tw.Flush()
	},
}

var jobsCatCmd = &cobra.Command{
	Use:   "cat <job-id>",
	Short: "Stream job output results to terminal",
	Long: `Download and print the results of all completed reduce tasks for a job.
Files are streamed one by one to stdout.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		jobID := args[0]

		// 1. Get output paths
		var resp map[string]interface{}
		if err := c.Get("/jobs/"+jobID+"/output", &resp); err != nil {
			return err
		}

		raw, _ := json.Marshal(resp["output_paths"])
		var paths []map[string]interface{}
		_ = json.Unmarshal(raw, &paths)

		if len(paths) == 0 {
			return fmt.Errorf("no output found for job %s (ensure it is COMPLETED)", jobID)
		}

		// 2. Download and stream each file
		for _, p := range paths {
			path := strField(p, "output_path")
			if path == "" {
				continue
			}

			// Append the path as part of the URL (for wildcard endpoint /files/download/*)
			// url.PathEscape encodes the path but keeps it safe for URL paths. 
			// However, since we want the slash to be passed to the wildcard router,
			// we should encode the segments and join them, or let Fiber handle the unescaped path.
			// Let's use the path as-is or encode segments.
			segments := strings.Split(path, "/")
			for i, seg := range segments {
				segments[i] = url.PathEscape(seg)
			}
			encodedPath := strings.Join(segments, "/")

			downloadURL := fmt.Sprintf("/files/download/%s?bucket=output", encodedPath)
			body, status, err := c.GetRaw(downloadURL)
			if err != nil {
				return fmt.Errorf("download %s: %w", path, err)
			}
			if status != 200 {
				return fmt.Errorf("download %s: server returned %d", path, status)
			}

			// Print to stdout
			_, _ = os.Stdout.Write(body)
		}

		return nil
	},
}

var jobsWatchCmd = &cobra.Command{
	Use:   "watch <job-id>",
	Short: "Watch a job's progress in real-time",
	Long: `Watch a MapReduce job and display live progress bars.

The command polls the server every 2 seconds and shows:
  - Current job phase (BUILDING → SPLITTING → MAP → REDUCE → DONE)
  - Map task completion progress bar
  - Reduce task completion progress bar
  - Elapsed time

Exits automatically when the job reaches a terminal state.`,
	Args: cobra.ExactArgs(1),
	RunE: runWatch,
}

// taskProgressInfo mirrors the server's progress response.
type taskProgressInfo struct {
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Total     int64 `json:"total"`
}

type progressInfo struct {
	JobID          string            `json:"job_id"`
	Status         string            `json:"status"`
	NumMappers     int32             `json:"num_mappers"`
	NumReducers    int32             `json:"num_reducers"`
	MapProgress    *taskProgressInfo `json:"map_progress"`
	ReduceProgress *taskProgressInfo `json:"reduce_progress"`
}

func runWatch(cmd *cobra.Command, args []string) error {
	c := newClient()
	jobID := args[0]
	start := time.Now()

	var (
		mapBar     *progressbar.ProgressBar
		reduceBar  *progressbar.ProgressBar
		lastStatus string
	)

	for {
		var progress progressInfo
		if err := c.Get("/jobs/"+jobID+"/progress", &progress); err != nil {
			return fmt.Errorf("get progress: %w", err)
		}

		elapsed := time.Since(start).Truncate(time.Second)

		// Print status header on phase change.
		if progress.Status != lastStatus {
			if lastStatus != "" {
				fmt.Println() // blank line between phases
			}
			fmt.Printf("\n⏱  Elapsed: %s  |  Phase: %s\n", elapsed, colourStatus(progress.Status))
			lastStatus = progress.Status

			// Create map bar when entering MAP_PHASE.
			if progress.Status == "MAP_PHASE" && mapBar == nil && progress.MapProgress != nil && progress.MapProgress.Total > 0 {
				mapBar = newTaskBar("  Map tasks   ", progress.MapProgress.Total)
			}

			// Create reduce bar when entering REDUCE_PHASE.
			if progress.Status == "REDUCE_PHASE" && reduceBar == nil {
				// Finish the map bar if it wasn't already.
				if mapBar != nil {
					mapBar.Finish() //nolint:errcheck
				}
				if progress.ReduceProgress != nil && progress.ReduceProgress.Total > 0 {
					reduceBar = newTaskBar("  Reduce tasks", progress.ReduceProgress.Total)
				}
			}
		}

		// Update map progress bar.
		if mapBar != nil && progress.MapProgress != nil {
			done := progress.MapProgress.Completed + progress.MapProgress.Failed
			mapBar.Set64(done) //nolint:errcheck
		}

		// Update reduce progress bar.
		if reduceBar != nil && progress.ReduceProgress != nil {
			done := progress.ReduceProgress.Completed + progress.ReduceProgress.Failed
			reduceBar.Set64(done) //nolint:errcheck
		}

		// Check for terminal state.
		if isTerminalStatus(progress.Status) {
			if mapBar != nil {
				mapBar.Finish() //nolint:errcheck
			}
			if reduceBar != nil {
				reduceBar.Finish() //nolint:errcheck
			}
			fmt.Printf("\n⏱  Elapsed: %s  |  Final: %s\n", elapsed, colourStatus(progress.Status))

			if progress.Status == "FAILED" {
				// Fetch full job details for error message.
				var job map[string]interface{}
				if err := c.Get("/jobs/"+jobID, &job); err == nil {
					if msg := strField(job, "error_message"); msg != "" {
						fmt.Printf("  Error: %s\n", msg)
					}
				}
			}
			return nil
		}

		time.Sleep(2 * time.Second)
	}
}

func newTaskBar(description string, total int64) *progressbar.ProgressBar {
	return progressbar.NewOptions64(total,
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetWidth(30),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() { fmt.Println() }),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "█",
			SaucerHead:    "█",
			SaucerPadding: "░",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)
}

func isTerminalStatus(s string) bool {
	switch strings.ToUpper(s) {
	case "COMPLETED", "FAILED", "CANCELLED":
		return true
	}
	return false
}

func strField(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}

	// Handle sql.NullString which comes back as a map if not unmarshaled into a struct.
	if mm, ok := v.(map[string]interface{}); ok {
		if s, ok := mm["String"].(string); ok {
			if valid, ok := mm["Valid"].(bool); ok && !valid {
				return ""
			}
			return s
		}
	}

	return fmt.Sprintf("%v", v)
}

func numField(m map[string]interface{}, key string) float64 {
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	f, _ := v.(float64)
	return f
}

func fmtTime(s string) string {
	if s == "" {
		return "-"
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return s
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

// statusColor returns an ANSI-coloured status string for terminals.
// Falls back to plain text if output is not a terminal (e.g. pipes).
func statusColour(s string) string {
	switch strings.ToUpper(s) {
	case "COMPLETED":
		return "\033[32m" + s + "\033[0m" // green
	case "FAILED":
		return "\033[31m" + s + "\033[0m" // red
	case "CANCELLED":
		return "\033[33m" + s + "\033[0m" // yellow
	case "MAP_PHASE", "REDUCE_PHASE", "SPLITTING", "BUILDING":
		return "\033[36m" + s + "\033[0m" // cyan
	default:
		return s
	}
}

// isTerminal returns true when stdout is a real terminal (not a pipe/redirect).
func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// colourStatus wraps statusColour only when running in an interactive terminal.
func colourStatus(s string) string {
	if isTerminal() {
		return statusColour(s)
	}
	return s
}
