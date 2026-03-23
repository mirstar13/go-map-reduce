package command

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

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
				strField(j, "status"),
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
	_ = jobsSubmitCmd.MarkFlagRequired("input")
	_ = jobsSubmitCmd.MarkFlagRequired("mapper")
	_ = jobsSubmitCmd.MarkFlagRequired("reducer")
}

func runSubmit(cmd *cobra.Command, args []string) error {
	c := newClient()

	// Validate local files exist before hitting the server.
	for _, f := range []string{submitFlags.input, submitFlags.mapper, submitFlags.reducer} {
		if _, err := os.Stat(f); err != nil {
			return fmt.Errorf("file not found: %s", f)
		}
	}

	// Step 1: upload input data.
	fmt.Printf("Uploading input file %s...\n", submitFlags.input)
	var inputResp struct {
		Path string `json:"path"`
	}
	if err := c.UploadFile("/files/input", submitFlags.input, &inputResp); err != nil {
		return fmt.Errorf("upload input: %w", err)
	}
	fmt.Printf("  → %s\n", inputResp.Path)

	// Step 2: upload mapper.
	fmt.Printf("Uploading mapper %s...\n", submitFlags.mapper)
	var mapperResp struct {
		Path string `json:"path"`
	}
	if err := c.UploadFile("/files/code", submitFlags.mapper, &mapperResp); err != nil {
		return fmt.Errorf("upload mapper: %w", err)
	}
	fmt.Printf("  → %s\n", mapperResp.Path)

	// Step 3: upload reducer.
	fmt.Printf("Uploading reducer %s...\n", submitFlags.reducer)
	var reducerResp struct {
		Path string `json:"path"`
	}
	if err := c.UploadFile("/files/code", submitFlags.reducer, &reducerResp); err != nil {
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
	fmt.Printf("\nTrack progress with:\n  mapreduce jobs get %s\n", jobID)
	return nil
}

var jobsCancelCmd = &cobra.Command{
	Use:   "cancel <job-id>",
	Short: "Cancel a running job",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		var resp map[string]interface{}
		if err := c.Delete("/jobs/"+args[0], &resp); err != nil {
			return err
		}
		fmt.Printf("Job %s cancelled.\n", args[0])
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

func strField(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
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
	case "MAP_PHASE", "REDUCE_PHASE", "SPLITTING":
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
