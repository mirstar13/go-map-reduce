package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"

	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/mirstar13/go-map-reduce/cmd/cli/config"
)

// This script automates running multiple iterations of MapReduce jobs.
// Usage: go run cmd/benchmark-runner/main.go <input> <mapper> <reducer> <iterations>

func main() {
	if len(os.Args) < 5 {
		fmt.Println("Usage: go run cmd/benchmark-runner/main.go <input> <mapper> <reducer> <iterations>")
		os.Exit(1)
	}

	inputPath := os.Args[1]
	mapperPath := os.Args[2]
	reducerPath := os.Args[3]
	iterations := 0
	fmt.Sscanf(os.Args[4], "%d", &iterations)

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	c := client.New(cfg.ServerURL, cfg.Token)
	currentInput := inputPath

	for i := 1; i <= iterations; i++ {
		fmt.Printf("\n--- Iteration %d of %d ---\n", i, iterations)

		// 1. Upload mapper/reducer with progress bars
		var mapperResp, reducerResp struct{ Path string `json:"path"` }
		if err := uploadWithProgress(c, "/files/code", mapperPath, "Uploading mapper ", &mapperResp); err != nil {
			log.Fatalf("upload mapper: %v", err)
		}
		if err := uploadWithProgress(c, "/files/code", reducerPath, "Uploading reducer", &reducerResp); err != nil {
			log.Fatalf("upload reducer: %v", err)
		}

		// 2. Upload input (only for the first iteration, subsequent ones use output)
		var inputURL string
		if i == 1 {
			var inputResp struct{ Path string `json:"path"` }
			if err := uploadWithProgress(c, "/files/input", currentInput, "Uploading input  ", &inputResp); err != nil {
				log.Fatalf("upload input: %v", err)
			}
			inputURL = inputResp.Path
		} else {
			inputURL = currentInput
		}

		// 3. Submit Job
		fmt.Printf("Submitting job...\n")
		payload := map[string]interface{}{
			"mapper_path":  mapperResp.Path,
			"reducer_path": reducerResp.Path,
			"input_path":   inputURL,
			"num_mappers":  0,
			"input_format": "text",
		}

		var jobResp struct{ JobID string `json:"job_id"` }
		if err := c.Post("/jobs", payload, &jobResp); err != nil {
			log.Fatalf("submit job: %v", err)
		}
		jobID := jobResp.JobID
		fmt.Printf("Job ID: %s\n", jobID)

		// 4. Poll with progress display
		clientStart := time.Now()
		var finalStatus map[string]interface{}

		var (
			mapBar     *progressbar.ProgressBar
			reduceBar  *progressbar.ProgressBar
			lastPhase  string
		)

		for {
			var progress struct {
				JobID       string `json:"job_id"`
				Status      string `json:"status"`
				NumMappers  int32  `json:"num_mappers"`
				NumReducers int32  `json:"num_reducers"`
				MapProgress *struct {
					Completed int64 `json:"completed"`
					Failed    int64 `json:"failed"`
					Total     int64 `json:"total"`
				} `json:"map_progress"`
				ReduceProgress *struct {
					Completed int64 `json:"completed"`
					Failed    int64 `json:"failed"`
					Total     int64 `json:"total"`
				} `json:"reduce_progress"`
			}
			if err := c.Get("/jobs/"+jobID+"/progress", &progress); err != nil {
				log.Printf("error getting progress: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			// Print phase transitions.
			if progress.Status != lastPhase {
				if lastPhase != "" {
					fmt.Println()
				}
				elapsed := time.Since(clientStart).Truncate(time.Second)
				fmt.Printf("  [%s] Phase: %s\n", elapsed, progress.Status)
				lastPhase = progress.Status

				if progress.Status == "MAP_PHASE" && mapBar == nil && progress.MapProgress != nil && progress.MapProgress.Total > 0 {
					mapBar = newBenchmarkBar("  Map tasks   ", progress.MapProgress.Total)
				}
				if progress.Status == "REDUCE_PHASE" && reduceBar == nil {
					if mapBar != nil {
						mapBar.Finish() //nolint:errcheck
					}
					if progress.ReduceProgress != nil && progress.ReduceProgress.Total > 0 {
						reduceBar = newBenchmarkBar("  Reduce tasks", progress.ReduceProgress.Total)
					}
				}
			}

			if mapBar != nil && progress.MapProgress != nil {
				done := progress.MapProgress.Completed + progress.MapProgress.Failed
				mapBar.Set64(done) //nolint:errcheck
			}
			if reduceBar != nil && progress.ReduceProgress != nil {
				done := progress.ReduceProgress.Completed + progress.ReduceProgress.Failed
				reduceBar.Set64(done) //nolint:errcheck
			}

			switch progress.Status {
			case "COMPLETED":
				if mapBar != nil {
					mapBar.Finish() //nolint:errcheck
				}
				if reduceBar != nil {
					reduceBar.Finish() //nolint:errcheck
				}
				// Fetch full job details for metrics.
				var statusResp map[string]interface{}
				if err := c.Get("/jobs/"+jobID, &statusResp); err != nil {
					log.Printf("error getting final status: %v", err)
				} else {
					finalStatus = statusResp
				}
			case "FAILED":
				var statusResp map[string]interface{}
				_ = c.Get("/jobs/"+jobID, &statusResp)
				log.Fatalf("Job failed: %v", statusResp["error_message"])
			}

			if finalStatus != nil {
				break
			}

			time.Sleep(2 * time.Second)
		}

		// Calculate true integration performance metrics
		clientDuration := time.Since(clientStart)

		startedAtStr, _ := finalStatus["started_at"].(string)
		completedAtStr, _ := finalStatus["completed_at"].(string)
		numMappers, _ := finalStatus["num_mappers"].(float64)
		numReducers, _ := finalStatus["num_reducers"].(float64)

		var serverDuration time.Duration
		if startedAtStr != "" && completedAtStr != "" {
			startedAt, err1 := time.Parse(time.RFC3339Nano, startedAtStr)
			completedAt, err2 := time.Parse(time.RFC3339Nano, completedAtStr)
			if err1 == nil && err2 == nil {
				serverDuration = completedAt.Sub(startedAt)
			}
		}

		fmt.Printf("\n--- Integration Performance Metrics (Iter %d) ---\n", i)
		fmt.Printf("Mappers provisioned : %.0f\n", numMappers)
		fmt.Printf("Reducers provisioned: %.0f\n", numReducers)
		fmt.Printf("True Cluster Time   : %v (Time spent executing tasks)\n", serverDuration)
		fmt.Printf("Total Client Time   : %v (Includes polling & network overhead)\n", clientDuration)
		fmt.Printf("--------------------------------------------------\n\n")

		// 5. Get output for next iteration
		var outResp struct {
			OutputPaths []struct {
				OutputPath struct {
					String string `json:"String"`
				} `json:"output_path"`
			} `json:"output_paths"`
		}
		if err := c.Get(fmt.Sprintf("/jobs/%s/output", jobID), &outResp); err != nil {
			log.Fatalf("get output: %v", err)
		}

		if len(outResp.OutputPaths) == 0 {
			log.Fatal("No output produced")
		}

		currentInput = outResp.OutputPaths[0].OutputPath.String
		fmt.Printf("Next iteration input: %s\n", currentInput)
	}

	fmt.Println("\nBenchmark complete!")
}

func uploadWithProgress(c *client.Client, endpoint, localPath, description string, resp any) error {
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

func newBenchmarkBar(description string, total int64) *progressbar.ProgressBar {
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
