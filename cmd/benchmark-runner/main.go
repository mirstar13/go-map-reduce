package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/schollz/progressbar/v3"

	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/mirstar13/go-map-reduce/cmd/cli/config"
)

// Usage: go run cmd/benchmark-runner/main.go <input> <mapper> <reducer> <iterations> [is_minio_path]

func main() {
	if len(os.Args) < 5 {
		fmt.Println("Usage: go run cmd/benchmark-runner/main.go <input> <mapper> <reducer> <iterations> [is_minio_path]")
		os.Exit(1)
	}

	inputPath := os.Args[1]
	mapperPath := os.Args[2]
	reducerPath := os.Args[3]
	iterations := 0
	fmt.Sscanf(os.Args[4], "%d", &iterations)
	
	isMinioPath := false
	if len(os.Args) > 5 && os.Args[5] == "true" {
		isMinioPath = true
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	c := client.New(cfg.ServerURL, cfg.Token)
	currentInput := inputPath

	for i := 1; i <= iterations; i++ {
		fmt.Printf("\n--- Iteration %d of %d ---\n", i, iterations)

		// 1. Upload mapper/reducer
		var mapperResp, reducerResp struct {
			Path string `json:"path"`
		}
		if err := uploadWithProgress(c, "/files/code", mapperPath, "Uploading mapper ", &mapperResp); err != nil {
			log.Fatalf("upload mapper: %v", err)
		}
		if err := uploadWithProgress(c, "/files/code", reducerPath, "Uploading reducer", &reducerResp); err != nil {
			log.Fatalf("upload reducer: %v", err)
		}

		// 2. Resolve input URL
		var inputURL string
		if i == 1 && !isMinioPath {
			var inputResp struct {
				Path string `json:"path"`
			}
			if err := uploadWithProgress(c, "/files/input", currentInput, "Uploading input  ", &inputResp); err != nil {
				log.Fatalf("upload input: %v", err)
			}
			inputURL = inputResp.Path
		} else {
			inputURL = currentInput
		}

		// 3. Determine counts
		numMappers, numReducers := 1, 1
		if i == 1 && !isMinioPath {
			fi, err := os.Stat(currentInput)
			if err == nil {
				sizeMB := float64(fi.Size()) / (1024 * 1024)
				numMappers = max(int(math.Ceil(sizeMB/float64(cfg.MapperThresholdMB))), 1)
				numReducers = max(numMappers/2, 1)
			}
		}

		fmt.Printf("Submitting job (mappers=%d, reducers=%d)...\n", numMappers, numReducers)

		// 4. Submit Job
		payload := map[string]interface{}{
			"mapper_path":  mapperResp.Path,
			"reducer_path": reducerResp.Path,
			"input_path":   inputURL,
			"num_mappers":  numMappers,
			"num_reducers": numReducers,
			"input_format": "text",
		}

		var jobResp struct {
			JobID string `json:"job_id"`
		}
		if err := c.Post("/jobs", payload, &jobResp); err != nil {
			log.Fatalf("submit job: %v", err)
		}
		jobID := jobResp.JobID
		fmt.Printf("Job ID: %s\n", jobID)

		// 4. Poll
		clientStart := time.Now()
		var finalStatus map[string]interface{}
		var mapBar, reduceBar *progressbar.ProgressBar
		lastPhase := ""

		for {
			var progress struct {
				JobID       string `json:"job_id"`
				Status      string `json:"status"`
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
			if err := c.Get("/jobs/"+jobID+"/progress", &progress); err == nil {
				if progress.Status != lastPhase {
					if lastPhase != "" { fmt.Println() }
					elapsed := time.Since(clientStart).Truncate(time.Second)
					fmt.Printf("  [%s] Phase: %s\n", elapsed, progress.Status)
					lastPhase = progress.Status
					if progress.Status == "MAP_PHASE" && progress.MapProgress != nil {
						mapBar = newBenchmarkBar("  Map tasks   ", progress.MapProgress.Total)
					}
					if progress.Status == "REDUCE_PHASE" && progress.ReduceProgress != nil {
						if mapBar != nil { mapBar.Finish() }
						reduceBar = newBenchmarkBar("  Reduce tasks", progress.ReduceProgress.Total)
					}
				}
				if mapBar != nil && progress.MapProgress != nil { mapBar.Set64(progress.MapProgress.Completed + progress.MapProgress.Failed) }
				if reduceBar != nil && progress.ReduceProgress != nil { reduceBar.Set64(progress.ReduceProgress.Completed + progress.ReduceProgress.Failed) }

				if progress.Status == "COMPLETED" {
					if mapBar != nil { mapBar.Finish() }
					if reduceBar != nil { reduceBar.Finish() }
					_ = c.Get("/jobs/"+jobID, &finalStatus)
					break
				} else if progress.Status == "FAILED" {
					var res map[string]interface{}
					_ = c.Get("/jobs/"+jobID, &res)
					log.Fatalf("Job failed: %v", res["error_message"])
				}
			}
			time.Sleep(2 * time.Second)
		}

		clientDuration := time.Since(clientStart)
		fmt.Printf("\n--- Integration Performance Metrics (Iter %d) ---\n", i)
		fmt.Printf("Total Client Time: %v\n", clientDuration)
		fmt.Printf("--------------------------------------------------\n\n")

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
		currentInput = outResp.OutputPaths[0].OutputPath.String
		fmt.Printf("Next iteration input: %s\n", currentInput)
	}
	fmt.Println("\nBenchmark complete!")
}

func uploadWithProgress(c *client.Client, endpoint, localPath, description string, resp any) error {
	fi, err := os.Stat(localPath)
	if err != nil { return err }
	bar := progressbar.NewOptions64(fi.Size(), progressbar.OptionSetDescription(description), progressbar.OptionSetWidth(30), progressbar.OptionShowBytes(true), progressbar.OptionShowCount(), progressbar.OptionOnCompletion(func() { fmt.Println() }), progressbar.OptionSetTheme(progressbar.Theme{Saucer: "█", SaucerHead: "█", SaucerPadding: "░", BarStart: "[", BarEnd: "]"}))
	if err := c.UploadFileWithProgress(endpoint, localPath, resp, func(n int64) { bar.Add64(n) }); err != nil { return err }
	bar.Finish()
	return nil
}

func newBenchmarkBar(description string, total int64) *progressbar.ProgressBar {
	return progressbar.NewOptions64(total, progressbar.OptionSetDescription(description), progressbar.OptionSetWidth(30), progressbar.OptionShowCount(), progressbar.OptionOnCompletion(func() { fmt.Println() }), progressbar.OptionSetTheme(progressbar.Theme{Saucer: "█", SaucerHead: "█", SaucerPadding: "░", BarStart: "[", BarEnd: "]"}))
}
