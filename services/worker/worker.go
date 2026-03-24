package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
	"github.com/mirstar13/go-map-reduce/services/worker/config"
)

func main() {
	log, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync() //nolint:errcheck

	cfg, err := config.Load()
	if err != nil {
		log.Fatal("configuration error", zap.Error(err))
	}

	log.Info("worker starting",
		zap.String("task_id", cfg.TaskID),
		zap.String("task_type", string(cfg.TaskType)),
		zap.String("job_id", cfg.JobID),
		zap.Int("task_index", cfg.TaskIndex),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	minioClient, err := minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioUseSSL,
	})
	if err != nil {
		log.Fatal("failed to create minio client", zap.Error(err))
	}

	w := &worker{
		cfg:    cfg,
		minio:  minioClient,
		log:    log,
		tmpDir: "/tmp/worker",
	}

	if err := os.MkdirAll(w.tmpDir, 0755); err != nil {
		log.Fatal("failed to create tmp dir", zap.Error(err))
	}

	var runErr error
	switch cfg.TaskType {
	case config.TaskTypeMap:
		runErr = w.runMap(ctx)
	case config.TaskTypeReduce:
		runErr = w.runReduce(ctx)
	}

	if runErr != nil {
		log.Error("task failed", zap.Error(runErr))
		if err := w.reportFailure(ctx); err != nil {
			log.Error("failed to report failure", zap.Error(err))
		}
		os.Exit(1)
	}

	log.Info("task completed successfully")
}

type worker struct {
	cfg    *config.Config
	minio  *minio.Client
	log    *zap.Logger
	tmpDir string
}

// runMap executes a map task:
// 1. Download compiled mapper plugin from MinIO
// 2. Download input data (byte range) from MinIO
// 3. Execute mapper plugin, partition output by reducer
// 4. Upload partitioned output to MinIO
// 5. Report completion to Manager
func (w *worker) runMap(ctx context.Context) error {
	w.log.Info("starting map task",
		zap.String("input_file", w.cfg.InputSpec.File),
		zap.Int64("offset", w.cfg.InputSpec.Offset),
		zap.Int64("length", w.cfg.InputSpec.Length),
		zap.String("mapper", w.cfg.MapperPath),
	)

	// Download compiled mapper plugin
	mapperLocal, err := w.downloadPlugin(ctx, w.cfg.MapperPath)
	if err != nil {
		return fmt.Errorf("download mapper: %w", err)
	}

	// Load mapper plugin via go-plugin
	mapper, cleanup, err := plugin.LoadMapper(ctx, mapperLocal)
	if err != nil {
		return fmt.Errorf("load mapper plugin: %w", err)
	}
	defer cleanup()

	// Download input data with byte range
	inputData, err := w.downloadInputRange(ctx)
	if err != nil {
		return fmt.Errorf("download input: %w", err)
	}

	// Execute mapper on each input line
	var allOutputs []plugin.Record
	lines := bytes.Split(inputData, []byte("\n"))
	for i, line := range lines {
		if len(line) == 0 {
			continue
		}
		key := fmt.Sprintf("%d", w.cfg.InputSpec.Offset+int64(i))
		records, err := mapper.Map(key, string(line))
		if err != nil {
			return fmt.Errorf("mapper.Map failed at line %d: %w", i, err)
		}
		allOutputs = append(allOutputs, records...)
	}

	// Partition output by reducer
	partitions := w.partitionRecordsByReducer(allOutputs)

	// Upload partitions and collect output locations
	outputLocations := make([]outputLocation, 0, len(partitions))
	for reducerIdx, records := range partitions {
		if len(records) == 0 {
			continue
		}
		path, err := w.uploadRecordPartition(ctx, reducerIdx, records)
		if err != nil {
			return fmt.Errorf("upload partition %d: %w", reducerIdx, err)
		}
		outputLocations = append(outputLocations, outputLocation{
			ReducerIndex: reducerIdx,
			Path:         path,
		})
	}

	// Report completion to manager
	return w.reportMapComplete(ctx, outputLocations)
}

// runReduce executes a reduce task:
// 1. Download compiled reducer plugin from MinIO
// 2. Download all input partitions from MinIO
// 3. Sort and group inputs by key
// 4. Execute reducer plugin for each key
// 5. Upload final output to MinIO
// 6. Report completion to Manager
func (w *worker) runReduce(ctx context.Context) error {
	w.log.Info("starting reduce task",
		zap.Int("input_locations", len(w.cfg.InputLocations)),
		zap.String("reducer", w.cfg.ReducerPath),
	)

	// Download compiled reducer plugin
	reducerLocal, err := w.downloadPlugin(ctx, w.cfg.ReducerPath)
	if err != nil {
		return fmt.Errorf("download reducer: %w", err)
	}

	// Load reducer plugin via go-plugin
	reducer, cleanup, err := plugin.LoadReducer(ctx, reducerLocal)
	if err != nil {
		return fmt.Errorf("load reducer plugin: %w", err)
	}
	defer cleanup()

	// Download and merge all input partitions
	var allRecords []keyValue
	for _, loc := range w.cfg.InputLocations {
		records, err := w.downloadPartition(ctx, loc.Path)
		if err != nil {
			return fmt.Errorf("download partition %s: %w", loc.Path, err)
		}
		allRecords = append(allRecords, records...)
	}

	// Sort by key
	sort.Slice(allRecords, func(i, j int) bool {
		return allRecords[i].Key < allRecords[j].Key
	})

	// Group by key and execute reducer
	var results []plugin.Record
	groups := groupByKey(allRecords)
	for key, values := range groups {
		records, err := reducer.Reduce(key, values)
		if err != nil {
			return fmt.Errorf("reducer.Reduce failed for key %q: %w", key, err)
		}
		results = append(results, records...)
	}

	// Upload final output
	outputPath, err := w.uploadReduceOutput(ctx, results)
	if err != nil {
		return fmt.Errorf("upload output: %w", err)
	}

	// Report completion to manager
	return w.reportReduceComplete(ctx, outputPath)
}

// groupByKey groups records by their key, returning a map of key -> values.
func groupByKey(records []keyValue) map[string][]string {
	groups := make(map[string][]string)
	for _, r := range records {
		groups[r.Key] = append(groups[r.Key], r.Value)
	}
	return groups
}

// downloadPlugin downloads a compiled plugin binary from MinIO.
func (w *worker) downloadPlugin(ctx context.Context, codePath string) (string, error) {
	localPath := filepath.Join(w.tmpDir, filepath.Base(codePath))

	obj, err := w.minio.GetObject(ctx, w.cfg.MinioBucketCode, codePath, minio.GetObjectOptions{})
	if err != nil {
		return "", fmt.Errorf("get object: %w", err)
	}
	defer obj.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, obj); err != nil {
		return "", fmt.Errorf("copy object: %w", err)
	}

	// Make plugin executable
	if err := os.Chmod(localPath, 0755); err != nil {
		return "", fmt.Errorf("chmod: %w", err)
	}

	w.log.Debug("downloaded plugin", zap.String("path", codePath), zap.String("local", localPath))
	return localPath, nil
}

// downloadInputRange downloads a byte range of the input file.
func (w *worker) downloadInputRange(ctx context.Context) ([]byte, error) {
	opts := minio.GetObjectOptions{}
	if w.cfg.InputSpec.Length > 0 {
		endByte := w.cfg.InputSpec.Offset + w.cfg.InputSpec.Length - 1
		if err := opts.SetRange(w.cfg.InputSpec.Offset, endByte); err != nil {
			return nil, fmt.Errorf("set range: %w", err)
		}
	}

	obj, err := w.minio.GetObject(ctx, w.cfg.MinioBucketInput, w.cfg.InputSpec.File, opts)
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("read object: %w", err)
	}

	w.log.Debug("downloaded input range",
		zap.String("file", w.cfg.InputSpec.File),
		zap.Int64("offset", w.cfg.InputSpec.Offset),
		zap.Int("bytes", len(data)),
	)
	return data, nil
}

type keyValue struct {
	Key   string
	Value string
}

// partitionRecordsByReducer partitions plugin.Record by hash(key) % numReducers.
func (w *worker) partitionRecordsByReducer(records []plugin.Record) map[int][]plugin.Record {
	partitions := make(map[int][]plugin.Record, w.cfg.NumReducers)

	for _, r := range records {
		reducerIdx := w.hashKey(r.Key) % w.cfg.NumReducers
		partitions[reducerIdx] = append(partitions[reducerIdx], r)
	}

	return partitions
}

// hashKey returns a consistent hash for a key.
func (w *worker) hashKey(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32())
}

// uploadRecordPartition uploads records to the jobs bucket as tab-separated lines.
func (w *worker) uploadRecordPartition(ctx context.Context, reducerIdx int, records []plugin.Record) (string, error) {
	var buf bytes.Buffer
	for _, r := range records {
		fmt.Fprintf(&buf, "%s\t%s\n", r.Key, r.Value)
	}

	objectKey := fmt.Sprintf("%s/map-%d-reduce-%d.txt", w.cfg.JobID, w.cfg.TaskIndex, reducerIdx)

	_, err := w.minio.PutObject(ctx, w.cfg.MinioBucketJobs, objectKey,
		bytes.NewReader(buf.Bytes()), int64(buf.Len()),
		minio.PutObjectOptions{ContentType: "text/plain"},
	)
	if err != nil {
		return "", fmt.Errorf("put object: %w", err)
	}

	w.log.Debug("uploaded partition",
		zap.String("key", objectKey),
		zap.Int("reducer", reducerIdx),
		zap.Int("records", len(records)),
	)
	return objectKey, nil
}

// downloadPartition downloads a partition file and parses it into key-value pairs.
func (w *worker) downloadPartition(ctx context.Context, path string) ([]keyValue, error) {
	obj, err := w.minio.GetObject(ctx, w.cfg.MinioBucketJobs, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("read object: %w", err)
	}

	var records []keyValue
	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		parts := bytes.SplitN(line, []byte("\t"), 2)
		kv := keyValue{Key: string(parts[0])}
		if len(parts) > 1 {
			kv.Value = string(parts[1])
		}
		records = append(records, kv)
	}

	w.log.Debug("downloaded partition",
		zap.String("path", path),
		zap.Int("records", len(records)),
	)
	return records, nil
}

// uploadReduceOutput uploads reducer results to the output bucket.
func (w *worker) uploadReduceOutput(ctx context.Context, records []plugin.Record) (string, error) {
	var buf bytes.Buffer
	for _, r := range records {
		fmt.Fprintf(&buf, "%s\t%s\n", r.Key, r.Value)
	}

	objectKey := fmt.Sprintf("%s/part-%d.txt", w.cfg.JobID, w.cfg.TaskIndex)

	_, err := w.minio.PutObject(ctx, w.cfg.MinioBucketOutput, objectKey,
		bytes.NewReader(buf.Bytes()), int64(buf.Len()),
		minio.PutObjectOptions{ContentType: "text/plain"},
	)
	if err != nil {
		return "", fmt.Errorf("put object: %w", err)
	}

	w.log.Info("uploaded reduce output",
		zap.String("key", objectKey),
		zap.Int("records", len(records)),
	)
	return objectKey, nil
}

type outputLocation struct {
	ReducerIndex int    `json:"reducer_index"`
	Path         string `json:"path"`
}

// reportMapComplete sends completion callback to the Manager.
func (w *worker) reportMapComplete(ctx context.Context, locations []outputLocation) error {
	url := fmt.Sprintf("%s/tasks/map/%s/complete", w.cfg.ManagerURL, w.cfg.TaskID)

	body, err := json.Marshal(map[string]interface{}{
		"output_locations": locations,
	})
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	return w.doCallback(ctx, url, body)
}

// reportReduceComplete sends completion callback to the Manager.
func (w *worker) reportReduceComplete(ctx context.Context, outputPath string) error {
	url := fmt.Sprintf("%s/tasks/reduce/%s/complete", w.cfg.ManagerURL, w.cfg.TaskID)

	body, err := json.Marshal(map[string]interface{}{
		"output_path": outputPath,
	})
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	return w.doCallback(ctx, url, body)
}

// reportFailure sends failure callback to the Manager.
func (w *worker) reportFailure(ctx context.Context) error {
	var url string
	if w.cfg.TaskType == config.TaskTypeMap {
		url = fmt.Sprintf("%s/tasks/map/%s/fail", w.cfg.ManagerURL, w.cfg.TaskID)
	} else {
		url = fmt.Sprintf("%s/tasks/reduce/%s/fail", w.cfg.ManagerURL, w.cfg.TaskID)
	}

	return w.doCallback(ctx, url, []byte("{}"))
}

// doCallback performs an HTTP POST to the Manager.
func (w *worker) doCallback(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("callback failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	w.log.Info("callback successful", zap.String("url", url))
	return nil
}
