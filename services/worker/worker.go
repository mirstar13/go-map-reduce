package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/pkg/plugin"
	"github.com/mirstar13/go-map-reduce/pkg/shuffle"
	"github.com/mirstar13/go-map-reduce/services/worker/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	// Connect to Shuffle Service
	conn, err := grpc.Dial(cfg.ShuffleServiceURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("failed to connect to shuffle service", zap.Error(err))
	}
	defer conn.Close()
	shuffleClient := shuffle.NewShuffleServiceClient(conn)

	w := &worker{
		cfg:           cfg,
		minio:         minioClient,
		shuffleClient: shuffleClient,
		log:           log,
		tmpDir:        "/tmp/worker",
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
	cfg           *config.Config
	minio         *minio.Client
	shuffleClient shuffle.ShuffleServiceClient
	log           *zap.Logger
	tmpDir        string
}

// runMap executes a map task:
// 1. Download compiled mapper plugin from MinIO
// 2. Download input data (byte range) from MinIO
// 3. Execute mapper plugin and stream records directly to the Shuffle Service
// 4. Report completion to Manager
func (w *worker) runMap(ctx context.Context) error {
	w.log.Info("starting map task",
		zap.String("input_file", w.cfg.InputSpec.File),
		zap.Int64("offset", w.cfg.InputSpec.Offset),
		zap.Int64("length", w.cfg.InputSpec.Length),
		zap.String("mapper", w.cfg.MapperPath),
		zap.String("input_bucket", w.cfg.MinioBucketInput),
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

	// Open input data split for streaming
	inputObj, err := w.openInputRange(ctx)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer inputObj.Close()

	// Initialize Shuffle Service stream
	stream, err := w.shuffleClient.Push(ctx)
	if err != nil {
		return fmt.Errorf("open shuffle stream: %w", err)
	}

	// Execute mapper on each input line via streaming with batching
	scanner := bufio.NewScanner(inputObj)
	const maxTokenSize = 10 * 1024 * 1024 // 10MB
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, maxTokenSize)

	lineCount := 0
	const batchSize = 1000
	var batch []plugin.MapInput

	processBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		records, err := mapper.Map(batch)
		if err != nil {
			return fmt.Errorf("mapper.Map failed: %w", err)
		}

		for _, r := range records {
			reducerIdx := w.partition(r.Key, mapper)
			if err := stream.Send(&shuffle.PushRequest{
				JobId:        w.cfg.JobID,
				TaskIndex:    int32(w.cfg.TaskIndex),
				ReducerIndex: int32(reducerIdx),
				Data:         w.recordToBytes(r),
			}); err != nil {
				return fmt.Errorf("push record to shuffle: %w", err)
			}
		}
		batch = batch[:0]
		return nil
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		key := fmt.Sprintf("%d", w.cfg.InputSpec.Offset+int64(lineCount))
		batch = append(batch, plugin.MapInput{Key: key, Value: line})

		if len(batch) >= batchSize {
			if err := processBatch(); err != nil {
				return err
			}
		}

		lineCount++
		if lineCount%100000 == 0 {
			w.log.Info("map progress", zap.Int("lines_processed", lineCount))
		}
	}
	// Process remaining records in the last batch
	if err := processBatch(); err != nil {
		return err
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	// Close stream and wait for response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("close shuffle stream: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("shuffle service reported failure")
	}

	// Report completion to manager
	return w.reportMapComplete(ctx, nil)
}

// runReduce executes a reduce task:
// 1. Download compiled reducer plugin from MinIO
// 2. Download all input partitions from MinIO to a local temp file
// 3. Perform a Pure Go external merge sort to handle large datasets within 512Mi RAM
// 4. Stream sorted records, group by key, and execute reducer
// 5. Stream final output directly to MinIO
func (w *worker) runReduce(ctx context.Context) error {
	w.log.Info("starting reduce task",
		zap.String("reducer", w.cfg.ReducerPath),
		zap.Int("num_inputs", len(w.cfg.InputLocations)),
		zap.String("output_path", w.cfg.OutputPath),
		zap.String("output_bucket", w.cfg.MinioBucketOutput),
	)

	// Download compiled reducer plugin
	reducerLocal, err := w.downloadPlugin(ctx, w.cfg.ReducerPath)
	if err != nil {
		return fmt.Errorf("download reducer: %w", err)
	}

	// Load reducer plugin
	reducer, cleanup, err := plugin.LoadReducer(ctx, reducerLocal)
	if err != nil {
		return fmt.Errorf("load reducer plugin: %w", err)
	}
	defer cleanup()

	// Phase 1: Download all partitions to a single local file
	rawFile := filepath.Join(w.tmpDir, fmt.Sprintf("reduce-%d-raw.txt", w.cfg.TaskIndex))
	rf, err := os.Create(rawFile)
	if err != nil {
		return fmt.Errorf("create raw file: %w", err)
	}
	defer os.Remove(rawFile)

	for i, loc := range w.cfg.InputLocations {
		// loc.Path now contains NodeIP:Port of a Shuffle Service
		w.log.Debug("pulling partition", zap.String("source", loc.Path))

		conn, err := grpc.Dial(loc.Path, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			rf.Close()
			return fmt.Errorf("dial shuffle service %s: %w", loc.Path, err)
		}

		client := shuffle.NewShuffleServiceClient(conn)
		stream, err := client.Pull(ctx, &shuffle.PullRequest{
			JobId:        w.cfg.JobID,
			ReducerIndex: int32(w.cfg.TaskIndex),
		})
		if err != nil {
			conn.Close()
			rf.Close()
			return fmt.Errorf("pull from %s: %w", loc.Path, err)
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				conn.Close()
				rf.Close()
				return fmt.Errorf("recv from %s: %w", loc.Path, err)
			}

			if _, err := rf.Write(resp.Data); err != nil {
				conn.Close()
				rf.Close()
				return fmt.Errorf("write to raw file: %w", err)
			}
		}
		conn.Close()

		if (i+1)%10 == 0 {
			w.log.Info("reduce pull progress", zap.Int("partitions_pulled", i+1))
		}
	}
	rf.Close()

	// Phase 2: Perform Pure Go External Merge Sort
	w.log.Info("starting external sort")
	sortedFile, err := w.externalSort(rawFile)
	if err != nil {
		return fmt.Errorf("external sort: %w", err)
	}
	defer os.Remove(sortedFile)

	// Phase 3: Group by key and stream to Reducer
	w.log.Info("streaming sorted data to reducer")
	sf, err := os.Open(sortedFile)
	if err != nil {
		return fmt.Errorf("open sorted file: %w", err)
	}
	defer sf.Close()

	// Create local output file to avoid multipart upload issues and track size
	outFile := filepath.Join(w.tmpDir, fmt.Sprintf("reduce-%d-out.txt", w.cfg.TaskIndex))
	of, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer os.Remove(outFile)
	bw := bufio.NewWriter(of)

	scanner := bufio.NewScanner(sf)
	const maxTokenSize = 10 * 1024 * 1024 // 10MB
	scanner.Buffer(make([]byte, 64*1024), maxTokenSize)
	var currentKey string
	var currentValues []string
	lineCount := 0
	const batchSize = 100 // Smaller batch size for reduce as values can be large
	var reduceBatch []plugin.ReduceInput

	processReduceBatch := func() error {
		if len(reduceBatch) == 0 {
			return nil
		}
		records, err := reducer.Reduce(reduceBatch)
		if err != nil {
			return fmt.Errorf("reducer.Reduce failed: %w", err)
		}
		for _, r := range records {
			fmt.Fprintf(bw, "%s\t%s\n", r.Key, r.Value)
		}
		reduceBatch = reduceBatch[:0]
		return nil
	}

	for scanner.Scan() {
		line := scanner.Text()
		lineCount++
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		key := parts[0]
		val := ""
		if len(parts) > 1 {
			val = parts[1]
		}

		if key != currentKey && len(currentValues) > 0 {
			reduceBatch = append(reduceBatch, plugin.ReduceInput{Key: currentKey, Values: currentValues})
			if len(reduceBatch) >= batchSize {
				if err := processReduceBatch(); err != nil {
					return err
				}
			}
			currentValues = nil

			if lineCount%100000 == 0 {
				w.log.Info("reduce progress", zap.Int("lines_processed", lineCount))
			}
		}
		currentKey = key
		currentValues = append(currentValues, val)
	}

	// Add last group to batch
	if len(currentValues) > 0 {
		reduceBatch = append(reduceBatch, plugin.ReduceInput{Key: currentKey, Values: currentValues})
	}
	// Process remaining batch
	if err := processReduceBatch(); err != nil {
		return err
	}

	if err := bw.Flush(); err != nil {
		of.Close()
		return fmt.Errorf("flush output writer: %w", err)
	}
	stat, _ := of.Stat()
	of.Seek(0, 0)

	// Upload to MinIO
	prefix := w.cfg.JobID
	if w.cfg.OutputPath != "" {
		prefix = w.cfg.OutputPath
	}
	objectKey := fmt.Sprintf("%s/part-%d.txt", prefix, w.cfg.TaskIndex)
	_, err = w.minio.PutObject(ctx, w.cfg.MinioBucketOutput, objectKey,
		of, stat.Size(),
		minio.PutObjectOptions{ContentType: "text/plain"},
	)
	of.Close()
	if err != nil {
		return fmt.Errorf("upload results: %w", err)
	}

	return w.reportReduceComplete(ctx, objectKey)
}

// externalSort implements a multi-pass merge sort to handle large files.
func (w *worker) externalSort(inputFile string) (string, error) {
	const chunkSize = 64 * 1024 * 1024 // 64MB chunks for 512MB RAM
	f, err := os.Open(inputFile)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var chunks []string
	scanner := bufio.NewScanner(f)
	const maxTokenSize = 10 * 1024 * 1024 // 10MB
	scanner.Buffer(make([]byte, 64*1024), maxTokenSize)
	var currentChunk []string
	var currentSize int

	for scanner.Scan() {
		line := scanner.Text()
		currentChunk = append(currentChunk, line)
		currentSize += len(line)

		if currentSize >= chunkSize {
			chunkFile, err := w.sortAndSaveChunk(currentChunk)
			if err != nil {
				return "", err
			}
			chunks = append(chunks, chunkFile)
			currentChunk = nil
			currentSize = 0
		}
	}

	if len(currentChunk) > 0 {
		chunkFile, err := w.sortAndSaveChunk(currentChunk)
		if err != nil {
			return "", err
		}
		chunks = append(chunks, chunkFile)
	}

	if len(chunks) == 0 {
		emptyFile := filepath.Join(w.tmpDir, "sorted-empty.txt")
		_ = os.WriteFile(emptyFile, []byte{}, 0644)
		return emptyFile, nil
	}

	if len(chunks) == 1 {
		return chunks[0], nil
	}

	return w.mergeChunks(chunks)
}

func (w *worker) sortAndSaveChunk(lines []string) (string, error) {
	sort.Strings(lines)
	f, err := os.CreateTemp(w.tmpDir, "chunk-")
	if err != nil {
		return "", err
	}
	defer f.Close()

	for _, line := range lines {
		fmt.Fprintln(f, line)
	}
	return f.Name(), nil
}

type mergeItem struct {
	line    string
	scanner *bufio.Scanner
	file    *os.File
}

type mergeHeap []*mergeItem

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].line < h[j].line }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(*mergeItem))
}
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (w *worker) mergeChunks(chunks []string) (string, error) {
	h := &mergeHeap{}
	heap.Init(h)

	for _, path := range chunks {
		f, err := os.Open(path)
		if err != nil {
			return "", err
		}
		scanner := bufio.NewScanner(f)
	const maxTokenSize = 10 * 1024 * 1024 // 10MB
	scanner.Buffer(make([]byte, 64*1024), maxTokenSize)
		if scanner.Scan() {
			heap.Push(h, &mergeItem{
				line:    scanner.Text(),
				scanner: scanner,
				file:    f,
			})
		} else {
			f.Close()
			os.Remove(path)
		}
	}

	output, err := os.CreateTemp(w.tmpDir, "sorted-")
	if err != nil {
		return "", err
	}
	defer output.Close()

	for h.Len() > 0 {
		item := heap.Pop(h).(*mergeItem)
		fmt.Fprintln(output, item.line)

		if item.scanner.Scan() {
			item.line = item.scanner.Text()
			heap.Push(h, item)
		} else {
			fname := item.file.Name()
			item.file.Close()
			os.Remove(fname)
		}
	}

	return output.Name(), nil
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

// openInputRange opens a byte range of the input file for streaming.
func (w *worker) openInputRange(ctx context.Context) (*minio.Object, error) {
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

	return obj, nil
}

// downloadInputRange downloads a byte range of the input file.
func (w *worker) downloadInputRange(ctx context.Context) ([]byte, error) {
	obj, err := w.openInputRange(ctx)
	if err != nil {
		return nil, err
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

// hashKey returns a consistent hash for a key.
func (w *worker) hashKey(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32())
}

// partition determines which reducer should handle a key.
func (w *worker) partition(key string, mapper plugin.Mapper) int {
	if p, ok := mapper.(plugin.Partitioner); ok {
		idx, err := p.Partition(key, w.cfg.NumReducers)
		if err == nil {
			return idx % w.cfg.NumReducers
		}
		w.log.Warn("custom partitioner failed, falling back to hash", zap.Error(err))
	}
	return w.hashKey(key) % w.cfg.NumReducers
}

// recordToBytes encodes a Record for the Shuffle Service.
func (w *worker) recordToBytes(r plugin.Record) []byte {
	return []byte(fmt.Sprintf("%s\t%s\n", r.Key, r.Value))
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
