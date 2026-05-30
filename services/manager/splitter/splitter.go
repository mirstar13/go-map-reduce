package splitter

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/mirstar13/go-map-reduce/services/manager/config"
)

// Split describes a byte-range of an input file to be processed by one map task.
type Split struct {
	Index  int
	File   string // MinIO object key
	Offset int64
	Length int64
}

// Splitter computes input splits from a MinIO object.
type Splitter struct {
	client *minio.Client
	cfg    *config.Config
}

// New creates a Splitter backed by MinIO.
func New(cfg *config.Config) (*Splitter, error) {
	mc, err := minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioUseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("splitter: init minio client: %w", err)
	}
	return &Splitter{client: mc, cfg: cfg}, nil
}

// GetSize returns the total size of the object(s) in MinIO matching the prefix.
func (s *Splitter) GetSize(ctx context.Context, inputPath string) (int64, error) {
	var totalSize int64
	paths := strings.Split(inputPath, ",")

	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		// Try to stat as a single object first
		stat, err := s.client.StatObject(ctx, s.cfg.MinioBucketInput, p, minio.StatObjectOptions{})
		if err == nil {
			totalSize += stat.Size
			continue
		}

		// If stat fails, try to list as a prefix
		objectCh := s.client.ListObjects(ctx, s.cfg.MinioBucketInput, minio.ListObjectsOptions{
			Prefix:    p,
			Recursive: true,
		})

		found := false
		for obj := range objectCh {
			if obj.Err != nil {
				return 0, fmt.Errorf("splitter: list objects for %s: %w", p, obj.Err)
			}
			totalSize += obj.Size
			found = true
		}

		if !found {
			return 0, fmt.Errorf("splitter: input path %s not found", p)
		}
	}

	return totalSize, nil
}

// Compute divides the input at `inputPath` (file or prefix, can be comma-separated) into `numSplits` tasks.
func (s *Splitter) Compute(ctx context.Context, inputPath string, numSplits int) ([]Split, error) {
	if numSplits < 1 {
		numSplits = 1
	}

	// 1. Identify all files to process
	files, totalSize, err := s.findFiles(ctx, inputPath)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("splitter: no input files found at %s", inputPath)
	}

	// 2. Compute splits
	targetSplitSize := totalSize / int64(numSplits)
	if targetSplitSize < 1024*1024 { // Minimum 1MB split unless data is very small
		targetSplitSize = 1024 * 1024
	}

	var splits []Split
	splitIndex := 0

	for _, f := range files {
		// Simple implementation:
		// - Every file <= targetSplitSize gets its own single split.
		// - Every file > targetSplitSize is partitioned.
		// This ensures we never miss a file and fits the current DB schema (1 Task = 1 File).

		if f.size <= targetSplitSize || numSplits == 1 {
			splits = append(splits, Split{
				Index:  splitIndex,
				File:   f.name,
				Offset: 0,
				Length: f.size,
			})
			splitIndex++
			continue
		}

		// Partition large file
		numFileSplits := int(f.size / targetSplitSize)
		if numFileSplits < 1 {
			numFileSplits = 1
		}

		var offset int64
		for i := 0; i < numFileSplits && offset < f.size; i++ {
			tentativeEnd := offset + targetSplitSize
			if tentativeEnd >= f.size || i == numFileSplits-1 {
				splits = append(splits, Split{
					Index:  splitIndex,
					File:   f.name,
					Offset: offset,
					Length: f.size - offset,
				})
				splitIndex++
				break
			}

			actualEnd, err := s.findNextNewline(ctx, f.name, tentativeEnd)
			if err != nil {
				return nil, err
			}
			splits = append(splits, Split{
				Index:  splitIndex,
				File:   f.name,
				Offset: offset,
				Length: actualEnd - offset,
			})
			offset = actualEnd
			splitIndex++
		}
	}

	return splits, nil
}

type fileInfo struct {
	name string
	size int64
}

func (s *Splitter) findFiles(ctx context.Context, inputPath string) ([]fileInfo, int64, error) {
	var files []fileInfo
	var totalSize int64
	paths := strings.Split(inputPath, ",")

	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		stat, err := s.client.StatObject(ctx, s.cfg.MinioBucketInput, p, minio.StatObjectOptions{})
		if err == nil {
			// Single file
			files = append(files, fileInfo{name: p, size: stat.Size})
			totalSize += stat.Size
		} else {
			// Prefix/Directory
			objectCh := s.client.ListObjects(ctx, s.cfg.MinioBucketInput, minio.ListObjectsOptions{
				Prefix:    p,
				Recursive: true,
			})
			for obj := range objectCh {
				if obj.Err != nil {
					return nil, 0, fmt.Errorf("splitter: list objects for %s: %w", p, obj.Err)
				}
				if obj.Size > 0 {
					files = append(files, fileInfo{name: obj.Key, size: obj.Size})
					totalSize += obj.Size
				}
			}
		}
	}
	return files, totalSize, nil
}

// findNextNewline reads a small lookahead window from `startOffset` and returns
// the byte position immediately after the first newline character found.
// If no newline is found in the lookahead, it recurses one window forward.
func (s *Splitter) findNextNewline(ctx context.Context, objectKey string, startOffset int64) (int64, error) {
	const lookahead = 4096

	opts := minio.GetObjectOptions{}
	opts.SetRange(startOffset, startOffset+lookahead-1)

	obj, err := s.client.GetObject(ctx, s.cfg.MinioBucketInput, objectKey, opts)
	if err != nil {
		return 0, err
	}
	defer obj.Close()

	buf := make([]byte, lookahead)
	n, err := io.ReadFull(obj, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return 0, err
	}

	for i := 0; i < n; i++ {
		if buf[i] == '\n' {
			return startOffset + int64(i) + 1, nil
		}
	}

	if n == 0 {
		// We've reached the end of the file without finding a newline.
		return startOffset, nil
	}
	return s.findNextNewline(ctx, objectKey, startOffset+int64(n))
}
