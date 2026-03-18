package splitter

import (
	"context"
	"fmt"
	"io"

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

// Compute divides the object at `objectKey` in the input bucket into
// `numSplits` byte-range splits, each boundary snapped to the next newline.
// This mirrors the design doc's splitting logic exactly.
func (s *Splitter) Compute(ctx context.Context, objectKey string, numSplits int) ([]Split, error) {
	stat, err := s.client.StatObject(ctx, s.cfg.MinioBucketInput, objectKey, minio.StatObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("splitter: stat %s: %w", objectKey, err)
	}
	fileSize := stat.Size

	if numSplits < 1 {
		numSplits = 1
	}

	// For very small files, produce a single split.
	targetSize := fileSize / int64(numSplits)
	if targetSize < 1 {
		targetSize = fileSize
		numSplits = 1
	}

	splits := make([]Split, 0, numSplits)
	var offset int64

	for i := 0; i < numSplits && offset < fileSize; i++ {
		tentativeEnd := offset + targetSize

		if tentativeEnd >= fileSize || i == numSplits-1 {
			// Last split: take everything that remains.
			splits = append(splits, Split{
				Index:  i,
				File:   objectKey,
				Offset: offset,
				Length: fileSize - offset,
			})
			break
		}

		// Snap the boundary forward to the next newline.
		actualEnd, err := s.findNextNewline(ctx, objectKey, tentativeEnd)
		if err != nil {
			return nil, fmt.Errorf("splitter: find newline boundary at %d: %w", tentativeEnd, err)
		}

		splits = append(splits, Split{
			Index:  i,
			File:   objectKey,
			Offset: offset,
			Length: actualEnd - offset,
		})
		offset = actualEnd
	}

	return splits, nil
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
