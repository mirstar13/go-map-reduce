package client

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/mirstar13/go-map-reduce/services/ui/config"
)

// MinioClient wraps the MinIO SDK and exposes the operations needed by the UI service.
type MinioClient struct {
	client *minio.Client
	cfg    *config.Config
}

// NewMinioClient initialises the MinIO SDK client.
func NewMinioClient(cfg *config.Config) (*MinioClient, error) {
	mc, err := minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioUseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio: init client: %w", err)
	}
	return &MinioClient{client: mc, cfg: cfg}, nil
}

// UploadInput streams a file to the `input` bucket.
// Returns the object path (key) that the caller can pass to the Manager.
func (m *MinioClient) UploadInput(ctx context.Context, filename string, r io.Reader, size int64) (string, error) {
	key := objectKey("input", filename)
	return m.upload(ctx, m.cfg.MinioBucketInput, key, r, size, contentType(filename))
}

// UploadCode streams a mapper or reducer script to the `code` bucket.
// Returns the object path the caller can pass to the Manager.
func (m *MinioClient) UploadCode(ctx context.Context, filename string, r io.Reader, size int64) (string, error) {
	key := objectKey("code", filename)
	return m.upload(ctx, m.cfg.MinioBucketCode, key, r, size, contentType(filename))
}

// PresignOutput generates a presigned GET URL for an output object.
// The URL expires after 1 hour and can be given directly to the CLI user.
func (m *MinioClient) PresignOutput(ctx context.Context, objectPath string) (string, error) {
	u, err := m.client.PresignedGetObject(
		ctx,
		m.cfg.MinioBucketOutput,
		objectPath,
		time.Hour,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("minio: presign output %s: %w", objectPath, err)
	}
	return u.String(), nil
}

// upload is the internal helper that puts an object into a bucket.
func (m *MinioClient) upload(
	ctx context.Context,
	bucket, key string,
	r io.Reader,
	size int64,
	contentType string,
) (string, error) {
	_, err := m.client.PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return "", fmt.Errorf("minio: upload %s/%s: %w", bucket, key, err)
	}
	return key, nil
}

// objectKey creates a unique object key to avoid collisions.
// Format: {prefix}/{uuid}-{original-filename}
func objectKey(prefix, filename string) string {
	id := uuid.New().String()
	base := filepath.Base(filename)
	// Sanitise: replace spaces with underscores.
	base = strings.ReplaceAll(base, " ", "_")
	return fmt.Sprintf("%s/%s-%s", prefix, id, base)
}

// contentType infers a content-type from the file extension.
func contentType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".py":
		return "text/x-python"
	case ".json", ".jsonl":
		return "application/json"
	case ".txt", ".csv":
		return "text/plain"
	default:
		return "application/octet-stream"
	}
}
