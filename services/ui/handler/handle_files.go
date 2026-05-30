package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"strings"

	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/services/ui/client"
)

// FileHandler handles file-upload routes.
type FileHandler struct {
	minio *client.MinioClient
	log   *zap.Logger
}

// NewFileHandler creates a new FileHandler.
func NewFileHandler(mc *client.MinioClient, log *zap.Logger) *FileHandler {
	return &FileHandler{minio: mc, log: log}
}

// UploadInput handles streaming upload for input data.
func (h *FileHandler) UploadInput(c fiber.Ctx) error {
	return h.streamUpload(c, "input")
}

// UploadCode handles streaming upload for mapper/reducer code.
func (h *FileHandler) UploadCode(c fiber.Ctx) error {
	return h.streamUpload(c, "code")
}

// DownloadFile handles streaming download for an object from the 'output' bucket.
func (h *FileHandler) DownloadFile(c fiber.Ctx) error {
	path := c.Params("*")
	if path == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "path is required in URL"})
	}

	bucket := c.Query("bucket", "output")

	reader, err := h.minio.DownloadFile(c.Context(), bucket, path)
	if err != nil {
		h.log.Error("download: minio error", zap.String("bucket", bucket), zap.String("path", path), zap.Error(err))
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "file not found or inaccessible"})
	}

	c.Set(fiber.HeaderContentType, "text/plain")
	return c.SendStream(reader)
}

// streamUpload uses a multipart reader to stream the file directly to MinIO
// without buffering the entire body in memory.
func (h *FileHandler) streamUpload(c fiber.Ctx, kind string) error {
	h.log.Info("streamUpload started", zap.String("kind", kind))
	contentType := string(c.Request().Header.ContentType())
	if !strings.HasPrefix(contentType, "multipart/form-data") {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "expected multipart/form-data",
		})
	}

	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid content-type",
		})
	}
	boundary := params["boundary"]
	if boundary == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "missing boundary in content-type",
		})
	}

	bodyReader := c.RequestCtx().RequestBodyStream()
	if bodyReader == nil {
		bodyReader = bytes.NewReader(c.Request().Body())
	}

	reader := multipart.NewReader(bodyReader, boundary)

	var (
		objectPath string
		created    bool
	)

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			h.log.Error("upload: next part error", zap.Error(err), zap.String("kind", kind))
			if err == io.ErrUnexpectedEOF {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "client disconnected prematurely"})
			}
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "failed to parse upload stream"})
		}

		if part.FormName() == "file" && !created {
			filename := part.FileName()
			if filename == "" {
				part.Close()
				continue
			}

			h.log.Info("uploading part", zap.String("filename", filename), zap.String("kind", kind))

			path, err := h.uploadToMinio(c.Context(), c, kind, filename, part)
			if err != nil {
				h.log.Error("upload: minio error", zap.String("kind", kind), zap.Error(err))
				part.Close()
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to store file"})
			}
			objectPath = path
			created = true
		}
		part.Close()
	}

	if created {
		h.log.Info("file upload successful", zap.String("kind", kind), zap.String("path", objectPath))
		bucket := map[string]string{"input": "input", "code": "code"}[kind]
		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"path":   objectPath,
			"bucket": bucket,
		})
	}

	return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "no file found in request"})
}

func (h *FileHandler) uploadToMinio(ctx context.Context, c fiber.Ctx, kind, filename string, r io.Reader) (string, error) {
	prefix := c.Query("prefix")
	if prefix != "" {
		// Ensure prefix doesn't have leading/trailing slashes and uses only alphanumeric/underscore/dash/slash
		prefix = strings.Trim(prefix, "/")
		if prefix != "" {
			filename = prefix + "/" + filename
		}
	}

	switch kind {
	case "input":
		return h.minio.UploadInput(ctx, filename, r, -1)
	case "code":
		return h.minio.UploadCode(ctx, filename, r, -1)
	default:
		return "", fmt.Errorf("invalid upload kind: %s", kind)
	}
}
