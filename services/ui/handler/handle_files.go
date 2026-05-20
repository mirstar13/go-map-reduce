package handler

import (
	"io"
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

// streamUpload uses a multipart reader to stream the file directly to MinIO
// without buffering the entire body in memory.
func (h *FileHandler) streamUpload(c fiber.Ctx, kind string) error {
	contentType := string(c.Request().Header.ContentType())
	if !strings.HasPrefix(contentType, "multipart/form-data") {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "expected multipart/form-data",
		})
	}

	// We use the standard multipart.Reader to stream the body.
	// Fiber v3 allows accessing the raw body stream via c.Request().BodyStream().
	reader := multipart.NewReader(c.Request().BodyStream(), getBoundary(contentType))

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			h.log.Error("upload: read next part", zap.Error(err))
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "failed to parse multipart form",
			})
		}

		if part.FormName() != "file" {
			part.Close()
			continue
		}

		filename := part.FileName()
		if filename == "" {
			part.Close()
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "no filename provided in 'file' part",
			})
		}

		var (
			objectPath string
			uploadErr  error
		)

		// Determine the size if possible. If not provided by client, MinIO will buffer 
		// internally to calculate it (partially defeating the stream if not careful), 
		// but it's still better than Fiber buffering the whole thing.
		// Benchmark runner sends content-length for each part usually.
		size := int64(-1)

		switch kind {
		case "input":
			objectPath, uploadErr = h.minio.UploadInput(c.Context(), filename, part, size)
		case "code":
			objectPath, uploadErr = h.minio.UploadCode(c.Context(), filename, part, size)
		}

		if uploadErr != nil {
			part.Close()
			h.log.Error("upload: store to minio", zap.String("kind", kind), zap.Error(uploadErr))
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "failed to store file",
			})
		}

		part.Close()

		h.log.Info("file uploaded (streamed)",
			zap.String("kind", kind),
			zap.String("path", objectPath),
		)

		bucket := map[string]string{"input": "input", "code": "code"}[kind]
		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"path":   objectPath,
			"bucket": bucket,
		})
	}

	return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
		"error": "no 'file' field found in form",
	})
}

func getBoundary(contentType string) string {
	parts := strings.Split(contentType, "boundary=")
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}
