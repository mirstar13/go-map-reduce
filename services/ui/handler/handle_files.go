package handler

import (
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/services/ui/client"
)

// FileHandler handles file-upload routes.
// Clients upload input data and code files here before submitting a job;
// the returned object paths are then passed in the POST /jobs body.
type FileHandler struct {
	minio *client.MinioClient
	log   *zap.Logger
}

// NewFileHandler creates a new FileHandler.
func NewFileHandler(mc *client.MinioClient, log *zap.Logger) *FileHandler {
	return &FileHandler{minio: mc, log: log}
}

// UploadInput godoc
//
//	POST /files/input
//	Requires: user or admin role.
//	Content-Type: multipart/form-data
//	Form field: file (the input data file)
//
// Streams the file to the MinIO `input` bucket and returns the object path.
// This path is used as `input_path` in the POST /jobs body.
//
// Example response:
//
//	{"path": "input/3f1c…-data.jsonl", "bucket": "input", "size": 204800}
func (h *FileHandler) UploadInput(c fiber.Ctx) error {
	return h.uploadFile(c, "input")
}

// UploadCode godoc
//
//	POST /files/code
//	Requires: user or admin role.
//	Content-Type: multipart/form-data
//	Form field: file (the mapper or reducer script)
//
// Streams the file to the MinIO `code` bucket and returns the object path.
// The path is used as `mapper_path` or `reducer_path` in the POST /jobs body.
//
// Example response:
//
//	{"path": "code/7d2a…-mapper.py", "bucket": "code", "size": 1024}
func (h *FileHandler) UploadCode(c fiber.Ctx) error {
	return h.uploadFile(c, "code")
}

// uploadFile is the shared implementation for both upload endpoints.
func (h *FileHandler) uploadFile(c fiber.Ctx, kind string) error {
	// Parse the multipart form.
	form, err := c.MultipartForm()
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "expected multipart/form-data with a 'file' field",
		})
	}

	files, ok := form.File["file"]
	if !ok || len(files) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "no file found in form field 'file'",
		})
	}

	fileHeader := files[0]
	f, err := fileHeader.Open()
	if err != nil {
		h.log.Error("upload: open multipart file", zap.Error(err))
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "could not read uploaded file",
		})
	}
	defer f.Close()

	var (
		objectPath string
		uploadErr  error
	)

	switch kind {
	case "input":
		objectPath, uploadErr = h.minio.UploadInput(
			c.Context(),
			fileHeader.Filename,
			f,
			fileHeader.Size,
		)
	case "code":
		objectPath, uploadErr = h.minio.UploadCode(
			c.Context(),
			fileHeader.Filename,
			f,
			fileHeader.Size,
		)
	default:
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "unknown upload kind",
		})
	}

	if uploadErr != nil {
		h.log.Error("upload: minio put object",
			zap.String("kind", kind),
			zap.String("filename", fileHeader.Filename),
			zap.Error(uploadErr),
		)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to store file",
		})
	}

	h.log.Info("file uploaded",
		zap.String("kind", kind),
		zap.String("path", objectPath),
		zap.Int64("size", fileHeader.Size),
	)

	bucket := map[string]string{"input": "input", "code": "code"}[kind]
	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"path":   objectPath,
		"bucket": bucket,
		"size":   fileHeader.Size,
	})
}
