package handler

import (
	"bytes"
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

	// In Fiber v3, SendStream will read until EOF. 
	// To ensure the reader is closed after streaming, we can wrap it or trust Fiber
	// if it's a blocking call. If it's non-blocking, we need to be careful.
	// However, the 502 suggests the connection was dropped.
	// Let's set the content type and stream it.
	c.Set(fiber.HeaderContentType, "text/plain")
	return c.SendStream(reader)
}

// streamUpload uses a multipart reader to stream the file directly to MinIO
// without buffering the entire body in memory.
func (h *FileHandler) streamUpload(c fiber.Ctx, kind string) error {
	h.log.Info("streamUpload started", zap.String("kind", kind), zap.String("content-type", string(c.Request().Header.ContentType())))
	contentType := string(c.Request().Header.ContentType())
	if !strings.HasPrefix(contentType, "multipart/form-data") {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "expected multipart/form-data",
		})
	}

	// Correctly parse media type to get the boundary parameter.
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

	// We use the underlying fasthttp request context to access the body stream.
	// This requires StreamRequestBody: true in Fiber config.
	bodyReader := c.RequestCtx().RequestBodyStream()
	if bodyReader == nil {
		bodyReader = bytes.NewReader(c.Request().Body())
	}
	reader := multipart.NewReader(bodyReader, boundary)

	var (
		objectPath string
		uploadErr  error
		created    bool
	)

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			h.log.Error("upload: read next part", zap.Error(err))
			if !created {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "failed to parse multipart form",
				})
			}
			break
		}

		if part.FormName() == "file" && !created {
			filename := part.FileName()
			if filename == "" {
				part.Close()
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "no filename provided in 'file' part",
				})
			}

			// Determine the size if possible.
			size := int64(-1)

			switch kind {
			case "input":
				objectPath, uploadErr = h.minio.UploadInput(c.Context(), filename, part, size)
			case "code":
				objectPath, uploadErr = h.minio.UploadCode(c.Context(), filename, part, size)
			}

			if uploadErr != nil {
				h.log.Error("upload: store to minio", zap.String("kind", kind), zap.Error(uploadErr))
				part.Close()
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "failed to store file",
				})
			}
			created = true
		}
		part.Close()
	}

	if created {
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
