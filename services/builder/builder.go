package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/mirstar13/go-map-reduce/services/builder/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	log := initLogger(cfg.LogLevel)
	defer log.Sync() //nolint:errcheck

	// Set environment variables for Go toolchain globally for this process and children.
	os.Setenv("HOME", "/tmp")
	os.Setenv("GOCACHE", "/tmp/go-cache")
	os.Setenv("GOMODCACHE", "/tmp/go-mod")
	os.Setenv("GOPATH", "/tmp/go")
	os.Setenv("CGO_ENABLED", "0")
	os.Setenv("GOPROXY", "https://proxy.golang.org,direct")

	// Pre-create Go cache directories in /tmp
	_ = os.MkdirAll("/tmp/go-cache", 0777)
	_ = os.MkdirAll("/tmp/go-mod", 0777)
	_ = os.MkdirAll("/tmp/go", 0777)
	_ = os.MkdirAll("/tmp/builder", 0777)

	minioClient, err := minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioUseSSL,
	})
	if err != nil {
		log.Fatal("failed to create minio client", zap.Error(err))
	}

	b := &builder{
		cfg:    cfg,
		minio:  minioClient,
		log:    log,
		tmpDir: "/tmp/builder",
	}

	app := fiber.New(fiber.Config{
		AppName: "Builder Service",
	})

	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})

	app.Post("/build", b.handleBuild)

	log.Info("builder service starting", zap.String("port", cfg.Port))
	if err := app.Listen(":" + cfg.Port); err != nil {
		log.Fatal("server failed", zap.Error(err))
	}
}

type builder struct {
	cfg    *config.Config
	minio  *minio.Client
	log    *zap.Logger
	tmpDir string
}

type buildRequest struct {
	JobID      string `json:"job_id"`
	PluginType string `json:"plugin_type"` // "mapper" | "reducer"
	SourcePath string `json:"source_path"`
	OutputPath string `json:"output_path"`
}

func (b *builder) handleBuild(c fiber.Ctx) error {
	var req buildRequest
	if err := c.Bind().Body(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	b.log.Info("received build request",
		zap.String("job_id", req.JobID),
		zap.String("type", req.PluginType),
		zap.String("source", req.SourcePath),
	)

	// Run build in a background goroutine to not block the request
	// and use the callback to report status.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		if err := b.runBuild(ctx, req); err != nil {
			b.log.Error("build failed", zap.String("job_id", req.JobID), zap.Error(err))
			if reportErr := b.reportFailure(ctx, req.JobID, req.PluginType, err.Error()); reportErr != nil {
				b.log.Error("failed to report failure", zap.Error(reportErr))
			}
		} else {
			b.log.Info("build successful", zap.String("job_id", req.JobID))
			if reportErr := b.reportSuccess(ctx, req.JobID, req.PluginType, req.OutputPath); reportErr != nil {
				b.log.Error("failed to report success", zap.Error(reportErr))
			}
		}
	}()

	return c.SendStatus(fiber.StatusAccepted)
}

func (b *builder) runBuild(ctx context.Context, req buildRequest) error {
	buildDir := filepath.Join(b.tmpDir, req.JobID+"-"+req.PluginType)
	_ = os.RemoveAll(buildDir)
	if err := os.MkdirAll(buildDir, 0755); err != nil {
		return fmt.Errorf("create build dir: %w", err)
	}

	sourcePath := filepath.Join(buildDir, "plugin.go")
	if err := b.downloadFile(ctx, req.SourcePath, sourcePath); err != nil {
		return fmt.Errorf("download source: %w", err)
	}

	mainPath := filepath.Join(buildDir, "main.go")
	if err := b.generateMain(mainPath, req.PluginType); err != nil {
		return fmt.Errorf("generate main: %w", err)
	}

	if err := b.initGoModule(ctx, buildDir); err != nil {
		return fmt.Errorf("init go module: %w", err)
	}

	binaryPath := filepath.Join(buildDir, "plugin")
	if err := b.compile(ctx, buildDir, binaryPath); err != nil {
		return fmt.Errorf("compile: %w", err)
	}

	if err := b.uploadFile(ctx, binaryPath, req.OutputPath); err != nil {
		return fmt.Errorf("upload plugin: %w", err)
	}

	return nil
}

func (b *builder) downloadFile(ctx context.Context, objectKey, localPath string) error {
	obj, err := b.minio.GetObject(ctx, b.cfg.MinioBucketCode, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer obj.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, obj); err != nil {
		return fmt.Errorf("copy object: %w", err)
	}
	return nil
}

func (b *builder) generateMain(path, pType string) error {
	code := mapperMain
	if pType == "reducer" {
		code = reducerMain
	}
	return os.WriteFile(path, []byte(code), 0644)
}

func (b *builder) initGoModule(ctx context.Context, buildDir string) error {
	cmd := exec.CommandContext(ctx, "go", "mod", "init", "userplugin")
	cmd.Dir = buildDir
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go mod init: %w", err)
	}

	cmd = exec.CommandContext(ctx, "go", "mod", "edit", "-replace", "github.com/mirstar13/go-map-reduce=/app/core")
	cmd.Dir = buildDir
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go mod edit -replace: %w", err)
	}

	cmd = exec.CommandContext(ctx, "go", "mod", "tidy")
	cmd.Dir = buildDir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		b.log.Warn("go mod tidy failed", zap.String("stderr", stderr.String()))
	}
	return nil
}

func (b *builder) compile(ctx context.Context, buildDir, outputPath string) error {
	cmd := exec.CommandContext(ctx, "go", "build",
		"-o", outputPath,
		"-tags", "plugin",
		"-ldflags=-s -w",
		".",
	)
	cmd.Dir = buildDir

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go build failed: %s", stderr.String())
	}
	return nil
}

func (b *builder) uploadFile(ctx context.Context, localPath, objectKey string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	_, err = b.minio.PutObject(ctx, b.cfg.MinioBucketCode, objectKey,
		f, stat.Size(),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	return err
}

func (b *builder) reportSuccess(ctx context.Context, jobID, pType, outputPath string) error {
	url := fmt.Sprintf("%s/builds/%s/complete", b.cfg.ManagerURL, jobID)
	body, _ := json.Marshal(map[string]interface{}{
		"plugin_type": pType,
		"plugin_path": outputPath,
	})
	return b.doCallback(ctx, url, body)
}

func (b *builder) reportFailure(ctx context.Context, jobID, pType, errMsg string) error {
	url := fmt.Sprintf("%s/builds/%s/fail", b.cfg.ManagerURL, jobID)
	body, _ := json.Marshal(map[string]interface{}{
		"plugin_type": pType,
		"error":       errMsg,
	})
	return b.doCallback(ctx, url, body)
}

func (b *builder) doCallback(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("callback failed (%d): %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func initLogger(level string) *zap.Logger {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		zapLevel = zap.InfoLevel
	}

	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zapLevel)
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, _ := config.Build()
	return logger
}

const mapperMain = `package main
import (
	"github.com/hashicorp/go-plugin"
	mrplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)
func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: mrplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"mapper": &mrplugin.MapperPlugin{Impl: Mapper},
		},
	})
}
`

const reducerMain = `package main
import (
	"github.com/hashicorp/go-plugin"
	mrplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)
func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: mrplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"reducer": &mrplugin.ReducerPlugin{Impl: Reducer},
		},
	})
}
`
