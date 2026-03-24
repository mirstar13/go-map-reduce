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

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/services/builder/config"
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

	log.Info("builder starting",
		zap.String("job_id", cfg.JobID),
		zap.String("plugin_type", string(cfg.PluginType)),
		zap.String("source", cfg.SourcePath),
		zap.String("output", cfg.OutputPath),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

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

	if err := os.MkdirAll(b.tmpDir, 0755); err != nil {
		log.Fatal("failed to create tmp dir", zap.Error(err))
	}

	if err := b.run(ctx); err != nil {
		log.Error("build failed", zap.Error(err))
		if callbackErr := b.reportFailure(ctx, err.Error()); callbackErr != nil {
			log.Error("failed to report failure", zap.Error(callbackErr))
		}
		os.Exit(1)
	}

	log.Info("build completed successfully")
}

type builder struct {
	cfg    *config.Config
	minio  *minio.Client
	log    *zap.Logger
	tmpDir string
}

// run executes the build process:
// 1. Download source .go file from MinIO
// 2. Generate plugin wrapper code
// 3. Compile to plugin binary
// 4. Upload compiled binary to MinIO
// 5. Report completion to Manager
func (b *builder) run(ctx context.Context) error {
	// Create a unique build directory
	buildDir := filepath.Join(b.tmpDir, b.cfg.JobID)
	if err := os.MkdirAll(buildDir, 0755); err != nil {
		return fmt.Errorf("create build dir: %w", err)
	}

	// Download source file
	sourcePath := filepath.Join(buildDir, "plugin.go")
	if err := b.downloadFile(ctx, b.cfg.SourcePath, sourcePath); err != nil {
		return fmt.Errorf("download source: %w", err)
	}

	// Generate the plugin main wrapper
	mainPath := filepath.Join(buildDir, "main.go")
	if err := b.generateMain(mainPath); err != nil {
		return fmt.Errorf("generate main: %w", err)
	}

	// Initialize go module
	if err := b.initGoModule(ctx, buildDir); err != nil {
		return fmt.Errorf("init go module: %w", err)
	}

	// Compile the plugin
	binaryPath := filepath.Join(buildDir, "plugin")
	if err := b.compile(ctx, buildDir, binaryPath); err != nil {
		return fmt.Errorf("compile: %w", err)
	}

	// Upload compiled plugin
	if err := b.uploadFile(ctx, binaryPath, b.cfg.OutputPath); err != nil {
		return fmt.Errorf("upload plugin: %w", err)
	}

	// Report success
	return b.reportSuccess(ctx)
}

// downloadFile downloads a file from MinIO to a local path.
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

	b.log.Debug("downloaded file", zap.String("object", objectKey), zap.String("local", localPath))
	return nil
}

// generateMain creates the plugin main.go that wraps the user's implementation.
func (b *builder) generateMain(path string) error {
	var code string

	if b.cfg.PluginType == config.PluginTypeMapper {
		code = mapperMain
	} else {
		code = reducerMain
	}

	if err := os.WriteFile(path, []byte(code), 0644); err != nil {
		return fmt.Errorf("write main.go: %w", err)
	}

	b.log.Debug("generated main.go", zap.String("path", path))
	return nil
}

// initGoModule initializes a go module in the build directory.
func (b *builder) initGoModule(ctx context.Context, buildDir string) error {
	// Create go.mod
	goMod := `module plugin

go 1.25

require github.com/hashicorp/go-plugin v1.6.0
require github.com/mirstar13/go-map-reduce v0.0.0
`
	modPath := filepath.Join(buildDir, "go.mod")
	if err := os.WriteFile(modPath, []byte(goMod), 0644); err != nil {
		return fmt.Errorf("write go.mod: %w", err)
	}

	// Run go mod tidy to resolve dependencies
	cmd := exec.CommandContext(ctx, "go", "mod", "tidy")
	cmd.Dir = buildDir
	cmd.Env = append(os.Environ(),
		"GOPROXY=https://proxy.golang.org,direct",
		"CGO_ENABLED=0",
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		b.log.Warn("go mod tidy failed (may be okay)", zap.String("stderr", stderr.String()))
	}

	return nil
}

// compile builds the plugin binary.
func (b *builder) compile(ctx context.Context, buildDir, outputPath string) error {
	cmd := exec.CommandContext(ctx, "go", "build",
		"-o", outputPath,
		"-ldflags=-s -w",
		".",
	)
	cmd.Dir = buildDir
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH=amd64",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	b.log.Info("compiling plugin", zap.String("dir", buildDir))

	if err := cmd.Run(); err != nil {
		b.log.Error("compilation failed",
			zap.String("stdout", stdout.String()),
			zap.String("stderr", stderr.String()),
			zap.Error(err),
		)
		return fmt.Errorf("go build failed: %s", stderr.String())
	}

	b.log.Info("compilation successful", zap.String("output", outputPath))
	return nil
}

// uploadFile uploads a local file to MinIO.
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
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}

	b.log.Info("uploaded plugin", zap.String("object", objectKey), zap.Int64("size", stat.Size()))
	return nil
}

// reportSuccess sends a success callback to the Manager.
func (b *builder) reportSuccess(ctx context.Context) error {
	url := fmt.Sprintf("%s/builds/%s/complete", b.cfg.ManagerURL, b.cfg.JobID)

	body, err := json.Marshal(map[string]interface{}{
		"plugin_type": string(b.cfg.PluginType),
		"plugin_path": b.cfg.OutputPath,
	})
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	return b.doCallback(ctx, url, body)
}

// reportFailure sends a failure callback to the Manager.
func (b *builder) reportFailure(ctx context.Context, errMsg string) error {
	url := fmt.Sprintf("%s/builds/%s/fail", b.cfg.ManagerURL, b.cfg.JobID)

	body, err := json.Marshal(map[string]interface{}{
		"plugin_type": string(b.cfg.PluginType),
		"error":       errMsg,
	})
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	return b.doCallback(ctx, url, body)
}

// doCallback performs an HTTP POST to the Manager.
func (b *builder) doCallback(ctx context.Context, url string, body []byte) error {
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

	b.log.Info("callback successful", zap.String("url", url))
	return nil
}

// mapperMain is the generated main.go for mapper plugins.
const mapperMain = `package main

import (
	"github.com/hashicorp/go-plugin"
	mrplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: mrplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"mapper": &mrplugin.MapperPlugin{Impl: &MapperImpl{}},
		},
	})
}
`

// reducerMain is the generated main.go for reducer plugins.
const reducerMain = `package main

import (
	"github.com/hashicorp/go-plugin"
	mrplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: mrplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"reducer": &mrplugin.ReducerPlugin{Impl: &ReducerImpl{}},
		},
	})
}
`
