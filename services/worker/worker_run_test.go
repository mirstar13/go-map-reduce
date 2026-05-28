package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/services/worker/config"
)

func buildDummyPlugin(t *testing.T, src string) []byte {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "main.go")
	require.NoError(t, os.WriteFile(srcFile, []byte(src), 0644))

	binFile := filepath.Join(tmpDir, "plugin")
	cmd := exec.Command("go", "build", "-o", binFile, srcFile)

	// Set cmd.Dir to current package to ensure we're inside the module
	wd, err := os.Getwd()
	require.NoError(t, err)
	cmd.Dir = wd

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to build plugin: %s", string(out))

	binData, err := os.ReadFile(binFile)
	require.NoError(t, err)
	return binData
}

const dummyMapperSrc = `package main

import (
	"github.com/hashicorp/go-plugin"
	mplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: mplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"mapper": &mplugin.MapperPlugin{
				Impl: mplugin.MapperFunc(func(key, value string) ([]mplugin.Record, error) {
					return []mplugin.Record{{Key: "M-" + key, Value: value}}, nil
				}),
			},
		},
	})
}
`

const dummyReducerSrc = `package main

import (
	"strings"

	"github.com/hashicorp/go-plugin"
	mplugin "github.com/mirstar13/go-map-reduce/pkg/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: mplugin.Handshake,
		Plugins: map[string]plugin.Plugin{
			"reducer": &mplugin.ReducerPlugin{
				Impl: mplugin.ReducerFunc(func(key string, values []string) ([]mplugin.Record, error) {
					return []mplugin.Record{{Key: "R-" + key, Value: strings.Join(values, ",")}}, nil
				}),
			},
		},
	})
}
`

func TestRunMap(t *testing.T) {
	mapperBin := buildDummyPlugin(t, dummyMapperSrc)

	var putObjects = make(map[string][]byte)
	var callbackCalled bool

	// MinIO mock server
	minioTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("MinIO mock got %s %s", r.Method, r.URL.String())
		if r.Method == http.MethodGet {
			w.Header().Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
			if strings.Contains(r.URL.RawQuery, "location") {
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
				return
			}
			if strings.Contains(r.URL.Path, "my-mapper.exe") {
				w.Write(mapperBin)
				return
			}
			if strings.Contains(r.URL.Path, "my-input") {
				// if range header is present, we could parse it, but for our test just return input
				w.Write([]byte("line1\nline2\n"))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		} else if r.Method == http.MethodPut {
			body, _ := io.ReadAll(r.Body)
			putObjects[r.URL.Path] = body
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer minioTs.Close()

	// Manager callback mock server
	managerTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Contains(t, r.URL.Path, "/tasks/map/task-1/complete")
		callbackCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer managerTs.Close()

	endpoint := strings.TrimPrefix(minioTs.URL, "http://")
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("access", "secret", ""),
		Secure: false,
	})
	require.NoError(t, err)

	log, _ := zap.NewDevelopment()

	w := &worker{
		cfg: &config.Config{
			TaskID:      "task-1",
			TaskType:    config.TaskTypeMap,
			JobID:       "job-1",
			TaskIndex:   0,
			NumReducers: 2,
			MapperPath:  "my-mapper.exe",
			InputSpec: &config.InputSpec{
				File:   "my-input",
				Offset: 0,
				Length: 100,
			},
			MinioEndpoint:    endpoint,
			MinioBucketCode:  "code",
			MinioBucketInput: "input",
			MinioBucketJobs:  "jobs",
			ManagerURL:       managerTs.URL,
		},
		minio:  minioClient,
		log:    log,
		tmpDir: t.TempDir(),
	}

	ctx := context.Background()
	err = w.runMap(ctx)
	require.NoError(t, err)

	assert.True(t, callbackCalled)
	assert.NotEmpty(t, putObjects)

	// verify that the partitions were uploaded
	// we expect keys to be something like jobs/job-1/map-0-reduce-x.txt
	// The mapper produces M-0, M-1 etc.
	// Hash of M-0 % 2 etc will determine partitions.
	for path, data := range putObjects {
		assert.True(t, strings.HasPrefix(path, "/jobs/job-1/map-0-reduce-"))
		assert.True(t, strings.Contains(string(data), "M-0\tline1") || strings.Contains(string(data), "M-1\tline2"))
	}
}

func TestRunReduce(t *testing.T) {
	reducerBin := buildDummyPlugin(t, dummyReducerSrc)

	var putObjects = make(map[string][]byte)
	var callbackCalled bool

	minioTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
			if strings.Contains(r.URL.RawQuery, "location") {
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
				return
			}
			if strings.Contains(r.URL.Path, "my-reducer.exe") {
				w.Write(reducerBin)
				return
			}
			if strings.Contains(r.URL.Path, "partition-1") {
				w.Write([]byte("key1\tval1\nkey2\tval2\n"))
				return
			}
			if strings.Contains(r.URL.Path, "partition-2") {
				w.Write([]byte("key1\tval3\n"))
				return
			}
			w.WriteHeader(http.StatusNotFound)
		} else if r.Method == http.MethodPut {
			body, _ := io.ReadAll(r.Body)
			putObjects[r.URL.Path] = body
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer minioTs.Close()

	managerTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Contains(t, r.URL.Path, "/tasks/reduce/task-2/complete")
		callbackCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer managerTs.Close()

	endpoint := strings.TrimPrefix(minioTs.URL, "http://")
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("access", "secret", ""),
		Secure: false,
	})
	require.NoError(t, err)

	log, _ := zap.NewDevelopment()

	w := &worker{
		cfg: &config.Config{
			TaskID:      "task-2",
			TaskType:    config.TaskTypeReduce,
			JobID:       "job-1",
			TaskIndex:   1,
			ReducerPath: "my-reducer.exe",
			InputLocations: []config.InputLocation{
				{Path: "partition-1"},
				{Path: "partition-2"},
			},
			MinioEndpoint:     endpoint,
			MinioBucketCode:   "code",
			MinioBucketJobs:   "jobs",
			MinioBucketOutput: "output",
			ManagerURL:        managerTs.URL,
		},
		minio:  minioClient,
		log:    log,
		tmpDir: t.TempDir(),
	}

	ctx := context.Background()
	err = w.runReduce(ctx)
	require.NoError(t, err)

	assert.True(t, callbackCalled)
	assert.NotEmpty(t, putObjects)

	// verify that the output was uploaded
	// we expect the output path to be /output/job-1/part-1.txt
	outputData, ok := putObjects["/output/job-1/part-1.txt"]
	assert.True(t, ok)

	outputStr := string(outputData)
	// key1 should have val1,val3
	// key2 should have val2
	assert.Contains(t, outputStr, "R-key1\tval1,val3")
	assert.Contains(t, outputStr, "R-key2\tval2")
}

func TestReportFailure(t *testing.T) {
	var managerURL string
	var callbackCalled bool

	managerTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		managerURL = r.URL.Path
		callbackCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer managerTs.Close()

	w := &worker{
		cfg: &config.Config{
			TaskID:     "task-err",
			TaskType:   config.TaskTypeMap,
			ManagerURL: managerTs.URL,
		},
		log: zap.NewNop(),
	}

	err := w.reportFailure(context.Background())
	require.NoError(t, err)
	assert.True(t, callbackCalled)
	assert.Equal(t, "/tasks/map/task-err/fail", managerURL)

	// test reduce failure
	callbackCalled = false
	w.cfg.TaskType = config.TaskTypeReduce
	err = w.reportFailure(context.Background())
	require.NoError(t, err)
	assert.True(t, callbackCalled)
	assert.Equal(t, "/tasks/reduce/task-err/fail", managerURL)
}

func TestDoCallback_Error(t *testing.T) {
	managerTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer managerTs.Close()

	w := &worker{
		log: zap.NewNop(),
	}

	err := w.doCallback(context.Background(), managerTs.URL, []byte("{}"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
	assert.Contains(t, err.Error(), "internal error")
}
