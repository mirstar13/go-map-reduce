package main

import (
	"context"
	"io"
	"net"
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
	"google.golang.org/grpc"
	"github.com/mirstar13/go-map-reduce/pkg/shuffle"
	"github.com/mirstar13/go-map-reduce/services/worker/config"
)

type mockShuffleClient struct {
	pushFn    func(ctx context.Context, opts ...grpc.CallOption) (shuffle.ShuffleService_PushClient, error)
	pullFn    func(ctx context.Context, in *shuffle.PullRequest, opts ...grpc.CallOption) (shuffle.ShuffleService_PullClient, error)
	cleanupFn func(ctx context.Context, in *shuffle.CleanupRequest, opts ...grpc.CallOption) (*shuffle.CleanupResponse, error)
}

func (m *mockShuffleClient) Push(ctx context.Context, opts ...grpc.CallOption) (shuffle.ShuffleService_PushClient, error) {
	return m.pushFn(ctx, opts...)
}
func (m *mockShuffleClient) Pull(ctx context.Context, in *shuffle.PullRequest, opts ...grpc.CallOption) (shuffle.ShuffleService_PullClient, error) {
	return m.pullFn(ctx, in, opts...)
}
func (m *mockShuffleClient) Cleanup(ctx context.Context, in *shuffle.CleanupRequest, opts ...grpc.CallOption) (*shuffle.CleanupResponse, error) {
	if m.cleanupFn != nil {
		return m.cleanupFn(ctx, in, opts...)
	}
	return &shuffle.CleanupResponse{Success: true}, nil
}

type mockPushClient struct {
	grpc.ClientStream
	sendFn  func(*shuffle.PushRequest) error
	closeFn func() (*shuffle.PushResponse, error)
}

func (m *mockPushClient) Send(req *shuffle.PushRequest) error         { return m.sendFn(req) }
func (m *mockPushClient) CloseAndRecv() (*shuffle.PushResponse, error) { return m.closeFn() }

type mockPullClient struct {
	grpc.ClientStream
	recvFn func() (*shuffle.PullResponse, error)
}

func (m *mockPullClient) Recv() (*shuffle.PullResponse, error) { return m.recvFn() }

func buildDummyPlugin(t *testing.T, src string) []byte {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "main.go")
	require.NoError(t, os.WriteFile(srcFile, []byte(src), 0644))

	binFile := filepath.Join(tmpDir, "plugin")
	cmd := exec.Command("go", "build", "-o", binFile, srcFile)

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
				Impl: mplugin.MapperFunc(func(inputs []mplugin.MapInput) ([]mplugin.Record, error) {
					var records []mplugin.Record
					for _, input := range inputs {
						records = append(records, mplugin.Record{Key: "M-" + input.Key, Value: input.Value})
					}
					return records, nil
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
				Impl: mplugin.ReducerFunc(func(inputs []mplugin.ReduceInput) ([]mplugin.Record, error) {
					var records []mplugin.Record
					for _, input := range inputs {
						records = append(records, mplugin.Record{Key: "R-" + input.Key, Value: strings.Join(input.Values, ",")})
					}
					return records, nil
				}),
			},
		},
	})
}
`

func TestRunMap(t *testing.T) {
	mapperBin := buildDummyPlugin(t, dummyMapperSrc)

	var pushedRecords []string
	var callbackCalled bool

	minioTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("ETag", "\"mock-etag\"")
		_ = os.WriteFile("debug_paths.txt", []byte(r.Method+" "+r.URL.Path+"\n"), 0644)
		if r.Method == http.MethodGet {
			if strings.Contains(r.URL.RawQuery, "location") {
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
				return
			}
			if strings.Contains(r.URL.Path, "my-mapper.exe") {
				w.Write(mapperBin)
				return
			}
			if strings.Contains(r.URL.Path, "my-input") {
				w.Write([]byte("line1\nline2\n"))
				return
			}
		}
	}))
	defer minioTs.Close()

	managerTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	mockShuffle := &mockShuffleClient{
		pushFn: func(ctx context.Context, opts ...grpc.CallOption) (shuffle.ShuffleService_PushClient, error) {
			return &mockPushClient{
				sendFn: func(req *shuffle.PushRequest) error {
					pushedRecords = append(pushedRecords, string(req.Data))
					return nil
				},
				closeFn: func() (*shuffle.PushResponse, error) {
					return &shuffle.PushResponse{Success: true}, nil
				},
			}, nil
		},
	}

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
		minio:         minioClient,
		log:           log,
		tmpDir:        t.TempDir(),
		shuffleClient: mockShuffle,
	}

	ctx := context.Background()
	err = w.runMap(ctx)
	require.NoError(t, err)

	assert.True(t, callbackCalled)
	assert.Len(t, pushedRecords, 2)
}

type shuffleServer struct {
	shuffle.UnimplementedShuffleServiceServer
}

func (s *shuffleServer) Pull(req *shuffle.PullRequest, stream shuffle.ShuffleService_PullServer) error {
	return stream.Send(&shuffle.PullResponse{Data: []byte("key1\tval1\nkey2\tval2\n")})
}

func TestRunReduce(t *testing.T) {
	reducerBin := buildDummyPlugin(t, dummyReducerSrc)

	var putObjects = make(map[string][]byte)
	var callbackCalled bool

	minioTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("ETag", "\"mock-etag\"")
		_ = os.WriteFile("debug_paths.txt", []byte(r.Method+" "+r.URL.Path+"\n"), 0644)
		if r.Method == http.MethodGet {
			if strings.Contains(r.URL.RawQuery, "location") {
				w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
				return
			}
			if strings.Contains(r.URL.Path, "my-reducer.exe") {
				w.Write(reducerBin)
				return
			}
		} else if r.Method == http.MethodPut {
			body, _ := io.ReadAll(r.Body)
			putObjects[r.URL.Path] = body
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer minioTs.Close()

	managerTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer managerTs.Close()

	// Start a real local gRPC server for shuffle service
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	shuffle.RegisterShuffleServiceServer(grpcServer, &shuffleServer{})
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()
	addr := lis.Addr().String()

	endpoint := strings.TrimPrefix(minioTs.URL, "http://")
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("access", "secret", ""),
		Secure: false,
	})
	require.NoError(t, err)

	log, _ := zap.NewDevelopment()

	mockShuffle := &mockShuffleClient{
		pullFn: func(ctx context.Context, in *shuffle.PullRequest, opts ...grpc.CallOption) (shuffle.ShuffleService_PullClient, error) {
			count := 0
			return &mockPullClient{
				recvFn: func() (*shuffle.PullResponse, error) {
					if count > 0 {
						return nil, io.EOF
					}
					count++
					return &shuffle.PullResponse{Data: []byte("key1\tval1\nkey2\tval2\n")}, nil
				},
			}, nil
		},
	}

	w := &worker{
		cfg: &config.Config{
			TaskID:      "task-2",
			TaskType:    config.TaskTypeReduce,
			JobID:       "job-1",
			TaskIndex:   1,
			ReducerPath: "my-reducer.exe",
			InputLocations: []config.InputLocation{
				{Path: addr},
			},
			MinioEndpoint:     endpoint,
			MinioBucketCode:   "code",
			MinioBucketJobs:   "jobs",
			MinioBucketOutput: "output",
			ManagerURL:        managerTs.URL,
		},
		minio:         minioClient,
		log:           log,
		tmpDir:        t.TempDir(),
		shuffleClient: mockShuffle,
	}

	ctx := context.Background()
	err = w.runReduce(ctx)
	require.NoError(t, err)

	assert.True(t, callbackCalled)
	assert.NotEmpty(t, putObjects)

	outputData, ok := putObjects["/output/job-1/part-1.txt"]
	assert.True(t, ok)
	assert.Contains(t, string(outputData), "R-key1\tval1")
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
