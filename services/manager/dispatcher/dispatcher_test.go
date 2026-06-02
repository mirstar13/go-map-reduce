package dispatcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/mirstar13/go-map-reduce/services/manager/config"
)

func TestDispatcher_DispatchMap(t *testing.T) {
	is := assert.New(t)
	must := require.New(t)

	var createdJob *batchv1.Job
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/jobs") {
			var job batchv1.Job
			must.NoError(json.NewDecoder(r.Body).Decode(&job))
			createdJob = &job
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(job)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/build") {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	restCfg := &rest.Config{Host: ts.URL}
	clientset, err := kubernetes.NewForConfig(restCfg)
	must.NoError(err)

	cfg := &config.Config{
		WorkerNamespace:     "default",
		WorkerImage:         "worker:latest",
		ManagerURL:          "http://manager:8080",
		MinioEndpoint:       "minio:9000",
		MinioBucketInput:    "input",
		MinioBucketCode:     "code",
		MinioBucketJobs:     "jobs",
		WorkerCPURequest:    "500m",
		WorkerMemoryRequest: "512Mi",
		WorkerCPULimit:      "500m",
		WorkerMemoryLimit:   "512Mi",
	}

	d := &Dispatcher{
		k8s: clientset,
		cfg: cfg,
	}

	spec := MapTaskSpec{
		TaskID:      "task-12345678",
		JobID:       "job-111",
		TaskIndex:   2,
		InputFile:   "test.txt",
		InputOffset: 100,
		InputLength: 50,
		MapperPath:  "mapper.so",
		NumReducers: 3,
	}

	jobName, err := d.DispatchMap(context.Background(), spec)
	must.NoError(err)
	is.Contains(jobName, "map-task-123-2")

	must.NotNil(createdJob)

	// Verify env vars
	env := createdJob.Spec.Template.Spec.Containers[0].Env
	envMap := make(map[string]string)
	for _, e := range env {
		if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil {
			envMap[e.Name] = "secret-" + e.ValueFrom.SecretKeyRef.Key
		} else {
			envMap[e.Name] = e.Value
		}
	}

	is.Equal("task-12345678", envMap["TASK_ID"])
	is.Equal("map", envMap["TASK_TYPE"])
	is.Equal("2", envMap["TASK_INDEX"])
	is.Equal("mapper.so", envMap["MAPPER_PATH"])
	is.Equal("3", envMap["NUM_REDUCERS"])

	var inputSpec map[string]interface{}
	must.NoError(json.Unmarshal([]byte(envMap["INPUT_PATH"]), &inputSpec))
	is.Equal("test.txt", inputSpec["file"])
	is.Equal(float64(100), inputSpec["offset"])
	is.Equal(float64(50), inputSpec["length"])

	is.Equal("secret-MINIO_ACCESS_KEY", envMap["MINIO_ACCESS_KEY"])
	is.Equal("secret-MINIO_SECRET_KEY", envMap["MINIO_SECRET_KEY"])
}

func TestDispatcher_DispatchReduce(t *testing.T) {
	is := assert.New(t)
	must := require.New(t)

	var createdJob *batchv1.Job
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/jobs") {
			var job batchv1.Job
			must.NoError(json.NewDecoder(r.Body).Decode(&job))
			createdJob = &job
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(job)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/build") {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	restCfg := &rest.Config{Host: ts.URL}
	clientset, err := kubernetes.NewForConfig(restCfg)
	must.NoError(err)

	cfg := &config.Config{
		WorkerNamespace:     "default",
		WorkerImage:         "worker:latest",
		ManagerURL:          "http://manager:8080",
		MinioEndpoint:       "minio:9000",
		MinioBucketOutput:   "output",
		WorkerCPURequest:    "500m",
		WorkerMemoryRequest: "512Mi",
		WorkerCPULimit:      "500m",
		WorkerMemoryLimit:   "512Mi",
	}

	d := &Dispatcher{
		k8s: clientset,
		cfg: cfg,
	}

	spec := ReduceTaskSpec{
		TaskID:         "task-87654321",
		JobID:          "job-222",
		TaskIndex:      1,
		ReducerPath:    "reducer.so",
		InputLocations: []byte(`[{"reducer_index":1,"path":"output.txt"}]`),
	}

	jobName, err := d.DispatchReduce(context.Background(), spec)
	must.NoError(err)
	is.Contains(jobName, "red-task-876-1")

	must.NotNil(createdJob)

	env := createdJob.Spec.Template.Spec.Containers[0].Env
	envMap := make(map[string]string)
	for _, e := range env {
		envMap[e.Name] = e.Value
	}

	is.Equal("reduce", envMap["TASK_TYPE"])
	is.Equal("reducer.so", envMap["REDUCER_PATH"])
	is.Equal(`[{"reducer_index":1,"path":"output.txt"}]`, envMap["INPUT_LOCATIONS"])
}

func TestDispatcher_DispatchBuild(t *testing.T) {
	is := assert.New(t)
	must := require.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/build") {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	restCfg := &rest.Config{Host: ts.URL}
	clientset, err := kubernetes.NewForConfig(restCfg)
	must.NoError(err)

	cfg := &config.Config{
		WorkerNamespace: "default",
		BuilderImage:    "builder:latest",
		ManagerURL:      "http://manager:8080",
		BuilderURL:      ts.URL, // Use the mock server URL
		MinioEndpoint:   "minio:9000",
		MinioBucketCode: "code",
	}

	d := &Dispatcher{
		k8s: clientset,
		cfg: cfg,
	}

	spec := BuildTaskSpec{
		JobID:      "job-abcdefgh123",
		PluginType: "mapper",
		SourcePath: "source.go",
		OutputPath: "mapper.so",
	}

	jobName, err := d.DispatchBuild(context.Background(), spec)
	must.NoError(err)
	is.Equal("build-mapper-job-", jobName)
}

func TestDispatcher_DeleteJob(t *testing.T) {
	is := assert.New(t)
	must := require.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/jobs/test-job") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"jobs.batch not found","reason":"NotFound","code":404}`))
	}))
	defer ts.Close()

	restCfg := &rest.Config{Host: ts.URL}
	clientset, err := kubernetes.NewForConfig(restCfg)
	must.NoError(err)

	cfg := &config.Config{
		WorkerNamespace: "default",
	}

	d := &Dispatcher{
		k8s: clientset,
		cfg: cfg,
	}

	err = d.DeleteJob(context.Background(), "test-job")
	must.NoError(err)

	// Test not found
	err = d.DeleteJob(context.Background(), "missing-job")
	is.NoError(err) // Should ignore not found errors!
}
