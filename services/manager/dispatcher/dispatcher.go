package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/mirstar13/go-map-reduce/services/manager/config"
)

// Dispatcher creates and manages Kubernetes Jobs for worker pods.
type Dispatcher struct {
	k8s *kubernetes.Clientset
	cfg *config.Config
}

// New creates a Dispatcher using the in-cluster Kubernetes config.
func New(cfg *config.Config) (*Dispatcher, error) {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("dispatcher: in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("dispatcher: build k8s clientset: %w", err)
	}
	return &Dispatcher{k8s: clientset, cfg: cfg}, nil
}

// MapTaskSpec carries everything the Dispatcher needs to launch a map worker.
type MapTaskSpec struct {
	TaskID      string
	JobID       string
	TaskIndex   int
	InputFile   string
	InputOffset int64
	InputLength int64
	MapperPath  string
	NumReducers int
}

// ReduceTaskSpec carries everything the Dispatcher needs to launch a reduce worker.
type ReduceTaskSpec struct {
	TaskID      string
	JobID       string
	TaskIndex   int
	ReducerPath string
	// InputLocations is a JSON array of {reducer_index, path} objects
	// collected from all completed map tasks for this reducer index.
	InputLocations json.RawMessage
}

// DispatchMap creates a Kubernetes Job for a map worker.
// Returns the Kubernetes Job name that should be stored in the DB.
func (d *Dispatcher) DispatchMap(ctx context.Context, spec MapTaskSpec) (string, error) {
	jobName := k8sJobName("map", spec.TaskID, spec.TaskIndex)

	inputSpec, err := json.Marshal(map[string]interface{}{
		"file":   spec.InputFile,
		"offset": spec.InputOffset,
		"length": spec.InputLength,
	})
	if err != nil {
		return "", fmt.Errorf("dispatcher: marshal input spec: %w", err)
	}

	env := []corev1.EnvVar{
		{Name: "TASK_ID", Value: spec.TaskID},
		{Name: "TASK_TYPE", Value: "map"},
		{Name: "JOB_ID", Value: spec.JobID},
		{Name: "TASK_INDEX", Value: fmt.Sprintf("%d", spec.TaskIndex)},
		{Name: "MANAGER_URL", Value: d.cfg.ManagerURL},
		// Input as a JSON spec so the worker can do a ranged GET from MinIO.
		{Name: "INPUT_PATH", Value: string(inputSpec)},
		{Name: "MAPPER_PATH", Value: spec.MapperPath},
		{Name: "NUM_REDUCERS", Value: fmt.Sprintf("%d", spec.NumReducers)},
		{Name: "MINIO_BUCKET_INPUT", Value: d.cfg.MinioBucketInput},
		{Name: "MINIO_BUCKET_CODE", Value: d.cfg.MinioBucketCode},
		{Name: "MINIO_BUCKET_JOBS", Value: d.cfg.MinioBucketJobs},
		d.minioEndpointVar(),
		d.minioAccessKeyVar(),
		d.minioSecretKeyVar(),
	}

	return jobName, d.createK8sJob(ctx, jobName, env)
}

// DispatchReduce creates a Kubernetes Job for a reduce worker.
func (d *Dispatcher) DispatchReduce(ctx context.Context, spec ReduceTaskSpec) (string, error) {
	jobName := k8sJobName("red", spec.TaskID, spec.TaskIndex)

	env := []corev1.EnvVar{
		{Name: "TASK_ID", Value: spec.TaskID},
		{Name: "TASK_TYPE", Value: "reduce"},
		{Name: "JOB_ID", Value: spec.JobID},
		{Name: "TASK_INDEX", Value: fmt.Sprintf("%d", spec.TaskIndex)},
		{Name: "MANAGER_URL", Value: d.cfg.ManagerURL},
		{Name: "REDUCER_PATH", Value: spec.ReducerPath},
		{Name: "INPUT_LOCATIONS", Value: string(spec.InputLocations)},
		{Name: "MINIO_BUCKET_CODE", Value: d.cfg.MinioBucketCode},
		{Name: "MINIO_BUCKET_JOBS", Value: d.cfg.MinioBucketJobs},
		{Name: "MINIO_BUCKET_OUTPUT", Value: d.cfg.MinioBucketOutput},
		d.minioEndpointVar(),
		d.minioAccessKeyVar(),
		d.minioSecretKeyVar(),
	}

	return jobName, d.createK8sJob(ctx, jobName, env)
}

// DeleteJob removes a Kubernetes Job and its pods (background propagation).
// Safe to call even if the job no longer exists.
func (d *Dispatcher) DeleteJob(ctx context.Context, jobName string) error {
	propagation := metav1.DeletePropagationBackground
	err := d.k8s.BatchV1().Jobs(d.cfg.WorkerNamespace).Delete(ctx, jobName, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	})
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("dispatcher: delete k8s job %s: %w", jobName, err)
	}
	return nil
}

// createK8sJob builds and submits the Kubernetes Job manifest.
func (d *Dispatcher) createK8sJob(ctx context.Context, name string, env []corev1.EnvVar) error {
	ttl := int32(300)        // auto-clean completed pod after 5 min
	backoffLimit := int32(0) // Manager handles retries, not K8s
	completions := int32(1)
	parallelism := int32(1)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: d.cfg.WorkerNamespace,
			Labels: map[string]string{
				"app":        "mapreduce-worker",
				"managed-by": "manager",
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &ttl,
			BackoffLimit:            &backoffLimit,
			Completions:             &completions,
			Parallelism:             &parallelism,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":      "mapreduce-worker",
						"job-name": name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "worker",
							Image:           d.cfg.WorkerImage,
							ImagePullPolicy: corev1.PullAlways,
							Env:             env,
						},
					},
				},
			},
		},
	}

	_, err := d.k8s.BatchV1().Jobs(d.cfg.WorkerNamespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("dispatcher: create k8s job %s: %w", name, err)
	}
	return nil
}

// k8sJobName produces a valid Kubernetes name (≤63 chars).
// Format: {prefix}-{taskID[:8]}-{index}
func k8sJobName(prefix, taskID string, index int) string {
	short := taskID
	if len(short) > 8 {
		short = short[:8]
	}
	return fmt.Sprintf("%s-%s-%d", prefix, short, index)
}

// minioEndpointVar, minioAccessKeyVar, minioSecretKeyVar inject MinIO credentials
// from the pod's environment (sourced from the mapreduce-secrets Secret).
func (d *Dispatcher) minioEndpointVar() corev1.EnvVar {
	return corev1.EnvVar{Name: "MINIO_ENDPOINT", Value: d.cfg.MinioEndpoint}
}
func (d *Dispatcher) minioAccessKeyVar() corev1.EnvVar {
	return corev1.EnvVar{
		Name: "MINIO_ACCESS_KEY",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "mapreduce-secrets"},
				Key:                  "MINIO_ACCESS_KEY",
			},
		},
	}
}
func (d *Dispatcher) minioSecretKeyVar() corev1.EnvVar {
	return corev1.EnvVar{
		Name: "MINIO_SECRET_KEY",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "mapreduce-secrets"},
				Key:                  "MINIO_SECRET_KEY",
			},
		},
	}
}

// isNotFound checks if a Kubernetes API error is a 404.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() != "" && (contains(err.Error(), "not found") || contains(err.Error(), "404"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
