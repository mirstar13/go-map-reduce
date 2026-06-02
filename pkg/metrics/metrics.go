package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsTotal tracks the total number of MapReduce jobs by status.
	JobsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mapreduce_manager_jobs_total",
			Help: "Total number of MapReduce jobs by status.",
		},
		[]string{"status"},
	)

	// JobDuration tracks the duration of MapReduce jobs in seconds.
	JobDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mapreduce_manager_job_duration_seconds",
			Help:    "Duration of MapReduce jobs in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"status"},
	)

	// TasksTotal tracks the total number of MapReduce tasks by type and status.
	TasksTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mapreduce_worker_tasks_total",
			Help: "Total number of MapReduce tasks by type and status.",
		},
		[]string{"type", "status"},
	)

	// TaskDuration tracks the duration of MapReduce tasks in seconds.
	TaskDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mapreduce_worker_task_duration_seconds",
			Help:    "Duration of MapReduce tasks in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"type", "status"},
	)

	// TaskBytesTotal tracks the total bytes processed by MapReduce tasks.
	TaskBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mapreduce_worker_task_bytes_total",
			Help: "Total bytes processed by MapReduce tasks by type (e.g., map_input, map_output).",
		},
		[]string{"type"},
	)

	// ShuffleBytesTotal tracks the total bytes transferred through the shuffle service.
	ShuffleBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mapreduce_shuffle_bytes_total",
			Help: "Total bytes transferred through the shuffle service by direction (push, pull).",
		},
		[]string{"direction"},
	)
)
