package watchdog

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/services/manager/config"
	"github.com/mirstar13/go-map-reduce/services/manager/dispatcher"
	"github.com/mirstar13/go-map-reduce/services/manager/supervisor"
)

// Watchdog runs as a background goroutine scanning for tasks that have been
// in the RUNNING state longer than TaskTimeoutSeconds. Stale tasks are reset
// to PENDING (retry count incremented) so the supervisor can re-dispatch them.
type Watchdog struct {
	queries  db.Querier
	disp     *dispatcher.Dispatcher
	registry *supervisor.Registry
	cfg      *config.Config
	log      *zap.Logger
}

// New creates a Watchdog.
func New(
	queries db.Querier,
	disp *dispatcher.Dispatcher,
	registry *supervisor.Registry,
	cfg *config.Config,
	log *zap.Logger,
) *Watchdog {
	return &Watchdog{
		queries:  queries,
		disp:     disp,
		registry: registry,
		cfg:      cfg,
		log:      log,
	}
}

// Run starts the watchdog loop. It ticks every 30 seconds and returns when
// ctx is cancelled.
func (w *Watchdog) Run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	w.log.Info("watchdog started",
		zap.Int("timeout_seconds", w.cfg.TaskTimeoutSeconds),
		zap.Int("max_retries", w.cfg.TaskMaxRetries),
	)

	for {
		select {
		case <-ctx.Done():
			w.log.Info("watchdog stopped")
			return
		case <-ticker.C:
			w.scan(ctx)
		}
	}
}

// scan performs one pass over stale map and reduce tasks.
func (w *Watchdog) scan(ctx context.Context) {
	threshold := sql.NullString{
		String: fmt.Sprintf("%d", w.cfg.TaskTimeoutSeconds),
		Valid:  true,
	}

	staleMaps, err := w.queries.GetStaleRunningMapTasks(ctx, threshold)
	if err != nil {
		w.log.Error("watchdog: get stale map tasks", zap.Error(err))
	} else {
		for _, task := range staleMaps {
			w.log.Warn("stale map task detected",
				zap.String("task_id", task.TaskID.String()),
				zap.String("job_id", task.JobID.String()),
				zap.Int32("retry_count", task.RetryCount),
				zap.String("k8s_job", task.K8sJobName.String),
			)

			// Delete the hung K8s Job (best effort).
			if task.K8sJobName.Valid {
				if err := w.disp.DeleteJob(ctx, task.K8sJobName.String); err != nil {
					w.log.Error("watchdog: delete stale k8s map job", zap.Error(err))
				}
			}

			// Reset to PENDING so the supervisor re-dispatches it.
			if err := w.queries.IncrementMapTaskRetry(ctx, task.TaskID); err != nil {
				w.log.Error("watchdog: increment map task retry", zap.Error(err))
				continue
			}

			// Poke the owning job's supervisor for an immediate re-dispatch.
			w.registry.Notify(task.JobID)
		}
	}

	staleReduces, err := w.queries.GetStaleRunningReduceTasks(ctx, threshold)
	if err != nil {
		w.log.Error("watchdog: get stale reduce tasks", zap.Error(err))
	} else {
		for _, task := range staleReduces {
			w.log.Warn("stale reduce task detected",
				zap.String("task_id", task.TaskID.String()),
				zap.String("job_id", task.JobID.String()),
				zap.Int32("retry_count", task.RetryCount),
			)

			if task.K8sJobName.Valid {
				if err := w.disp.DeleteJob(ctx, task.K8sJobName.String); err != nil {
					w.log.Error("watchdog: delete stale k8s reduce job", zap.Error(err))
				}
			}

			if err := w.queries.IncrementReduceTaskRetry(ctx, task.TaskID); err != nil {
				w.log.Error("watchdog: increment reduce task retry", zap.Error(err))
				continue
			}

			w.registry.Notify(task.JobID)
		}
	}
}
