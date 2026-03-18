package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	fiberlog "github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/mirstar13/go-map-reduce/services/manager/config"
	"github.com/mirstar13/go-map-reduce/services/manager/dispatcher"
	"github.com/mirstar13/go-map-reduce/services/manager/handler"
	"github.com/mirstar13/go-map-reduce/services/manager/splitter"
	"github.com/mirstar13/go-map-reduce/services/manager/supervisor"
	"github.com/mirstar13/go-map-reduce/services/manager/watchdog"
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
	log.Info("manager starting",
		zap.String("replica", cfg.MyReplicaName),
		zap.String("port", cfg.Port),
	)

	pool, err := pgxpool.New(context.Background(), cfg.PostgresDSN)
	if err != nil {
		log.Fatal("connect to postgres", zap.Error(err))
	}
	defer pool.Close()

	sqlDB := stdlib.OpenDBFromPool(pool)
	queries := db.New(sqlDB)

	spl, err := splitter.New(cfg)
	if err != nil {
		log.Fatal("init splitter", zap.Error(err))
	}

	disp, err := dispatcher.New(cfg)
	if err != nil {
		log.Fatal("init dispatcher", zap.Error(err))
	}

	registry := supervisor.NewRegistry()

	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	// Called from the job handler (new job) and from startup recovery.
	launchSupervisor := func(job db.Job) {
		sup := supervisor.New(job, queries, spl, disp, cfg, log, registry)
		go sup.Run(rootCtx)
	}

	// Re-attach to any in-flight jobs that were owned by this replica
	// before the last restart (pod crash, rolling update, etc.).
	activeJobs, err := queries.GetActiveJobsByReplica(context.Background(), cfg.MyReplicaName)
	if err != nil {
		log.Fatal("startup recovery: query active jobs", zap.Error(err))
	}
	log.Info("startup recovery", zap.Int("active_jobs", len(activeJobs)))
	for _, job := range activeJobs {
		j := job // capture loop variable
		launchSupervisor(j)
	}

	wd := watchdog.New(queries, disp, registry, cfg, log)
	go wd.Run(rootCtx)

	jobHandler := handler.NewJobHandler(queries, registry, spl, disp, cfg, log, launchSupervisor)
	taskHandler := handler.NewTaskHandler(queries, registry, log)

	app := fiber.New(fiber.Config{
		ErrorHandler: func(c fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			msg := "internal server error"
			if fe, ok := err.(*fiber.Error); ok {
				code = fe.Code
				msg = fe.Message
			}
			return c.Status(code).JSON(fiber.Map{"error": msg})
		},
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	})

	app.Use(recover.New())
	app.Use(fiberlog.New())

	// The Manager is an internal service. It trusts the X-User-* headers that
	// the UI service injects after validating the JWT — it does not re-validate
	// the token itself. This is safe because the Manager's ClusterIP service is
	// not reachable from outside the cluster.
	internalAuth := auth.NewInternal()

	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok", "replica": cfg.MyReplicaName})
	})

	api := app.Group("", internalAuth)

	api.Post("/jobs", jobHandler.SubmitJob)
	api.Get("/jobs", jobHandler.ListJobs)
	api.Get("/jobs/:id", jobHandler.GetJob)
	api.Post("/jobs/:id/cancel", jobHandler.CancelJob)
	api.Get("/jobs/:id/output", jobHandler.GetJobOutput)

	// Admin: all jobs regardless of owner
	api.Get("/admin/jobs", jobHandler.AdminListJobs)

	// Workers call these directly. No user auth — they are internal pod-to-pod
	// calls within the cluster network.
	app.Post("/tasks/map/:id/complete", taskHandler.CompleteMapTask)
	app.Post("/tasks/map/:id/fail", taskHandler.FailMapTask)
	app.Post("/tasks/reduce/:id/complete", taskHandler.CompleteReduceTask)
	app.Post("/tasks/reduce/:id/fail", taskHandler.FailReduceTask)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		addr := ":" + cfg.Port
		log.Info("manager listening", zap.String("addr", addr))
		if err := app.Listen(addr); err != nil {
			log.Fatal("server error", zap.Error(err))
		}
	}()

	<-quit
	log.Info("shutting down manager…")
	rootCancel() // stop all supervisors and the watchdog

	if err := app.Shutdown(); err != nil {
		log.Error("shutdown error", zap.Error(err))
	}
	log.Info("manager stopped")
}
