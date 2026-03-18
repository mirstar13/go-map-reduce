package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/cors"
	fiberlog "github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"go.uber.org/zap"

	"github.com/mirstar13/go-map-reduce/pkg/middleware/auth"
	"github.com/mirstar13/go-map-reduce/pkg/middleware/rbac"
	"github.com/mirstar13/go-map-reduce/services/ui/client"
	"github.com/mirstar13/go-map-reduce/services/ui/config"
	"github.com/mirstar13/go-map-reduce/services/ui/handler"
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

	managerClient := client.NewManagerClient(cfg)

	keycloakClient := client.NewKeycloakClient(cfg)

	minioClient, err := client.NewMinioClient(cfg)
	if err != nil {
		log.Fatal("failed to init minio client", zap.Error(err))
	}

	authHandler := handler.NewAuthHandler(keycloakClient, log)
	jobHandler := handler.NewJobHandler(managerClient, log)
	fileHandler := handler.NewFileHandler(minioClient, log)
	adminHandler := handler.NewAdminHandler(managerClient, keycloakClient, log)

	app := fiber.New(fiber.Config{
		// Disable Fiber's default error page to always return JSON.
		ErrorHandler: func(c fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			var fe *fiber.Error
			if fErr, ok := err.(*fiber.Error); ok {
				code = fErr.Code
				fe = fErr
			}
			msg := "internal server error"
			if fe != nil {
				msg = fe.Message
			}
			return c.Status(code).JSON(fiber.Map{"error": msg})
		},
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		BodyLimit:    32 * 1024 * 1024,
	})

	app.Use(recover.New())
	app.Use(fiberlog.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: []string{"*"},
		AllowHeaders: []string{"Origin", "Content-Type", "Authorization"},
		AllowMethods: []string{"GET", "POST", "DELETE", "OPTIONS"},
	}))

	jwtMiddleware := auth.New(auth.Config{
		KeycloakURL: cfg.KeycloakURL,
		Realm:       cfg.KeycloakRealm,
		ClientID:    cfg.KeycloakClientID,
	})

	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
	})

	// POST /auth/login — Keycloak direct-grant proxy; no JWT needed.
	app.Post("/auth/login", authHandler.Login)

	api := app.Group("", jwtMiddleware)

	// Jobs (accessible by any authenticated user; Manager enforces ownership).
	jobs := api.Group("/jobs", rbac.RequireAnyRole("user", "admin"))
	jobs.Get("/", jobHandler.ListJobs)
	jobs.Post("/", jobHandler.SubmitJob)
	jobs.Get("/:id", jobHandler.GetJob)
	jobs.Delete("/:id", jobHandler.CancelJob)
	jobs.Get("/:id/output", jobHandler.GetJobOutput)

	// File uploads (accessible by any authenticated user).
	files := api.Group("/files", rbac.RequireAnyRole("user", "admin"))
	files.Post("/input", fileHandler.UploadInput)
	files.Post("/code", fileHandler.UploadCode)

	// Admin routes — admin role required.
	admin := api.Group("/admin", rbac.RequireAdmin())
	admin.Get("/jobs", adminHandler.ListAllJobs)
	admin.Get("/users", adminHandler.ListUsers)
	admin.Post("/users", adminHandler.CreateUser)
	admin.Get("/users/:id", adminHandler.GetUser)
	admin.Delete("/users/:id", adminHandler.DeleteUser)
	admin.Post("/users/:id/roles", adminHandler.AssignRole)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		addr := ":" + cfg.Port
		log.Info("UI service starting", zap.String("addr", addr))
		if err := app.Listen(addr); err != nil {
			log.Fatal("server error", zap.Error(err))
		}
	}()

	<-quit
	log.Info("shutting down UI service…")
	if err := app.Shutdown(); err != nil {
		log.Error("shutdown error", zap.Error(err))
	}
	log.Info("UI service stopped")
}
