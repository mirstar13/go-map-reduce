package database

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// Embed all migration files into the binary at compile time.
// This means no external SQL files are needed at runtime —
// the binary carries its own migrations.
//
//go:embed schema/*.sql
var migrations embed.FS

// Connect creates a pgxpool connection pool and runs any pending
// Goose migrations before returning. Call this once at service startup.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("database: parse DSN: %w", err)
	}

	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("database: open pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database: ping failed: %w", err)
	}

	sqlDB := stdlib.OpenDB(*cfg.ConnConfig)
	defer sqlDB.Close()

	if err := runMigrations(sqlDB); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database: migrations failed: %w", err)
	}

	return pool, nil
}

// runMigrations applies any pending Goose Up migrations from the
// embedded FS. Safe to call every time the service starts — Goose
// is idempotent and skips already-applied migrations.
func runMigrations(db *sql.DB) error {
	goose.SetBaseFS(migrations)

	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	return goose.Up(db, "migrations")
}
