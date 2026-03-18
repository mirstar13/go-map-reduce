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

// Embed all Goose migration files into the binary at compile time.
// The embed path is relative to this file (sql/db.go), so "schema/*.sql"
// picks up sql/schema/001_jobs.sql, 002_map_tasks.sql, 003_reduce_tasks.sql.
// The embedded FS therefore contains a top-level directory named "schema".
//
//go:embed schema/*.sql
var migrations embed.FS

// Connect creates a pgxpool connection pool and returns it.
// It does NOT run migrations — use RunMigrations for that (called by the
// migrate Job before any application pods start).
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

	return pool, nil
}

// RunMigrations applies any pending Goose Up migrations from the embedded FS.
// Safe to call every time the migrate Job runs — Goose tracks applied
// migrations in the goose_db_version table and skips already-applied ones.
func RunMigrations(dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("database: open for migrations: %w", err)
	}
	defer db.Close()

	// Verify the connection before attempting migrations.
	if err := db.Ping(); err != nil {
		return fmt.Errorf("database: ping for migrations: %w", err)
	}

	goose.SetBaseFS(migrations)

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("database: set goose dialect: %w", err)
	}

	// "schema" matches the top-level directory produced by the embed directive.
	if err := goose.Up(db, "schema"); err != nil {
		return fmt.Errorf("database: goose up: %w", err)
	}

	return nil
}

// MustConnect is a convenience wrapper that calls Connect and panics on error.
// Use only in main() where a failed DB connection is unrecoverable.
func MustConnect(ctx context.Context, dsn string) *pgxpool.Pool {
	pool, err := Connect(ctx, dsn)
	if err != nil {
		panic(fmt.Sprintf("database: fatal connect error: %v", err))
	}
	return pool
}

// OpenStdlib opens a *sql.DB from the pool config.
// Needed by sqlc's database/sql driver.
func OpenStdlib(pool *pgxpool.Pool) *sql.DB {
	return stdlib.OpenDBFromPool(pool)
}
