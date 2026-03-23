package main

import (
	"fmt"
	"os"

	database "github.com/mirstar13/go-map-reduce/sql"
)

func main() {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "error: POSTGRES_DSN environment variable is required")
		os.Exit(1)
	}

	fmt.Println("Starting database migrations...")

	if err := database.RunMigrations(dsn); err != nil {
		fmt.Fprintf(os.Stderr, "error: migration failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Migrations completed successfully.")
}
