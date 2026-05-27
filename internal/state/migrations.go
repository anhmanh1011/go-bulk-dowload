package state

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed migrations/001_init.sql
var migration001 string

const pragmas = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -64000;
PRAGMA temp_store = MEMORY;
PRAGMA busy_timeout = 5000;
`

func applyMigrations(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, pragmas); err != nil {
		return fmt.Errorf("exec pragmas: %w", err)
	}
	if _, err := db.ExecContext(ctx, migration001); err != nil {
		return fmt.Errorf("exec migration 001: %w", err)
	}
	return nil
}
