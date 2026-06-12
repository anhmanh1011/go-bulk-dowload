package state

import (
	"context"
	"fmt"
	"strings"
)

// FilterUnforwarded returns the subset of ids not yet present in the
// `forwarded` table, preserving input order. Used by the forward command to
// skip messages already mirrored on a previous (possibly interrupted) run.
// An empty input returns an empty slice without touching the DB.
func (s *Store) FilterUnforwarded(ctx context.Context, ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	// Build an IN (...) set of the candidate ids and select those that already
	// exist, then return the complement. One round-trip; ids per page ≤ 100 so
	// the placeholder list stays small.
	placeholders := strings.Repeat("?,", len(ids))
	placeholders = placeholders[:len(placeholders)-1]
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT msg_id FROM forwarded WHERE msg_id IN ("+placeholders+")", args...)
	if err != nil {
		return nil, fmt.Errorf("select forwarded: %w", err)
	}
	defer rows.Close()
	done := make(map[int64]struct{})
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan forwarded: %w", err)
		}
		done[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate forwarded: %w", err)
	}
	out := make([]int64, 0, len(ids)-len(done))
	for _, id := range ids {
		if _, ok := done[id]; !ok {
			out = append(out, id)
		}
	}
	return out, nil
}

// MarkForwarded records ids as forwarded at unix time `at`. Uses INSERT OR
// IGNORE so re-marking an already-recorded id is a no-op (idempotent). Call
// only after the corresponding forwardMessages RPC succeeds.
func (s *Store) MarkForwarded(ctx context.Context, ids []int64, at int64) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin mark forwarded: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx,
		"INSERT OR IGNORE INTO forwarded (msg_id, forwarded_at) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("prepare mark forwarded: %w", err)
	}
	defer stmt.Close()
	for _, id := range ids {
		if _, err := stmt.ExecContext(ctx, id, at); err != nil {
			return fmt.Errorf("insert forwarded %d: %w", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit mark forwarded: %w", err)
	}
	return nil
}
