package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	// modernc.org/sqlite uses "sqlite" driver name
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Single connection avoids "database is locked" with :memory: across
	// tests; for file-based DBs WAL handles concurrency fine.
	db.SetMaxOpenConns(1)
	return &Store{db: db}, nil
}

func (s *Store) Init(ctx context.Context) error {
	if err := applyMigrations(ctx, s.db); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status=?, updated_at=? WHERE status=?`,
		string(StatusPending), time.Now().Unix(), string(StatusInProgress),
	); err != nil {
		return fmt.Errorf("reset in_progress: %w", err)
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

// InsertJobIfAbsent inserts a job using INSERT OR IGNORE — calling twice with
// the same msg_id is a no-op (idempotent). Crawler can re-run safely.
func (s *Store) InsertJobIfAbsent(ctx context.Context, j Job) error {
	_, err := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO jobs
(msg_id, chat_id, chat_access_hash, file_id, access_hash, file_reference, dc_id, size,
 file_name, mime_type, status, retries, output_path, error_msg, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '', '', ?, ?)`,
		j.MsgID, j.ChatID, j.ChatAccessHash, j.FileID, j.AccessHash, j.FileReference, j.DCID, j.Size,
		j.FileName, j.MimeType, string(StatusPending),
		j.CreatedAt.Unix(), j.UpdatedAt.Unix(),
	)
	if err != nil {
		return fmt.Errorf("insert job %d: %w", j.MsgID, err)
	}
	return nil
}

func (s *Store) PickPending(ctx context.Context, n int) ([]Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx,
		`SELECT msg_id, chat_id, chat_access_hash, file_id, access_hash, file_reference, dc_id, size,
                file_name, mime_type, retries, output_path, error_msg, created_at, updated_at
         FROM jobs WHERE status = ? ORDER BY msg_id LIMIT ?`,
		string(StatusPending), n,
	)
	if err != nil {
		return nil, fmt.Errorf("query pending: %w", err)
	}
	var picked []Job
	for rows.Next() {
		var j Job
		var created, updated int64
		if err := rows.Scan(&j.MsgID, &j.ChatID, &j.ChatAccessHash, &j.FileID, &j.AccessHash,
			&j.FileReference, &j.DCID, &j.Size, &j.FileName, &j.MimeType,
			&j.Retries, &j.OutputPath, &j.ErrorMsg, &created, &updated); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan job: %w", err)
		}
		j.Status = StatusInProgress
		j.CreatedAt = time.Unix(created, 0)
		j.UpdatedAt = time.Unix(updated, 0)
		picked = append(picked, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending rows: %w", err)
	}
	if len(picked) == 0 {
		return nil, nil
	}
	ids := make([]int64, len(picked))
	for i, j := range picked {
		ids[i] = j.MsgID
	}
	// Build placeholders dynamically (SQLite no array type)
	q := "UPDATE jobs SET status=?, updated_at=? WHERE msg_id IN ("
	args := []any{string(StatusInProgress), time.Now().Unix()}
	for i, id := range ids {
		if i > 0 {
			q += ","
		}
		q += "?"
		args = append(args, id)
	}
	q += ")"
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return nil, fmt.Errorf("mark in_progress: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit pick: %w", err)
	}
	return picked, nil
}

func (s *Store) MarkDone(ctx context.Context, msgID int64, outputPath string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status=?, output_path=?, updated_at=? WHERE msg_id=?`,
		string(StatusDone), outputPath, time.Now().Unix(), msgID,
	)
	if err != nil {
		return fmt.Errorf("mark done %d: %w", msgID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark done rows affected %d: %w", msgID, err)
	}
	if n == 0 {
		return fmt.Errorf("mark done: msg_id %d not found", msgID)
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, msgID int64, errMsg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status=?, error_msg=?, updated_at=? WHERE msg_id=?`,
		string(StatusFailed), errMsg, time.Now().Unix(), msgID,
	)
	if err != nil {
		return fmt.Errorf("mark failed %d: %w", msgID, err)
	}
	return nil
}

func (s *Store) UpdateFileReference(ctx context.Context, msgID int64, ref []byte) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET file_reference=?, updated_at=? WHERE msg_id=?`,
		ref, time.Now().Unix(), msgID,
	)
	if err != nil {
		return fmt.Errorf("update file_reference %d: %w", msgID, err)
	}
	return nil
}

func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	rows, err := s.db.QueryContext(ctx,
		`SELECT status, COUNT(*), COALESCE(SUM(size),0) FROM jobs GROUP BY status`,
	)
	if err != nil {
		return st, fmt.Errorf("stats query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count, totalSize int64
		if err := rows.Scan(&status, &count, &totalSize); err != nil {
			return st, err
		}
		switch JobStatus(status) {
		case StatusPending:
			st.Pending = count
		case StatusInProgress:
			st.InProgress = count
		case StatusDone:
			st.Done = count
			st.DoneSize = totalSize
		case StatusFailed:
			st.Failed = count
		}
		st.TotalSize += totalSize
	}
	return st, rows.Err()
}

// ResetStatus flips all jobs in `from` state back to `to` state. Used by the
// retry CLI to re-queue failed jobs.
func (s *Store) ResetStatus(ctx context.Context, from, to JobStatus) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status=?, updated_at=?, retries=0, error_msg='' WHERE status=?`,
		string(to), time.Now().Unix(), string(from),
	)
	return err
}

// ResetMsgID flips a specific msg_id back to pending. Used by the reset CLI
// for manual recovery of individual rows.
func (s *Store) ResetMsgID(ctx context.Context, msgID int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status=?, updated_at=?, retries=0, error_msg='' WHERE msg_id=?`,
		string(StatusPending), time.Now().Unix(), msgID,
	)
	return err
}

// Sentinel error for callers that want to distinguish.
var ErrNotFound = errors.New("not found")
