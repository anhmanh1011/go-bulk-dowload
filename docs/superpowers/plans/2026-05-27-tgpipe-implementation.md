# tgpipe Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `tgpipe` — a Go binary that bulk-downloads `.txt` files from a Telegram source channel, parses lines as `url:user:pass` → extracts `email:pass`, batches into 20 MB output files, and uploads them to a target channel. End-to-end throughput target: 15–25 MB/s sustained.

**Architecture:** Single-process 6-stage pipeline (Crawler + Fetcher + Splitter + Processor + Writer + Uploader) orchestrated via `errgroup`, bounded channels between stages, SQLite for durable job state, 1 Premium Telegram account split into 2 MTProto sub-pools (6 fetch + 2 upload sessions). See `docs/superpowers/specs/2026-05-27-tgpipe-design.md`.

**Tech Stack:** Go ≥1.22, `github.com/gotd/td` (MTProto), `modernc.org/sqlite` (pure-Go SQLite, no CGO), `github.com/spf13/cobra` (CLI), `gopkg.in/yaml.v3` (config), `golang.org/x/sync/errgroup`, `log/slog` (stdlib), `github.com/stretchr/testify` (tests).

**Required skills for implementers (per `~/.claude/CLAUDE.md`):**
- @golang-project-layout, @golang-code-style, @golang-naming
- @golang-error-handling, @golang-testing, @golang-design-patterns
- @golang-concurrency, @golang-safety, @golang-security
- @golang-performance, @golang-linter
- @security-review (auth, API, sensitive data)
- @database-migrations (migrations/001_init.sql)
- @api-design (when defining package interfaces)

---

## Plan-wide conventions

**Commit message style:** Conventional commits — `feat:`, `fix:`, `test:`, `refactor:`, `chore:`, `docs:`. Scope = package name when relevant (`feat(state): atomic PickPending`).

**Test runner:** `go test -race ./...` from project root. Per-test: `go test -race -run TestName ./internal/pkg`.

**Build verify:** `go build ./...` should pass after every task (even if functionality not wired up — type signatures must compile).

**Imports policy:** Group imports: stdlib → third-party → internal. Use `goimports`-style ordering.

**Errors:** Wrap with `%w` and `errors.Join` (stdlib only). Never bare `errors.New` for non-sentinel errors.

**Naming:** Per @golang-naming — package name = directory name, no stutter (`splitter.New` not `splitter.NewSplitter`).

---

## File structure (created across all chunks)

```
.
├── .gitignore
├── README.md                              # Polished in Chunk 5
├── Makefile
├── go.mod
├── go.sum
├── config.example.yaml                    # Polished in Chunk 5
├── cmd/tgpipe/
│   ├── main.go
│   ├── cmd_auth.go                        # NEW: interactive MTProto auth
│   ├── cmd_crawl.go
│   ├── cmd_run.go
│   ├── cmd_stats.go
│   ├── cmd_retry.go
│   └── cmd_reset.go
├── internal/
│   ├── config/
│   │   ├── config.go
│   │   └── config_test.go
│   ├── logging/
│   │   └── logging.go
│   ├── types/
│   │   └── types.go                       # Chunk, Line, Record, OutputFile, JobStatus
│   ├── retry/
│   │   ├── retry.go
│   │   └── retry_test.go
│   ├── state/
│   │   ├── migrations/
│   │   │   └── 001_init.sql               # Embedded via //go:embed (no `..` allowed)
│   │   ├── store.go
│   │   ├── store_test.go
│   │   ├── migrations.go
│   │   └── types.go                       # Job, Stats
│   ├── tracker/
│   │   ├── tracker.go
│   │   └── tracker_test.go
│   ├── channels/                          # NEW: resolve channel access hashes
│   │   ├── resolver.go
│   │   └── resolver_test.go
│   ├── session/
│   │   ├── floodgate.go
│   │   ├── floodgate_test.go
│   │   ├── pool.go
│   │   └── auth.go
│   ├── splitter/
│   │   ├── splitter.go
│   │   └── splitter_test.go
│   ├── processor/
│   │   ├── interface.go
│   │   ├── url_user_pass.go
│   │   ├── url_user_pass_test.go
│   │   └── processor.go
│   ├── fetcher/
│   │   ├── fetcher.go
│   │   └── fetcher_test.go
│   ├── writer/
│   │   ├── writer.go
│   │   ├── writer_test.go
│   │   └── backpressure.go
│   ├── uploader/
│   │   ├── uploader.go
│   │   └── uploader_test.go
│   ├── crawler/
│   │   └── crawler.go
│   ├── telemetry/
│   │   ├── counters.go
│   │   ├── recorder.go
│   │   └── logger.go
│   └── pipeline/
│       └── pipeline.go
├── tests/
│   └── integration/
│       └── pipeline_test.go
└── docs/superpowers/
    ├── specs/2026-05-27-tgpipe-design.md  # already exists
    └── plans/2026-05-27-tgpipe-implementation.md  # this file
```

---

## Chunk 1: Foundation (bootstrap + config + state + shared helpers)

Build the layer below all stages: project skeleton, configuration, logging, shared types, retry helper, SQLite state store, FloodGate sync, SourceTracker accounting.

### Task 1.1: Project bootstrap

**Files:**
- Create: `.gitignore`
- Create: `go.mod` (via `go mod init`)
- Create: `Makefile`
- Create: `cmd/tgpipe/main.go` (skeleton)

- [ ] **Step 1: Init git repo**

Run: `git init && git branch -m main`
Expected: `Initialized empty Git repository`.

- [ ] **Step 2: Create `.gitignore`**

Content:
```gitignore
# Binaries
tgpipe
tgpipe.exe
/bin/

# Test + coverage
*.out
*.test
coverage.html

# Local state
*.db
*.db-wal
*.db-shm
/out/
/sessions/

# Config (only committed: config.example.yaml)
config.yaml
config.local.yaml

# IDE
.idea/
.vscode/
*.swp
```

- [ ] **Step 3: Init Go module**

Run: `go mod init github.com/manh/tgpipe`
(If anh có repo path khác — sửa lại module path tương ứng.)
Expected: `go.mod` created.

- [ ] **Step 4: Add core dependencies**

Run:
```bash
go get github.com/gotd/td@latest
go get modernc.org/sqlite@latest
go get github.com/spf13/cobra@latest
go get gopkg.in/yaml.v3@latest
go get golang.org/x/sync@latest
go get github.com/stretchr/testify@latest
```
Expected: `go.sum` populated, `go.mod` has require block.

- [ ] **Step 5: Create `Makefile`**

```makefile
.PHONY: build test test-race vet lint clean

build:
	go build -o bin/tgpipe ./cmd/tgpipe

test:
	go test ./...

test-race:
	go test -race -coverprofile=coverage.out ./...

vet:
	go vet ./...

lint: vet
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed, skipping"

clean:
	rm -rf bin/ coverage.out tgpipe tgpipe.exe
```

- [ ] **Step 6: Create skeleton `cmd/tgpipe/main.go`**

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "tgpipe",
	Short: "Telegram bulk downloader & republisher",
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}
```

- [ ] **Step 7: Verify build**

Run: `go build ./...`
Expected: no output (exit 0). Binary not produced because subcommands not registered yet, but compilation succeeds.

- [ ] **Step 8: Commit**

```bash
git add .gitignore go.mod go.sum Makefile cmd/tgpipe/main.go
git commit -m "chore: project bootstrap (go.mod, .gitignore, Makefile, main.go skeleton)"
```

---

### Task 1.2: `internal/config` — load + validate YAML

**Files:**
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`

- [ ] **Step 1: Write failing test `config_test.go`**

```go
package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/manh/tgpipe/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
	return p
}

func TestLoad_HappyPath(t *testing.T) {
	p := writeYAML(t, `
account:
  api_id: 12345
  api_hash: "abc"
  session_file: sessions/main.session
source_channel: -100111
target_channel: -100222
fetcher:
  sessions: 6
  chunk_size_bytes: 1048576
  chunk_channel_cap: 64
  job_channel_cap: 32
  max_retries_per_chunk: 5
  max_retries_per_job: 3
splitter:
  workers: 0
  line_channel_cap: 4096
processor:
  workers: 0
  record_channel_cap: 4096
writer:
  output_dir: ./out
  batch_size_mb: 20
  flush_interval_sec: 30
  output_channel_cap: 16
uploader:
  sessions: 2
  parallel_parts: 4
  upload_channel_cap: 4
backpressure:
  max_pending_output_files: 32
state:
  db_path: ./state.db
logging:
  level: info
  format: json
  progress_interval_sec: 30
`)
	cfg, err := config.Load(p)
	require.NoError(t, err)
	assert.Equal(t, 12345, cfg.Account.APIID)
	assert.Equal(t, int64(-100111), cfg.SourceChannel)
	assert.Equal(t, 6, cfg.Fetcher.Sessions)
	assert.Equal(t, 20, cfg.Writer.BatchSizeMB)
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := config.Load("/nonexistent/path.yaml")
	assert.Error(t, err)
}

func TestValidate_RejectsZeroSessions(t *testing.T) {
	p := writeYAML(t, `
account: {api_id: 1, api_hash: a, session_file: s.session}
source_channel: -1
target_channel: -2
fetcher: {sessions: 0, chunk_size_bytes: 1048576, chunk_channel_cap: 1, job_channel_cap: 1, max_retries_per_chunk: 1, max_retries_per_job: 1}
splitter: {line_channel_cap: 1}
processor: {record_channel_cap: 1}
writer: {output_dir: ./out, batch_size_mb: 1, flush_interval_sec: 1, output_channel_cap: 1}
uploader: {sessions: 1, parallel_parts: 1, upload_channel_cap: 1}
backpressure: {max_pending_output_files: 1}
state: {db_path: ./s.db}
logging: {level: info, format: text, progress_interval_sec: 1}
`)
	_, err := config.Load(p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetcher.sessions")
}

func TestValidate_RejectsMissingAPICredentials(t *testing.T) {
	p := writeYAML(t, `
account: {api_id: 0, api_hash: "", session_file: ""}
source_channel: 0
target_channel: 0
fetcher: {sessions: 1, chunk_size_bytes: 1, chunk_channel_cap: 1, job_channel_cap: 1, max_retries_per_chunk: 1, max_retries_per_job: 1}
splitter: {line_channel_cap: 1}
processor: {record_channel_cap: 1}
writer: {output_dir: ./out, batch_size_mb: 1, flush_interval_sec: 1, output_channel_cap: 1}
uploader: {sessions: 1, parallel_parts: 1, upload_channel_cap: 1}
backpressure: {max_pending_output_files: 1}
state: {db_path: ./s.db}
logging: {level: info, format: text, progress_interval_sec: 1}
`)
	_, err := config.Load(p)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test (expect FAIL — package doesn't exist)**

Run: `go test ./internal/config/...`
Expected: build error `no Go files` or `cannot find package`.

- [ ] **Step 3: Implement `config.go`**

```go
package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Account       AccountConfig       `yaml:"account"`
	SourceChannel int64               `yaml:"source_channel"`
	TargetChannel int64               `yaml:"target_channel"`
	Fetcher       FetcherConfig       `yaml:"fetcher"`
	Splitter      SplitterConfig      `yaml:"splitter"`
	Processor     ProcessorConfig     `yaml:"processor"`
	Writer        WriterConfig        `yaml:"writer"`
	Uploader      UploaderConfig      `yaml:"uploader"`
	Backpressure  BackpressureConfig  `yaml:"backpressure"`
	State         StateConfig         `yaml:"state"`
	Logging       LoggingConfig       `yaml:"logging"`
}

type AccountConfig struct {
	APIID       int    `yaml:"api_id"`
	APIHash     string `yaml:"api_hash"`
	SessionFile string `yaml:"session_file"`
}

type FetcherConfig struct {
	Sessions           int `yaml:"sessions"`
	ChunkSizeBytes     int `yaml:"chunk_size_bytes"`
	ChunkChannelCap    int `yaml:"chunk_channel_cap"`
	JobChannelCap      int `yaml:"job_channel_cap"`
	MaxRetriesPerChunk int `yaml:"max_retries_per_chunk"`
	MaxRetriesPerJob   int `yaml:"max_retries_per_job"`
}

type SplitterConfig struct {
	Workers        int `yaml:"workers"`
	LineChannelCap int `yaml:"line_channel_cap"`
}

type ProcessorConfig struct {
	Workers          int `yaml:"workers"`
	RecordChannelCap int `yaml:"record_channel_cap"`
}

type WriterConfig struct {
	OutputDir         string `yaml:"output_dir"`
	BatchSizeMB       int    `yaml:"batch_size_mb"`
	FlushIntervalSec  int    `yaml:"flush_interval_sec"`
	OutputChannelCap  int    `yaml:"output_channel_cap"`
}

type UploaderConfig struct {
	Sessions         int `yaml:"sessions"`
	ParallelParts    int `yaml:"parallel_parts"`
	UploadChannelCap int `yaml:"upload_channel_cap"`
}

type BackpressureConfig struct {
	MaxPendingOutputFiles int `yaml:"max_pending_output_files"`
}

type StateConfig struct {
	DBPath string `yaml:"db_path"`
}

type LoggingConfig struct {
	Level               string `yaml:"level"`
	Format              string `yaml:"format"`
	ProgressIntervalSec int    `yaml:"progress_interval_sec"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return &c, nil
}

func (c *Config) Validate() error {
	var errs []error
	if c.Account.APIID == 0 {
		errs = append(errs, errors.New("account.api_id must be set"))
	}
	if c.Account.APIHash == "" {
		errs = append(errs, errors.New("account.api_hash must be set"))
	}
	if c.Account.SessionFile == "" {
		errs = append(errs, errors.New("account.session_file must be set"))
	}
	if c.SourceChannel == 0 {
		errs = append(errs, errors.New("source_channel must be set"))
	}
	if c.TargetChannel == 0 {
		errs = append(errs, errors.New("target_channel must be set"))
	}
	if c.Fetcher.Sessions < 1 {
		errs = append(errs, errors.New("fetcher.sessions must be >= 1"))
	}
	if c.Fetcher.ChunkSizeBytes <= 0 {
		errs = append(errs, errors.New("fetcher.chunk_size_bytes must be > 0"))
	}
	if c.Uploader.Sessions < 1 {
		errs = append(errs, errors.New("uploader.sessions must be >= 1"))
	}
	if c.Writer.BatchSizeMB <= 0 {
		errs = append(errs, errors.New("writer.batch_size_mb must be > 0"))
	}
	if c.State.DBPath == "" {
		errs = append(errs, errors.New("state.db_path must be set"))
	}
	if c.Logging.Format != "json" && c.Logging.Format != "text" {
		errs = append(errs, fmt.Errorf("logging.format must be 'json' or 'text', got %q", c.Logging.Format))
	}
	return errors.Join(errs...)
}
```

- [ ] **Step 4: Run test (expect PASS)**

Run: `go test -race -v ./internal/config/...`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/
git commit -m "feat(config): YAML load + validate"
```

---

### Task 1.3: `internal/logging` — slog setup

**Files:**
- Create: `internal/logging/logging.go`

- [ ] **Step 1: Implement `logging.go` (no test — thin wrapper around stdlib)**

```go
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

type Config struct {
	Level  string
	Format string
}

func Setup(cfg Config) (*slog.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch strings.ToLower(cfg.Format) {
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	case "text", "":
		handler = slog.NewTextHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("unknown log format %q", cfg.Format)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger, nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./...`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/logging/
git commit -m "feat(logging): slog setup with level/format config"
```

---

### Task 1.4: `internal/types` — shared channel types

**Files:**
- Create: `internal/types/types.go`

- [ ] **Step 1: Implement `types.go`**

```go
package types

// Chunk is a 1MB raw-bytes block fetched from Telegram (output of Stage 1 Fetcher).
//
// Contract with Splitter (Stage 2):
//   - Seq=0 marks the first chunk of a source file → Splitter drops bytes before the first '\n'.
//   - IsLast=true marks the final chunk → Splitter drops bytes after the last '\n' and any carried remainder.
//   - Chunks for a single MsgID arrive in Seq order (Fetcher emits them sequentially).
type Chunk struct {
	MsgID  int64  // source message ID — propagated for tracking
	Seq    int    // sequence within source file (0-based); load-bearing for edge-drop logic
	Data   []byte // raw bytes, ≤ chunk_size_bytes
	IsLast bool   // last chunk of source file (load-bearing for tail-edge drop)
}

// Line is the output of Stage 2 (Splitter). Wraps raw line bytes with source MsgID
// so downstream Writer can track which sources contributed to each output file.
// Line.Data is a fresh allocation (not aliased to any chunk buffer) — safe to retain.
type Line struct {
	MsgID int64
	Data  []byte
}

// Record is the output of Stage 3 (Processor) — a parsed credential.
type Record struct {
	MsgID int64
	Email []byte
	Pass  []byte
}

// OutputFile is the output of Stage 4 (Writer) — a finalized batch file ready to upload.
type OutputFile struct {
	Path         string
	LineCount    int
	SizeBytes    int64
	BatchSeq     int
	SourceMsgIDs []int64 // dedup'd list of source MsgIDs contributing to this file
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./...`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/types/
git commit -m "feat(types): shared channel types (Chunk, Line, Record, OutputFile)"
```

---

### Task 1.5: `internal/retry` — exponential backoff helper

**Files:**
- Create: `internal/retry/retry.go`
- Create: `internal/retry/retry_test.go`

- [ ] **Step 1: Write failing test `retry_test.go`**

```go
package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/manh/tgpipe/internal/retry"
	"github.com/stretchr/testify/assert"
)

func TestWithBackoff_SucceedsFirstTry(t *testing.T) {
	calls := 0
	err := retry.WithBackoff(context.Background(), 3, func() error {
		calls++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestWithBackoff_RetriesThenSucceeds(t *testing.T) {
	calls := 0
	err := retry.WithBackoff(context.Background(), 5, func() error {
		calls++
		if calls < 3 {
			return retry.Retryable(errors.New("transient"))
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestWithBackoff_NonRetryableStopsImmediately(t *testing.T) {
	calls := 0
	terminal := errors.New("permanent failure")
	err := retry.WithBackoff(context.Background(), 5, func() error {
		calls++
		return terminal
	})
	assert.ErrorIs(t, err, terminal)
	assert.Equal(t, 1, calls)
}

func TestWithBackoff_ExhaustsAttempts(t *testing.T) {
	calls := 0
	err := retry.WithBackoff(context.Background(), 3, func() error {
		calls++
		return retry.Retryable(errors.New("flaky"))
	})
	assert.Error(t, err)
	assert.Equal(t, 3, calls)
}

func TestWithBackoff_RespectsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := retry.WithBackoff(ctx, 10, func() error {
		calls++
		return retry.Retryable(errors.New("flaky"))
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, calls, 10)
}
```

- [ ] **Step 2: Run test (expect FAIL — package missing)**

Run: `go test ./internal/retry/...`
Expected: build error.

- [ ] **Step 3: Implement `retry.go`**

```go
package retry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// retryableErr marks an error as eligible for retry. Use Retryable() to wrap.
type retryableErr struct{ err error }

func (e *retryableErr) Error() string { return e.err.Error() }
func (e *retryableErr) Unwrap() error { return e.err }

// Retryable wraps err so WithBackoff will retry. Without this wrapper,
// any error is treated as terminal.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryableErr{err: err}
}

// IsRetryable returns true if err (or any wrapped error) was marked Retryable.
func IsRetryable(err error) bool {
	var r *retryableErr
	return errors.As(err, &r)
}

// WithBackoff invokes op up to maxAttempts times, doubling the delay each time
// (capped at 8s). Returns nil on success, the last error on exhaustion, or
// ctx.Err() if canceled. Non-retryable errors stop immediately.
func WithBackoff(ctx context.Context, maxAttempts int, op func() error) error {
	if maxAttempts < 1 {
		return errors.New("retry: maxAttempts must be >= 1")
	}
	backoff := 500 * time.Millisecond
	const maxBackoff = 8 * time.Second
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := op()
		if err == nil {
			return nil
		}
		lastErr = err
		if !IsRetryable(err) {
			return err
		}
		if attempt == maxAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return fmt.Errorf("retry: exhausted %d attempts: %w", maxAttempts, lastErr)
}
```

- [ ] **Step 4: Run test (expect PASS)**

Run: `go test -race -v ./internal/retry/...`
Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/retry/
git commit -m "feat(retry): exponential backoff helper with Retryable wrapper"
```

---

### Task 1.6: SQLite migrations + `internal/state` (skeleton)

**Files:**
- Create: `internal/state/migrations/001_init.sql`
- Create: `internal/state/types.go`
- Create: `internal/state/migrations.go`
- Create: `internal/state/store.go`
- Create: `internal/state/store_test.go`

**@database-migrations applies here.**

> **Why `migrations/` lives under `internal/state/`:** Go's `//go:embed` directive does NOT permit parent-path traversal (`../../migrations/...`). The migration SQL must live in the same package tree (or below) as the file that embeds it. Top-level `migrations/` directory is removed.

- [ ] **Step 1: Create `internal/state/migrations/001_init.sql`**

```sql
CREATE TABLE IF NOT EXISTS jobs (
    msg_id            INTEGER PRIMARY KEY,
    chat_id           INTEGER NOT NULL,
    chat_access_hash  INTEGER NOT NULL,
    file_id           INTEGER NOT NULL,
    access_hash       INTEGER NOT NULL,
    file_reference    BLOB    NOT NULL,
    dc_id             INTEGER NOT NULL,
    size              INTEGER NOT NULL,
    file_name         TEXT,
    mime_type         TEXT,
    status            TEXT    NOT NULL DEFAULT 'pending',
    retries           INTEGER NOT NULL DEFAULT 0,
    output_path       TEXT,
    error_msg         TEXT,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_status  ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_updated ON jobs(updated_at);
```

> **Why `chat_access_hash`:** `tg.InputPeerChannel{ChannelID, AccessHash}` requires the channel access hash for every API call (`upload.getFile`, `messages.getMessages` for file_reference refresh). Storing it per-job makes each job self-contained and crash-safe — no need to re-resolve dialogs on resume.

- [ ] **Step 2: Create `internal/state/types.go`**

```go
package state

import "time"

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusInProgress JobStatus = "in_progress"
	StatusDone       JobStatus = "done"
	StatusFailed     JobStatus = "failed"
)

type Job struct {
	MsgID          int64
	ChatID         int64
	ChatAccessHash int64
	FileID         int64
	AccessHash     int64
	FileReference  []byte
	DCID           int
	Size           int64
	FileName       string
	MimeType       string
	Status         JobStatus
	Retries        int
	OutputPath     string
	ErrorMsg       string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Stats struct {
	Pending    int64
	InProgress int64
	Done       int64
	Failed     int64
	TotalSize  int64
	DoneSize   int64
}
```

- [ ] **Step 3: Create `internal/state/migrations.go`**

```go
package state

import (
	"context"
	"database/sql"
	_ "embed"
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
		return err
	}
	if _, err := db.ExecContext(ctx, migration001); err != nil {
		return err
	}
	return nil
}
```

- [ ] **Step 4: Write failing test `store_test.go`**

```go
package state_test

import (
	"context"
	"testing"
	"time"

	"github.com/manh/tgpipe/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustOpen(t *testing.T) *state.Store {
	t.Helper()
	s, err := state.Open(":memory:")
	require.NoError(t, err)
	require.NoError(t, s.Init(context.Background()))
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func sampleJob(msgID int64) state.Job {
	return state.Job{
		MsgID:          msgID,
		ChatID:         -100,
		ChatAccessHash: 5678,
		FileID:         1000 + msgID,
		AccessHash:     1234,
		FileReference:  []byte{1, 2, 3},
		DCID:           2,
		Size:           1024,
		FileName:       "x.txt",
		MimeType:       "text/plain",
		Status:         state.StatusPending,
		CreatedAt:      time.Unix(1000, 0),
		UpdatedAt:      time.Unix(1000, 0),
	}
}

func TestStore_InsertAndPick(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	for i := int64(1); i <= 5; i++ {
		require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(i)))
	}
	jobs, err := s.PickPending(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, jobs, 5)
	for _, j := range jobs {
		assert.Equal(t, state.StatusInProgress, j.Status)
		assert.Equal(t, int64(5678), j.ChatAccessHash)
	}
	more, err := s.PickPending(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, more, "no more pending")
}

func TestStore_MarkDone(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	_, err := s.PickPending(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, s.MarkDone(ctx, 1, "/out/x.txt"))
	stats, err := s.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Done)
}

func TestStore_ResumeResetsInProgress(t *testing.T) {
	s, err := state.Open(":memory:")
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, s.Init(ctx))
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	_, err = s.PickPending(ctx, 1)
	require.NoError(t, err)
	// Re-Init simulates restart — should flip in_progress → pending
	require.NoError(t, s.Init(ctx))
	jobs, err := s.PickPending(ctx, 10)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	_ = s.Close()
}

func TestStore_UpdateFileReference(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	newRef := []byte{9, 9, 9}
	require.NoError(t, s.UpdateFileReference(ctx, 1, newRef))
}

func TestStore_MarkFailed(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	_, err := s.PickPending(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, s.MarkFailed(ctx, 1, "auth invalid"))
	stats, err := s.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Failed)
}

func TestStore_InsertJobIfAbsent_Idempotent(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	// Inserting same msg_id should not error and should not duplicate.
	require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(1)))
	jobs, err := s.PickPending(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, jobs, 1)
}

func TestStore_PickPendingConcurrent(t *testing.T) {
	s := mustOpen(t)
	ctx := context.Background()
	for i := int64(1); i <= 30; i++ {
		require.NoError(t, s.InsertJobIfAbsent(ctx, sampleJob(i)))
	}
	type res struct{ jobs []state.Job; err error }
	ch := make(chan res, 3)
	for g := 0; g < 3; g++ {
		go func() {
			j, e := s.PickPending(ctx, 10)
			ch <- res{j, e}
		}()
	}
	seen := map[int64]bool{}
	total := 0
	for i := 0; i < 3; i++ {
		r := <-ch
		require.NoError(t, r.err)
		for _, j := range r.jobs {
			assert.False(t, seen[j.MsgID], "duplicate MsgID %d", j.MsgID)
			seen[j.MsgID] = true
			total++
		}
	}
	assert.LessOrEqual(t, total, 30)
}
```

- [ ] **Step 5: Run test (expect FAIL)**

Run: `go test ./internal/state/...`
Expected: build error (Store not implemented).

- [ ] **Step 6: Implement `store.go`**

```go
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
		return nil, err
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
	n, _ := res.RowsAffected()
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

// Sentinel error for callers that want to distinguish.
var ErrNotFound = errors.New("not found")
```

- [ ] **Step 7: Run test (expect PASS)**

Run: `go test -race -v ./internal/state/...`
Expected: all tests PASS. If `:memory:` flaky, investigate connection limit (already set to 1).

- [ ] **Step 8: Commit**

```bash
git add internal/state/
git commit -m "feat(state): SQLite store with migrations, atomic PickPending, status transitions"
```

---

### Task 1.7: `internal/tracker` — SourceTracker

**Files:**
- Create: `internal/tracker/tracker.go`
- Create: `internal/tracker/tracker_test.go`

- [ ] **Step 1: Write failing test `tracker_test.go`**

```go
package tracker_test

import (
	"context"
	"sync"
	"testing"

	"github.com/manh/tgpipe/internal/state"
	"github.com/manh/tgpipe/internal/tracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	mu      sync.Mutex
	done    map[int64]string
	failed  map[int64]string
}

func newFakeStore() *fakeStore {
	return &fakeStore{done: map[int64]string{}, failed: map[int64]string{}}
}
func (f *fakeStore) MarkDone(_ context.Context, id int64, path string) error {
	f.mu.Lock(); defer f.mu.Unlock(); f.done[id] = path; return nil
}
func (f *fakeStore) MarkFailed(_ context.Context, id int64, msg string) error {
	f.mu.Lock(); defer f.mu.Unlock(); f.failed[id] = msg; return nil
}

// unused state methods to satisfy interface — provide noop stubs in test
func (f *fakeStore) Init(context.Context) error                      { return nil }
func (f *fakeStore) InsertJob(context.Context, state.Job) error      { return nil }
func (f *fakeStore) PickPending(context.Context, int) ([]state.Job, error) { return nil, nil }
func (f *fakeStore) UpdateFileReference(context.Context, int64, []byte) error { return nil }
func (f *fakeStore) Stats(context.Context) (state.Stats, error)      { return state.Stats{}, nil }
func (f *fakeStore) Close() error                                    { return nil }

func TestTracker_MarksDoneWhenAllUploaded(t *testing.T) {
	store := newFakeStore()
	tr := tracker.New(store)
	ctx := context.Background()
	tr.Register(100, 3) // 3 chunks for msg_id 100
	// Stage 2 consumes all 3 chunks
	tr.ChunkConsumed(100)
	tr.ChunkConsumed(100)
	tr.ChunkConsumed(100)
	// Writer flushes 2 output files containing msg_id=100
	tr.OutputFlushed([]int64{100}, "/out/0001.txt")
	tr.OutputFlushed([]int64{100}, "/out/0002.txt")
	// First upload — not done yet (still 1 file pending)
	require.NoError(t, tr.OutputUploaded(ctx, []int64{100}, "/out/0001.txt"))
	store.mu.Lock(); _, doneAfter1 := store.done[100]; store.mu.Unlock()
	assert.False(t, doneAfter1)
	// Second upload — now should mark done
	require.NoError(t, tr.OutputUploaded(ctx, []int64{100}, "/out/0002.txt"))
	store.mu.Lock(); path, doneAfter2 := store.done[100]; store.mu.Unlock()
	assert.True(t, doneAfter2)
	assert.Equal(t, "/out/0002.txt", path)
}

func TestTracker_DoesNotMarkDoneIfChunksMissing(t *testing.T) {
	store := newFakeStore()
	tr := tracker.New(store)
	ctx := context.Background()
	tr.Register(200, 5)
	tr.ChunkConsumed(200) // only 1 of 5
	tr.OutputFlushed([]int64{200}, "/out/x.txt")
	require.NoError(t, tr.OutputUploaded(ctx, []int64{200}, "/out/x.txt"))
	store.mu.Lock(); _, done := store.done[200]; store.mu.Unlock()
	assert.False(t, done)
}

func TestTracker_MultipleSources_Mixed(t *testing.T) {
	store := newFakeStore()
	tr := tracker.New(store)
	ctx := context.Background()
	tr.Register(1, 1)
	tr.Register(2, 1)
	tr.ChunkConsumed(1)
	tr.ChunkConsumed(2)
	tr.OutputFlushed([]int64{1, 2}, "/out/mixed.txt")
	require.NoError(t, tr.OutputUploaded(ctx, []int64{1, 2}, "/out/mixed.txt"))
	store.mu.Lock()
	_, d1 := store.done[1]
	_, d2 := store.done[2]
	store.mu.Unlock()
	assert.True(t, d1)
	assert.True(t, d2)
}

func TestTracker_ConcurrentSafe(t *testing.T) {
	store := newFakeStore()
	tr := tracker.New(store)
	ctx := context.Background()
	const N = 100
	for i := int64(1); i <= N; i++ {
		tr.Register(i, 2)
	}
	var wg sync.WaitGroup
	for i := int64(1); i <= N; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			tr.ChunkConsumed(id)
			tr.ChunkConsumed(id)
			tr.OutputFlushed([]int64{id}, "/out/x.txt")
			_ = tr.OutputUploaded(ctx, []int64{id}, "/out/x.txt")
		}(i)
	}
	wg.Wait()
	store.mu.Lock(); defer store.mu.Unlock()
	assert.Len(t, store.done, N)
}
```

- [ ] **Step 2: Run test (expect FAIL)**

Run: `go test ./internal/tracker/...`
Expected: build error.

- [ ] **Step 3: Implement `tracker.go`**

```go
package tracker

import (
	"context"
	"sync"

	"github.com/manh/tgpipe/internal/state"
)

// Store is the subset of state.Store that the tracker needs.
type Store interface {
	MarkDone(ctx context.Context, msgID int64, outputPath string) error
	MarkFailed(ctx context.Context, msgID int64, errMsg string) error
}

// Verify state.Store satisfies the interface at compile time.
var _ Store = (*state.Store)(nil)

type srcState struct {
	totalChunks        int
	chunksConsumed     int
	outputFilesPending int
	lastOutputPath     string
}

// SourceTracker accounts for per-source completion across the pipeline.
// All methods are safe for concurrent use.
type SourceTracker struct {
	mu      sync.Mutex
	sources map[int64]*srcState
	store   Store
}

func New(store Store) *SourceTracker {
	return &SourceTracker{
		sources: make(map[int64]*srcState),
		store:   store,
	}
}

func (t *SourceTracker) Register(msgID int64, totalChunks int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.sources[msgID]; exists {
		return // idempotent
	}
	t.sources[msgID] = &srcState{totalChunks: totalChunks}
}

func (t *SourceTracker) ChunkConsumed(msgID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.sources[msgID]; ok {
		s.chunksConsumed++
	}
}

func (t *SourceTracker) OutputFlushed(srcIDs []int64, path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range srcIDs {
		if s, ok := t.sources[id]; ok {
			s.outputFilesPending++
			s.lastOutputPath = path
		}
	}
}

// OutputUploaded notifies the tracker that path has been uploaded successfully.
// For each contributing source, decrement the pending count and — if all chunks
// have been consumed and no pending uploads remain — persist done state.
func (t *SourceTracker) OutputUploaded(ctx context.Context, srcIDs []int64, path string) error {
	t.mu.Lock()
	var toMarkDone []struct {
		msgID int64
		path  string
	}
	for _, id := range srcIDs {
		s, ok := t.sources[id]
		if !ok {
			continue
		}
		s.outputFilesPending--
		if s.chunksConsumed >= s.totalChunks && s.outputFilesPending <= 0 {
			toMarkDone = append(toMarkDone, struct {
				msgID int64
				path  string
			}{id, s.lastOutputPath})
			delete(t.sources, id)
		}
	}
	t.mu.Unlock()
	for _, m := range toMarkDone {
		if err := t.store.MarkDone(ctx, m.msgID, m.path); err != nil {
			return err
		}
	}
	return nil
}

// Fail removes the source from tracking and marks it failed in the store.
func (t *SourceTracker) Fail(ctx context.Context, msgID int64, errMsg string) error {
	t.mu.Lock()
	delete(t.sources, msgID)
	t.mu.Unlock()
	return t.store.MarkFailed(ctx, msgID, errMsg)
}
```

- [ ] **Step 4: Run test (expect PASS)**

Run: `go test -race -v ./internal/tracker/...`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/tracker/
git commit -m "feat(tracker): SourceTracker for per-source completion accounting"
```

---

### Task 1.8: `internal/session/floodgate` — account-wide FLOOD_WAIT pause

**Files:**
- Create: `internal/session/floodgate.go`
- Create: `internal/session/floodgate_test.go`

**@golang-concurrency applies here (CAS, atomic).**

- [ ] **Step 1: Write failing test `floodgate_test.go`**

```go
package session_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/manh/tgpipe/internal/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloodGate_NoTriggerPassesImmediately(t *testing.T) {
	g := &session.FloodGate{}
	start := time.Now()
	require.NoError(t, g.Wait(context.Background()))
	assert.Less(t, time.Since(start), 5*time.Millisecond)
}

func TestFloodGate_TriggerBlocks(t *testing.T) {
	g := &session.FloodGate{}
	g.Trigger(80 * time.Millisecond)
	start := time.Now()
	require.NoError(t, g.Wait(context.Background()))
	assert.GreaterOrEqual(t, time.Since(start), 70*time.Millisecond)
}

func TestFloodGate_TriggerExtendsButDoesntShrink(t *testing.T) {
	g := &session.FloodGate{}
	g.Trigger(200 * time.Millisecond)
	g.Trigger(50 * time.Millisecond) // shorter — should NOT shrink
	start := time.Now()
	require.NoError(t, g.Wait(context.Background()))
	assert.GreaterOrEqual(t, time.Since(start), 180*time.Millisecond)
}

func TestFloodGate_CancelInterrupts(t *testing.T) {
	g := &session.FloodGate{}
	g.Trigger(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	err := g.Wait(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestFloodGate_ConcurrentTriggers(t *testing.T) {
	g := &session.FloodGate{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); g.Trigger(20 * time.Millisecond) }()
	}
	wg.Wait()
	require.NoError(t, g.Wait(context.Background()))
}

// Regression: Trigger() extension while Wait() is asleep must be honoured.
func TestFloodGate_ExtensionDuringWait(t *testing.T) {
	g := &session.FloodGate{}
	g.Trigger(50 * time.Millisecond)
	go func() {
		time.Sleep(30 * time.Millisecond)
		g.Trigger(150 * time.Millisecond) // extend mid-wait
	}()
	start := time.Now()
	require.NoError(t, g.Wait(context.Background()))
	// Original would have fired at ~50ms; extension pushes total to ~180ms.
	assert.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond)
}
```

- [ ] **Step 2: Run test (expect FAIL)**

Run: `go test ./internal/session/...`
Expected: build error.

- [ ] **Step 3: Implement `floodgate.go`**

```go
package session

import (
	"context"
	"sync/atomic"
	"time"
)

// FloodGate coordinates account-wide FLOOD_WAIT pauses across both
// fetch and upload sub-pools. When the Telegram API returns FLOOD_WAIT_X
// on any session, every other session on the same account should also
// back off to avoid amplifying the throttle.
type FloodGate struct {
	until atomic.Int64 // unix nanos; 0 = no active flood
}

// Trigger records a flood wait of duration d from now. Repeated calls
// extend the wait monotonically — a later, shorter trigger never shrinks
// an existing longer wait.
func (g *FloodGate) Trigger(d time.Duration) {
	if d <= 0 {
		return
	}
	newUntil := time.Now().Add(d).UnixNano()
	for {
		cur := g.until.Load()
		if newUntil <= cur {
			return
		}
		if g.until.CompareAndSwap(cur, newUntil) {
			return
		}
	}
}

// Wait blocks until any active flood-wait period has elapsed, or ctx is canceled.
// Returns nil if the gate was already open or the wait completed normally.
//
// If Trigger() extends the deadline while Wait() is already sleeping, the
// extension MUST be honoured — otherwise a quick second flood could leak
// through. The loop re-reads `until` after each timer firing.
func (g *FloodGate) Wait(ctx context.Context) error {
	for {
		until := g.until.Load()
		if until == 0 {
			return nil
		}
		diff := time.Until(time.Unix(0, until))
		if diff <= 0 {
			return nil
		}
		timer := time.NewTimer(diff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			// Re-check: an extending Trigger() may have raised `until` while we slept.
		}
	}
}
```

- [ ] **Step 4: Run test (expect PASS)**

Run: `go test -race -v ./internal/session/...`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/session/
git commit -m "feat(session): FloodGate for account-wide FLOOD_WAIT coordination"
```

---

## Chunk 2: MTProto session pool + processing stages (Stages 1, 2, 3)

Wire `gotd/td` into a `Pool` interface that round-robins RPCs across N clients sharing 1 `.session` file. Build Stage 2 (pure-func Splitter), Stage 3 (UrlUserPassExtractor + Processor), then Stage 1 (Fetcher) which uses Pool + tracker + store.

### Task 2.1: `internal/session/pool` — MTProto sub-pool

**Files:**
- Create: `internal/session/pool.go`
- Create: `internal/session/auth.go`

This task wraps `gotd/td`'s `telegram.Client` into a `Pool` that implements `tg.Invoker`. Because gotd's client lifecycle requires `client.Run(ctx)` to be blocking, the Pool spawns N goroutines, each running its own client. Round-robin uses an atomic counter (cheap, lock-free, monotonic) — NOT a channel of clients (channels add latency and the dequeue/requeue dance is racy).

**Why `tg.Invoker`:** gotd's typed wrappers (`tg.NewClient(invoker).UploadGetFile(...)`, `UploadSaveFilePart(...)`, etc.) take any `tg.Invoker`. By making Pool implement that interface, callers get type-safe RPC instead of hand-rolling `bin.Encoder`/`bin.Decoder`.

**@golang-concurrency, @security-review (auth) apply here.**

- [ ] **Step 1: Implement `auth.go` (session storage on disk)**

```go
package session

import (
	"github.com/gotd/td/session"
)

// newSessionStorage returns gotd's file-backed session storage at `path`.
// Multiple clients pointing at the same path share the same auth_key.
// (Concurrent writes to the .session file are serialised internally by gotd.)
func newSessionStorage(path string) session.Storage {
	return &session.FileStorage{Path: path}
}
```

- [ ] **Step 2: Implement `pool.go`**

```go
package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// Pool is a round-robin RPC dispatcher across N MTProto clients sharing one auth_key.
// Implementations also enforce a shared FloodGate so any single client hitting
// FLOOD_WAIT will pause the whole pool.
//
// Pool implements tg.Invoker, so callers can wrap it via tg.NewClient(pool)
// to use the typed API surface (UploadGetFile, UploadSaveFilePart, …).
type Pool interface {
	tg.Invoker
	Size() int
	Close() error
}

// Config for constructing a Pool.
type Config struct {
	APIID       int
	APIHash     string
	SessionFile string
	Size        int
}

type clientPool struct {
	gate    *FloodGate
	clients []*telegram.Client
	next    atomic.Uint64 // monotonic round-robin counter
	closeFn func()
	wg      sync.WaitGroup
}

// NewFetchPool and NewUploadPool currently use identical construction. They
// remain separate symbols so the caller's intent (and future divergence —
// e.g. different DC affinity) stays explicit.
func NewFetchPool(ctx context.Context, cfg Config, gate *FloodGate) (Pool, error) {
	return newPool(ctx, cfg, gate)
}

func NewUploadPool(ctx context.Context, cfg Config, gate *FloodGate) (Pool, error) {
	return newPool(ctx, cfg, gate)
}

func newPool(ctx context.Context, cfg Config, gate *FloodGate) (*clientPool, error) {
	if cfg.Size < 1 {
		return nil, errors.New("pool size must be >= 1")
	}
	if cfg.APIID == 0 || cfg.APIHash == "" {
		return nil, errors.New("pool requires APIID and APIHash")
	}
	p := &clientPool{gate: gate}
	poolCtx, cancel := context.WithCancel(ctx)
	p.closeFn = cancel

	for i := 0; i < cfg.Size; i++ {
		client := telegram.NewClient(cfg.APIID, cfg.APIHash, telegram.Options{
			SessionStorage: newSessionStorage(cfg.SessionFile),
		})
		p.clients = append(p.clients, client)
		ready := make(chan struct{})
		p.wg.Add(1)
		go func(c *telegram.Client) {
			defer p.wg.Done()
			_ = c.Run(poolCtx, func(ctx context.Context) error {
				close(ready)
				<-ctx.Done()
				return nil
			})
		}(client)
		select {
		case <-ready:
			// client is initialized
		case <-poolCtx.Done():
			_ = p.Close()
			return nil, poolCtx.Err()
		}
	}
	return p, nil
}

// Invoke implements tg.Invoker. Round-robin via an atomic counter — no channel,
// no lock, no race between "pick" and "use".
func (p *clientPool) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	if err := p.gate.Wait(ctx); err != nil {
		return err
	}
	idx := p.next.Add(1) - 1
	client := p.clients[int(idx%uint64(len(p.clients)))]
	err := client.Invoke(ctx, input, output)
	if ok, wait := IsFloodWait(err); ok {
		// One client hit FLOOD_WAIT → pause the whole pool (the throttle is
		// account-wide, not per-session). Caller observes the error and
		// decides whether to retry; the gate ensures subsequent Invoke calls
		// block until the wait elapses.
		p.gate.Trigger(secondsToDuration(wait))
	}
	return err
}

func (p *clientPool) Size() int { return len(p.clients) }

func (p *clientPool) Close() error {
	if p.closeFn != nil {
		p.closeFn()
	}
	p.wg.Wait()
	return nil
}

// IsFloodWait inspects err for a Telegram FLOOD_WAIT_X code and returns (true, X seconds).
// Uses gotd's tgerr.AsType which recognises the canonical FLOOD_WAIT shape.
func IsFloodWait(err error) (bool, int) {
	if err == nil {
		return false, 0
	}
	if rpc, ok := tgerr.As(err); ok && rpc.IsCode(420) {
		// Type is FLOOD_WAIT_<n>; tgerr exposes Argument when matched.
		var n int
		if _, perr := fmt.Sscanf(rpc.Type, "FLOOD_WAIT_%d", &n); perr == nil {
			return true, n
		}
		return true, 0
	}
	return false, 0
}
```

> **Note on `secondsToDuration`:** declared in the same package — used only here and in callers that translate FLOOD_WAIT seconds to `time.Duration`. Add to `floodgate.go` or a small shared helper:
>
> ```go
> func secondsToDuration(s int) time.Duration { return time.Duration(s) * time.Second }
> ```

- [ ] **Step 3: Verify build**

Run: `go build ./...`
Expected: compiles without errors. (Pool not unit-tested — needs real Telegram. Integration test in Chunk 5 will cover via a fake `Pool` interface.)

- [ ] **Step 4: Commit**

```bash
git add internal/session/pool.go internal/session/auth.go
git commit -m "feat(session): MTProto Pool implements tg.Invoker with atomic round-robin"
```

---

### Task 2.2: `internal/splitter` — Stage 2 (per-source drop-edges)

**Files:**
- Create: `internal/splitter/splitter.go`
- Create: `internal/splitter/splitter_test.go`

> **Why single-goroutine (not a worker pool):** Edges must be dropped ONCE per source file, not once per chunk. To stitch lines that span a 1 MB chunk boundary the splitter must carry a per-MsgID remainder buffer from chunk N to chunk N+1. If multiple workers consumed `chunkCh` concurrently, chunks of the same MsgID could land on different workers and the remainder would race. Splitting 25 MB/s of text is well under 1 CPU core, so single-goroutine is the simplest correct design. The `workers` config knob is retained but ignored (logged as a warning if >1).

> **Contract from Fetcher:** chunks for a single MsgID arrive in `Seq` order (Fetcher's `fetchJob` loops Seq=0,1,2,…). Chunks of different MsgIDs may interleave on `chunkCh`. The splitter relies on this in-order delivery per MsgID.

> **Edge-drop semantics:**
> - `Seq == 0`: drop everything before the FIRST `\n` (head edge of the source file).
> - `IsLast == true`: drop everything after the LAST `\n` and discard the carried remainder.
> - Middle chunks: stitch carried remainder + this chunk, emit complete lines, carry the unfinished tail.

- [ ] **Step 1: Write failing test `splitter_test.go`**

```go
package splitter_test

import (
	"context"
	"sync"
	"testing"

	"github.com/manh/tgpipe/internal/splitter"
	"github.com/manh/tgpipe/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeTracker struct {
	mu             sync.Mutex
	chunksConsumed map[int64]int
}

func (f *fakeTracker) ChunkConsumed(id int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.chunksConsumed == nil {
		f.chunksConsumed = map[int64]int{}
	}
	f.chunksConsumed[id]++
}

func TestSplitter_SingleChunkFile(t *testing.T) {
	tr := &fakeTracker{}
	s := splitter.New(1, tr)
	in := make(chan types.Chunk, 1)
	out := make(chan types.Line, 16)
	// Whole file in one chunk: Seq=0 + IsLast=true → drop head + tail.
	in <- types.Chunk{MsgID: 7, Seq: 0, IsLast: true,
		Data: []byte("HEAD\nfoo\nbar\nbaz\nTAIL")}
	close(in)
	require.NoError(t, s.Run(context.Background(), in, out))
	close(out)
	var got []string
	for ln := range out {
		got = append(got, string(ln.Data))
	}
	assert.Equal(t, []string{"foo", "bar", "baz"}, got)
}

func TestSplitter_MultiChunkFile_StitchesAcrossBoundary(t *testing.T) {
	tr := &fakeTracker{}
	s := splitter.New(1, tr)
	in := make(chan types.Chunk, 3)
	out := make(chan types.Line, 16)
	// File: "HEAD\nfoo\nbar\nbaz\nqux\nTAIL"
	// Chunked at byte 9 and byte 18 — "bar" spans the first boundary.
	in <- types.Chunk{MsgID: 9, Seq: 0, IsLast: false, Data: []byte("HEAD\nfoo\nba")}
	in <- types.Chunk{MsgID: 9, Seq: 1, IsLast: false, Data: []byte("r\nbaz\nqu")}
	in <- types.Chunk{MsgID: 9, Seq: 2, IsLast: true, Data: []byte("x\nTAIL")}
	close(in)
	require.NoError(t, s.Run(context.Background(), in, out))
	close(out)
	var got []string
	for ln := range out {
		got = append(got, string(ln.Data))
	}
	// HEAD dropped (Seq=0 head), TAIL dropped (IsLast tail), "bar" stitched.
	assert.Equal(t, []string{"foo", "bar", "baz", "qux"}, got)
	assert.Equal(t, 3, tr.chunksConsumed[9])
}

func TestSplitter_InterleavedSources(t *testing.T) {
	tr := &fakeTracker{}
	s := splitter.New(1, tr)
	in := make(chan types.Chunk, 4)
	out := make(chan types.Line, 16)
	// MsgID 7 and MsgID 8 interleave on the channel.
	in <- types.Chunk{MsgID: 7, Seq: 0, IsLast: false, Data: []byte("H7\nA\nB")}
	in <- types.Chunk{MsgID: 8, Seq: 0, IsLast: false, Data: []byte("H8\nX\nY")}
	in <- types.Chunk{MsgID: 7, Seq: 1, IsLast: true, Data: []byte("C\nT7")}
	in <- types.Chunk{MsgID: 8, Seq: 1, IsLast: true, Data: []byte("Z\nT8")}
	close(in)
	require.NoError(t, s.Run(context.Background(), in, out))
	close(out)
	got := map[int64][]string{}
	for ln := range out {
		got[ln.MsgID] = append(got[ln.MsgID], string(ln.Data))
	}
	assert.Equal(t, []string{"A", "B", "C"}, got[7]) // H7 head dropped, T7 tail dropped, "B"+"C" stitched
	assert.Equal(t, []string{"X", "Y", "Z"}, got[8])
}

func TestSplitter_ContextCancel(t *testing.T) {
	s := splitter.New(1, &fakeTracker{})
	in := make(chan types.Chunk)
	out := make(chan types.Line, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.Run(ctx, in, out)
	assert.NoError(t, err)
	close(in)
	close(out)
}
```

- [ ] **Step 2: Run test (expect FAIL)**

Run: `go test ./internal/splitter/...`
Expected: build error.

- [ ] **Step 3: Implement `splitter.go`**

```go
package splitter

import (
	"bytes"
	"context"
	"log/slog"

	"github.com/manh/tgpipe/internal/types"
)

// Tracker is the subset of SourceTracker the splitter needs.
type Tracker interface {
	ChunkConsumed(msgID int64)
}

// Splitter is the Stage-2 line splitter. It runs as a SINGLE goroutine
// (workers >1 is ignored — see Task header for rationale) and maintains
// a per-MsgID remainder buffer so lines that span chunk boundaries get
// stitched. Edge bytes are dropped exactly once per source file:
//   - head of Seq==0 chunk (bytes before the first '\n')
//   - tail of IsLast chunk  (bytes after the last '\n' + remainder discarded)
type Splitter struct {
	workers int
	tracker Tracker
}

func New(workers int, tr Tracker) *Splitter {
	if workers > 1 {
		slog.Warn("splitter: workers>1 ignored — splitter is single-goroutine by design",
			"requested", workers)
	}
	return &Splitter{workers: 1, tracker: tr}
}

// Run consumes chunks until `in` is closed or ctx is cancelled. It never
// returns an error — failures upstream propagate via ctx cancellation.
func (s *Splitter) Run(ctx context.Context, in <-chan types.Chunk, out chan<- types.Line) error {
	// Per-MsgID remainder: bytes after the last '\n' of the previous chunk,
	// waiting to be stitched with the head of the next chunk for the same MsgID.
	remainders := make(map[int64][]byte)

	emit := func(msgID int64, line []byte) bool {
		// Copy because remainder slices may alias chunk.Data which is
		// reused/discarded after the chunk leaves the splitter.
		buf := make([]byte, len(line))
		copy(buf, line)
		select {
		case <-ctx.Done():
			return false
		case out <- types.Line{MsgID: msgID, Data: buf}:
			return true
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case chunk, ok := <-in:
			if !ok {
				return nil
			}
			data := chunk.Data

			// Step 1: drop head edge if this is the first chunk of the source.
			if chunk.Seq == 0 {
				if i := bytes.IndexByte(data, '\n'); i >= 0 {
					data = data[i+1:]
				} else {
					// No newline at all in the first chunk → nothing usable yet;
					// remainder buffer stays empty.
					data = nil
				}
			} else if r, has := remainders[chunk.MsgID]; has && len(r) > 0 {
				// Stitch: previous remainder + this chunk.
				stitched := make([]byte, 0, len(r)+len(data))
				stitched = append(stitched, r...)
				stitched = append(stitched, data...)
				data = stitched
				delete(remainders, chunk.MsgID)
			}

			// Step 2: split complete lines (everything up to the last '\n').
			lastNL := bytes.LastIndexByte(data, '\n')
			if lastNL >= 0 {
				body := data[:lastNL]
				// Save the tail (after the last '\n') as remainder UNLESS this
				// is the last chunk — in which case the tail is the dropped
				// trailing edge.
				if !chunk.IsLast && lastNL+1 < len(data) {
					tail := make([]byte, len(data)-(lastNL+1))
					copy(tail, data[lastNL+1:])
					remainders[chunk.MsgID] = tail
				}
				for len(body) > 0 {
					idx := bytes.IndexByte(body, '\n')
					if idx < 0 {
						if !emit(chunk.MsgID, body) {
							return nil
						}
						break
					}
					if !emit(chunk.MsgID, body[:idx]) {
						return nil
					}
					body = body[idx+1:]
				}
			} else if !chunk.IsLast {
				// No newline in this chunk and more chunks coming — carry it all.
				buf := make([]byte, len(data))
				copy(buf, data)
				remainders[chunk.MsgID] = buf
			}

			// Step 3: if last chunk, discard any leftover remainder (trailing edge).
			if chunk.IsLast {
				delete(remainders, chunk.MsgID)
			}

			s.tracker.ChunkConsumed(chunk.MsgID)
		}
	}
}
```

- [ ] **Step 4: Run test (expect PASS)**

Run: `go test -race -v ./internal/splitter/...`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/splitter/
git commit -m "feat(splitter): Stage 2 — per-MsgID drop-edges with cross-chunk stitching"
```

---

### Task 2.3: `internal/processor` — Stage 3 (url:user:pass → email:pass)

**Files:**
- Create: `internal/processor/interface.go`
- Create: `internal/processor/url_user_pass.go`
- Create: `internal/processor/url_user_pass_test.go`
- Create: `internal/processor/processor.go`

- [ ] **Step 1: Implement `interface.go`**

```go
package processor

import "github.com/manh/tgpipe/internal/types"

// LineProcessor transforms a raw line into a Record. The bool return
// indicates whether the record should be kept (false → drop, e.g.
// malformed input). A non-nil error is fatal for the pipeline.
type LineProcessor interface {
	Process(line []byte) (types.Record, bool, error)
}
```

- [ ] **Step 2: Write failing test `url_user_pass_test.go`**

```go
package processor_test

import (
	"testing"

	"github.com/manh/tgpipe/internal/processor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUrlUserPassExtractor(t *testing.T) {
	cases := []struct {
		in        string
		wantKeep  bool
		wantEmail string
		wantPass  string
	}{
		// Happy path
		{"https://site.com:user@x.com:pass123", true, "user@x.com", "pass123"},
		{"http://a.b.c:foo@bar.io:p:a:ss", true, "foo@bar.io", "p:a:ss"},
		{"site.com:8080:e@x.com:pass", true, "e@x.com", "pass"},
		{"user@x.com:pass", true, "user@x.com", "pass"}, // 1 colon = 2 parts
		// Reject — user is not email
		{"https://site.com:johnsmith:1234", false, "", ""},
		{"https://site.com:user@:1234", false, "", ""},
		{"https://site.com:@x.com:1234", false, "", ""},
		// Malformed
		{"", false, "", ""},
		{"abc", false, "", ""},
		{"only:one_colon", false, "", ""},
		// Edge — empty pass → drop per design decision
		{"https://site:user@x.com:", false, "", ""},
		// Edge — leading/trailing whitespace email → reject
		{"https://site.com: user@x.com :pass", false, "", ""},
	}
	p := &processor.UrlUserPassExtractor{}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			rec, keep, err := p.Process([]byte(tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.wantKeep, keep)
			if tc.wantKeep {
				assert.Equal(t, tc.wantEmail, string(rec.Email))
				assert.Equal(t, tc.wantPass, string(rec.Pass))
			}
		})
	}
}
```

- [ ] **Step 3: Run test (expect FAIL)**

Run: `go test ./internal/processor/...`
Expected: build error.

- [ ] **Step 4: Implement `url_user_pass.go`**

```go
package processor

import (
	"bytes"

	"github.com/manh/tgpipe/internal/types"
)

// UrlUserPassExtractor parses lines of the form url:user:pass and emits
// email:pass records. The split strategy is "from-the-right": we find the
// last two ':' characters in the line and treat the segments after them
// as user and pass. This naturally handles URLs containing ':' (schema,
// port) and passwords containing ':'.
//
// Strict mode: a line is kept only when the user segment is a valid email.
// Empty user, empty pass, or non-email user → drop.
type UrlUserPassExtractor struct{}

func (e *UrlUserPassExtractor) Process(line []byte) (types.Record, bool, error) {
	if len(line) == 0 {
		return types.Record{}, false, nil
	}
	lastColon := bytes.LastIndexByte(line, ':')
	if lastColon <= 0 || lastColon == len(line)-1 {
		// no separator, or pass is empty
		return types.Record{}, false, nil
	}
	pass := line[lastColon+1:]
	left := line[:lastColon]
	prevColon := bytes.LastIndexByte(left, ':')
	if prevColon < 0 {
		// only one ':' overall → treat the part before as user (no URL)
		prevColon = -1
	}
	user := left[prevColon+1:]
	if len(user) == 0 || !isValidEmail(user) {
		return types.Record{}, false, nil
	}
	return types.Record{Email: user, Pass: pass}, true, nil
}

// isValidEmail performs a minimal RFC-5321ish check:
//   - exactly one '@'
//   - non-empty local part with no whitespace
//   - domain has at least one '.' and no whitespace
// We deliberately avoid regex for hot-path performance.
func isValidEmail(b []byte) bool {
	at := bytes.IndexByte(b, '@')
	if at <= 0 || at == len(b)-1 {
		return false
	}
	if bytes.Count(b, []byte("@")) != 1 {
		return false
	}
	local := b[:at]
	domain := b[at+1:]
	if hasWhitespace(local) || hasWhitespace(domain) {
		return false
	}
	if bytes.IndexByte(domain, '.') < 0 {
		return false
	}
	return true
}

func hasWhitespace(b []byte) bool {
	for _, c := range b {
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			return true
		}
	}
	return false
}
```

- [ ] **Step 5: Run test (expect PASS)**

Run: `go test -race -v ./internal/processor/...`
Expected: all tests PASS.

- [ ] **Step 6: Implement `processor.go` (the stage runner)**

```go
package processor

import (
	"context"
	"runtime"
	"sync"

	"github.com/manh/tgpipe/internal/types"
)

// Recorder is the subset of telemetry.Counters the processor needs.
type Recorder interface {
	IncDroppedInvalidLine()
	IncLinesEmitted()
}

type Processor struct {
	workers  int
	impl     LineProcessor
	recorder Recorder
}

func New(workers int, impl LineProcessor, rec Recorder) *Processor {
	if workers <= 0 {
		workers = runtime.NumCPU() * 2
	}
	return &Processor{workers: workers, impl: impl, recorder: rec}
}

func (p *Processor) Run(ctx context.Context, in <-chan types.Line, out chan<- types.Record) error {
	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case ln, ok := <-in:
					if !ok {
						return
					}
					rec, keep, err := p.impl.Process(ln.Data)
					if err != nil {
						return // fatal — orchestrator handles via ctx
					}
					if !keep {
						p.recorder.IncDroppedInvalidLine()
						continue
					}
					rec.MsgID = ln.MsgID
					p.recorder.IncLinesEmitted()
					select {
					case <-ctx.Done():
						return
					case out <- rec:
					}
				}
			}
		}()
	}
	wg.Wait()
	return nil
}
```

- [ ] **Step 7: Verify build**

Run: `go build ./...`
Expected: passes.

- [ ] **Step 8: Commit**

```bash
git add internal/processor/
git commit -m "feat(processor): Stage 3 LineProcessor interface + UrlUserPassExtractor"
```

---

### Task 2.4: `internal/fetcher` — Stage 1 (parallel chunk fetch)

**Files:**
- Create: `internal/fetcher/fetcher.go`
- Create: `internal/fetcher/fetcher_test.go`

The fetcher dispatches `upload.getFile` RPCs in parallel across the fetch sub-pool, with `FILE_REFERENCE_EXPIRED` inline refresh via `channels.getMessages`. Uses gotd's typed client wrapper (`tg.NewClient(pool)`) instead of raw `Invoke(...)` with hand-rolled decoders — typed RPC methods return `(*tg.UploadFile, error)` directly and won't compile against a misshapen response.

**@golang-concurrency, @security-review apply here.**

- [ ] **Step 1: Implement `fetcher.go`**

```go
package fetcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/manh/tgpipe/internal/retry"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
	"github.com/manh/tgpipe/internal/types"
)

// Store is the subset of state.Store the fetcher needs.
type Store interface {
	UpdateFileReference(ctx context.Context, msgID int64, ref []byte) error
}

type Tracker interface {
	Register(msgID int64, totalChunks int)
}

type Recorder interface {
	AddDownloadBytes(int64)
	IncFloodWait()
	IncRetry()
	IncFileRefExpired()
}

type Config struct {
	Sessions           int
	ChunkSizeBytes     int
	MaxRetriesPerChunk int
}

type Fetcher struct {
	pool     session.Pool
	api      *tg.Client // typed wrapper around pool
	store    Store
	tracker  Tracker
	gate     *session.FloodGate
	recorder Recorder
	cfg      Config
}

func New(pool session.Pool, store Store, tracker Tracker, gate *session.FloodGate, rec Recorder, cfg Config) *Fetcher {
	return &Fetcher{
		pool:     pool,
		api:      tg.NewClient(pool),
		store:    store,
		tracker:  tracker,
		gate:     gate,
		recorder: rec,
		cfg:      cfg,
	}
}

// Run consumes Jobs from `jobs` and emits Chunks to `out`. Returns when `jobs`
// is closed and all in-flight fetches are done, or when ctx is canceled.
func (f *Fetcher) Run(ctx context.Context, jobs <-chan state.Job, out chan<- types.Chunk) error {
	var wg sync.WaitGroup
	for i := 0; i < f.cfg.Sessions; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					if err := f.fetchJob(ctx, job, out); err != nil {
						if errors.Is(err, context.Canceled) {
							return
						}
						slog.Error("fetch job", "stage", "fetcher", "msg_id", job.MsgID, "err", err)
					}
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func (f *Fetcher) fetchJob(ctx context.Context, job state.Job, out chan<- types.Chunk) error {
	totalChunks := int((job.Size + int64(f.cfg.ChunkSizeBytes) - 1) / int64(f.cfg.ChunkSizeBytes))
	f.tracker.Register(job.MsgID, totalChunks)

	loc := &tg.InputDocumentFileLocation{
		ID:            job.FileID,
		AccessHash:    job.AccessHash,
		FileReference: job.FileReference,
	}

	for seq := 0; seq < totalChunks; seq++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		offset := int64(seq) * int64(f.cfg.ChunkSizeBytes)
		var data []byte
		err := retry.WithBackoff(ctx, f.cfg.MaxRetriesPerChunk, func() error {
			res, invokeErr := f.api.UploadGetFile(ctx, &tg.UploadGetFileRequest{
				Location: loc,
				Offset:   offset,
				Limit:    f.cfg.ChunkSizeBytes,
			})
			if invokeErr != nil {
				if tgerr.Is(invokeErr, "FILE_REFERENCE_EXPIRED") {
					f.recorder.IncFileRefExpired()
					if refreshErr := f.refreshFileReference(ctx, &job, loc); refreshErr != nil {
						return refreshErr
					}
					return retry.Retryable(invokeErr)
				}
				if fw, sec := session.IsFloodWait(invokeErr); fw {
					f.recorder.IncFloodWait()
					f.gate.Trigger(time.Duration(sec+1) * time.Second)
					return retry.Retryable(invokeErr)
				}
				f.recorder.IncRetry()
				return retry.Retryable(invokeErr)
			}
			file, ok := res.(*tg.UploadFile)
			if !ok {
				// upload.fileCdnRedirect — not supported in this pipeline (would
				// require a separate CDN client). Fail the job so it's retried
				// later or routed to a human.
				return fmt.Errorf("unexpected upload response type %T", res)
			}
			data = file.Bytes
			return nil
		})
		if err != nil {
			return fmt.Errorf("fetch chunk seq=%d msg=%d: %w", seq, job.MsgID, err)
		}
		f.recorder.AddDownloadBytes(int64(len(data)))
		chunk := types.Chunk{
			MsgID:  job.MsgID,
			Seq:    seq,
			Data:   data,
			IsLast: seq == totalChunks-1,
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- chunk:
		}
	}
	return nil
}

// refreshFileReference re-fetches the source message to obtain a fresh
// FILE_REFERENCE. Uses channels.getMessages with a properly-resolved channel
// AccessHash (stored on the Job from the crawler's dialog resolution).
func (f *Fetcher) refreshFileReference(ctx context.Context, job *state.Job, loc *tg.InputDocumentFileLocation) error {
	res, err := f.api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: &tg.InputChannel{
			ChannelID:  job.ChatID,
			AccessHash: job.ChatAccessHash,
		},
		ID: []tg.InputMessageClass{&tg.InputMessageID{ID: int(job.MsgID)}},
	})
	if err != nil {
		return fmt.Errorf("refresh ref: %w", err)
	}
	msgs, ok := res.(interface{ GetMessages() []tg.MessageClass })
	if !ok {
		return errors.New("refresh ref: unexpected messages response shape")
	}
	for _, m := range msgs.GetMessages() {
		msg, ok := m.(*tg.Message)
		if !ok {
			continue
		}
		media, ok := msg.Media.(*tg.MessageMediaDocument)
		if !ok {
			continue
		}
		doc, ok := media.Document.(*tg.Document)
		if !ok {
			continue
		}
		loc.FileReference = doc.FileReference
		job.FileReference = doc.FileReference
		return f.store.UpdateFileReference(ctx, job.MsgID, doc.FileReference)
	}
	return errors.New("refresh ref: document not found in response")
}
```

- [ ] **Step 2: Write `fetcher_test.go` (mock-based test)**

> **Why fakePool round-trips via bin buffers:** when callers use `tg.NewClient(pool).UploadGetFile(...)`, gotd passes a fresh `*tg.UploadFile` (or `*tg.UploadFileCdnRedirect`) as the `bin.Decoder` and decodes the wire response into it. The fake must therefore (1) encode a synthetic `*tg.UploadFile` into a `bin.Buffer`, (2) decode it back into the caller-supplied `output`. This is the canonical pattern gotd's own tests use.

```go
package fetcher_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/manh/tgpipe/internal/fetcher"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
	"github.com/manh/tgpipe/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimal fakes
type fakePool struct {
	mu         sync.Mutex
	calls      int
	respondGet func(offset int64) []byte
	failFirst  int32
}

// roundTrip encodes `src` to a bin.Buffer and decodes it into `dst`.
// This is how a real Telegram round-trip would arrive at the typed client.
func roundTrip(src bin.Encoder, dst bin.Decoder) error {
	var b bin.Buffer
	if err := src.Encode(&b); err != nil {
		return err
	}
	return dst.Decode(&b)
}

func (p *fakePool) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if getReq, ok := input.(*tg.UploadGetFileRequest); ok {
		if atomic.LoadInt32(&p.failFirst) > 0 {
			atomic.AddInt32(&p.failFirst, -1)
			return errors.New("transient")
		}
		resp := &tg.UploadFile{Bytes: p.respondGet(getReq.Offset)}
		return roundTrip(resp, output)
	}
	return errors.New("unhandled rpc")
}
func (p *fakePool) Size() int    { return 1 }
func (p *fakePool) Close() error { return nil }

type fakeStore struct{}
func (fakeStore) UpdateFileReference(context.Context, int64, []byte) error { return nil }

type fakeTracker struct{ registered map[int64]int; mu sync.Mutex }
func (f *fakeTracker) Register(id int64, total int) {
	f.mu.Lock(); defer f.mu.Unlock()
	if f.registered == nil { f.registered = map[int64]int{} }
	f.registered[id] = total
}

type fakeRecorder struct {
	dl    atomic.Int64
	flood atomic.Int64
	retry atomic.Int64
	expired atomic.Int64
}
func (f *fakeRecorder) AddDownloadBytes(n int64) { f.dl.Add(n) }
func (f *fakeRecorder) IncFloodWait()            { f.flood.Add(1) }
func (f *fakeRecorder) IncRetry()                { f.retry.Add(1) }
func (f *fakeRecorder) IncFileRefExpired()       { f.expired.Add(1) }

func TestFetcher_DispatchesParallelChunks(t *testing.T) {
	pool := &fakePool{respondGet: func(offset int64) []byte {
		return []byte{byte(offset / 1024)}
	}}
	gate := &session.FloodGate{}
	tr := &fakeTracker{}
	rec := &fakeRecorder{}
	f := fetcher.New(pool, fakeStore{}, tr, gate, rec, fetcher.Config{
		Sessions: 2, ChunkSizeBytes: 1024, MaxRetriesPerChunk: 3,
	})
	jobs := make(chan state.Job, 1)
	out := make(chan types.Chunk, 16)
	jobs <- state.Job{MsgID: 42, FileID: 1, AccessHash: 1, FileReference: []byte{1}, Size: 3 * 1024}
	close(jobs)
	go func() { require.NoError(t, f.Run(context.Background(), jobs, out)); close(out) }()
	count := 0
	for range out {
		count++
	}
	assert.Equal(t, 3, count)
	assert.Equal(t, 3, tr.registered[42])
}

func TestFetcher_RetryOnTransient(t *testing.T) {
	pool := &fakePool{respondGet: func(offset int64) []byte { return []byte("ok") }, failFirst: 2}
	gate := &session.FloodGate{}
	rec := &fakeRecorder{}
	f := fetcher.New(pool, fakeStore{}, &fakeTracker{}, gate, rec, fetcher.Config{
		Sessions: 1, ChunkSizeBytes: 1024, MaxRetriesPerChunk: 5,
	})
	jobs := make(chan state.Job, 1)
	out := make(chan types.Chunk, 4)
	jobs <- state.Job{MsgID: 1, FileID: 1, AccessHash: 1, FileReference: []byte{1}, Size: 1024}
	close(jobs)
	go func() { require.NoError(t, f.Run(context.Background(), jobs, out)); close(out) }()
	got := 0
	for range out {
		got++
	}
	assert.Equal(t, 1, got)
	assert.GreaterOrEqual(t, rec.retry.Load(), int64(1))
}
```

- [ ] **Step 3: Run test (expect PASS)**

Run: `go test -race -v ./internal/fetcher/...`
Expected: tests PASS. If gotd type assertions fail, double-check `tg.UploadFile.Bytes` field name in your installed version (`go doc github.com/gotd/td/tg.UploadFile`).

- [ ] **Step 4: Commit**

```bash
git add internal/fetcher/
git commit -m "feat(fetcher): Stage 1 — parallel chunk fetch with FILE_REFERENCE refresh + FLOOD_WAIT handling"
```

---

## Chunk 3: Output stages (Writer + Uploader)

Build Stage 4 (Writer: batch records into 20MB files, expose backpressure) and Stage 5 (Uploader: parallel-part upload to Channel B, notify tracker).

### Task 3.1: `internal/writer` — Stage 4 (batch + backpressure)

**Files:**
- Create: `internal/writer/backpressure.go`
- Create: `internal/writer/writer.go`
- Create: `internal/writer/writer_test.go`

- [ ] **Step 1: Implement `backpressure.go`**

```go
package writer

import (
	"context"
	"os"
	"path/filepath"
	"time"
)

// BackpressureGate blocks the writer when too many output files are pending
// upload (i.e., still present on disk in OutputDir). Polls dir count at
// short intervals — coarse-grained but simple.
type BackpressureGate struct {
	Dir       string
	MaxFiles  int
	pollEvery time.Duration
}

func NewBackpressureGate(dir string, maxFiles int) *BackpressureGate {
	return &BackpressureGate{Dir: dir, MaxFiles: maxFiles, pollEvery: 200 * time.Millisecond}
}

func (g *BackpressureGate) Acquire(ctx context.Context) error {
	if g.MaxFiles <= 0 {
		return nil
	}
	for {
		n, err := countFiles(g.Dir)
		if err != nil {
			return err
		}
		if n < g.MaxFiles {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(g.pollEvery):
		}
	}
}

func countFiles(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".txt" {
			n++
		}
	}
	return n, nil
}
```

- [ ] **Step 2: Write failing test `writer_test.go`**

```go
package writer_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manh/tgpipe/internal/types"
	"github.com/manh/tgpipe/internal/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeTracker struct {
	mu      sync.Mutex
	flushed []struct{ ids []int64; path string }
}
func (f *fakeTracker) OutputFlushed(ids []int64, path string) {
	f.mu.Lock(); defer f.mu.Unlock()
	f.flushed = append(f.flushed, struct{ ids []int64; path string }{append([]int64{}, ids...), path})
}

func TestWriter_FlushOnSize(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTracker{}
	w := writer.New(writer.Config{
		OutputDir:        dir,
		BatchSizeMB:      1, // small for test
		FlushIntervalSec: 60,
		BatchSeqStart:    1,
	}, nil, tr)
	in := make(chan types.Record, 1024)
	out := make(chan types.OutputFile, 4)
	// Produce ~1MB of records
	go func() {
		for i := 0; i < 20000; i++ {
			in <- types.Record{MsgID: int64(i % 3), Email: []byte("u@x.com"), Pass: []byte("password1234567")}
		}
		close(in)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { require.NoError(t, w.Run(ctx, in, out)); close(out) }()
	count := 0
	var totalLines int
	for f := range out {
		count++
		totalLines += f.LineCount
		assert.FileExists(t, f.Path)
	}
	assert.Greater(t, count, 0)
	assert.Equal(t, 20000, totalLines)
}

func TestWriter_FlushOnInterval(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTracker{}
	w := writer.New(writer.Config{
		OutputDir:        dir,
		BatchSizeMB:      1000,
		FlushIntervalSec: 1, // 1s
		BatchSeqStart:    1,
	}, nil, tr)
	in := make(chan types.Record, 4)
	out := make(chan types.OutputFile, 4)
	in <- types.Record{MsgID: 1, Email: []byte("a@b.com"), Pass: []byte("x")}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx, in, out); close(out) }()
	select {
	case f := <-out:
		assert.Equal(t, 1, f.LineCount)
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("expected flush by interval")
	}
	close(in)
	<-done
}

func TestWriter_TracksSourceMsgIDs(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTracker{}
	w := writer.New(writer.Config{
		OutputDir:        dir,
		BatchSizeMB:      1,
		FlushIntervalSec: 60,
		BatchSeqStart:    1,
	}, nil, tr)
	in := make(chan types.Record, 100)
	out := make(chan types.OutputFile, 4)
	for i := 0; i < 30000; i++ {
		in <- types.Record{MsgID: int64(i % 4), Email: []byte("u@x.com"), Pass: []byte("paddingpaddingpadding")}
	}
	close(in)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Run(ctx, in, out); close(out) }()
	for f := range out {
		assert.NotEmpty(t, f.SourceMsgIDs)
		for _, id := range f.SourceMsgIDs {
			assert.True(t, id >= 0 && id < 4)
		}
	}
}

func TestWriter_FileNamingConvention(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTracker{}
	w := writer.New(writer.Config{
		OutputDir:        dir,
		BatchSizeMB:      1,
		FlushIntervalSec: 60,
		BatchSeqStart:    1,
	}, nil, tr)
	in := make(chan types.Record, 1024)
	out := make(chan types.OutputFile, 4)
	for i := 0; i < 30000; i++ {
		in <- types.Record{MsgID: 1, Email: []byte("a@b.com"), Pass: []byte("pad12345678901234567")}
	}
	close(in)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Run(ctx, in, out); close(out) }()
	for f := range out {
		base := filepath.Base(f.Path)
		assert.True(t, strings.HasPrefix(base, "out_"))
		assert.True(t, strings.HasSuffix(base, ".txt"))
	}
}

func TestWriter_FlushOnShutdownPreservesData(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTracker{}
	w := writer.New(writer.Config{
		OutputDir:        dir,
		BatchSizeMB:      1000, // never triggers by size
		FlushIntervalSec: 60,
		BatchSeqStart:    1,
	}, nil, tr)
	in := make(chan types.Record, 4)
	out := make(chan types.OutputFile, 4)
	in <- types.Record{MsgID: 1, Email: []byte("a@b.com"), Pass: []byte("x")}
	close(in)
	require.NoError(t, w.Run(context.Background(), in, out))
	close(out)
	files, _ := os.ReadDir(dir)
	assert.Len(t, files, 1, "expected one flushed file on graceful shutdown")
}
```

- [ ] **Step 3: Run test (expect FAIL)**

Run: `go test ./internal/writer/...`
Expected: build error.

- [ ] **Step 4: Implement `writer.go`**

```go
package writer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/manh/tgpipe/internal/types"
)

type Tracker interface {
	OutputFlushed(srcIDs []int64, path string)
}

type Config struct {
	OutputDir        string
	BatchSizeMB      int
	FlushIntervalSec int
	OutputChannelCap int
	BatchSeqStart    int // for tests; production = 1
}

type Writer struct {
	cfg      Config
	gate     *BackpressureGate
	tracker  Tracker
	seq      atomic.Int32
}

func New(cfg Config, gate *BackpressureGate, tr Tracker) *Writer {
	if cfg.BatchSeqStart < 1 {
		cfg.BatchSeqStart = 1
	}
	w := &Writer{cfg: cfg, gate: gate, tracker: tr}
	w.seq.Store(int32(cfg.BatchSeqStart - 1))
	return w
}

// Run consumes Records from `in`, batches them in-memory up to BatchSizeMB or
// FlushIntervalSec, and emits finalized OutputFile metadata on `out`. The
// writer is single-goroutine (the batch buffer is not shared across workers).
//
// On `in` close, flushes any remaining records (graceful shutdown preserves data).
func (w *Writer) Run(ctx context.Context, in <-chan types.Record, out chan<- types.OutputFile) error {
	if err := os.MkdirAll(w.cfg.OutputDir, 0o755); err != nil {
		return fmt.Errorf("mkdir output: %w", err)
	}
	var buf bytes.Buffer
	srcSet := make(map[int64]struct{})
	lineCount := 0
	threshold := w.cfg.BatchSizeMB * 1024 * 1024
	flushInterval := time.Duration(w.cfg.FlushIntervalSec) * time.Second
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()
	resetTimer := func() {
		if !timer.Stop() {
			select { case <-timer.C: default: }
		}
		timer.Reset(flushInterval)
	}
	flush := func() error {
		if buf.Len() == 0 {
			return nil
		}
		// Acquire backpressure BEFORE writing to disk — the gate exists to
		// prevent the output directory from growing unbounded. Acquiring after
		// the write would defeat that purpose (the disk has already grown).
		if w.gate != nil {
			if err := w.gate.Acquire(ctx); err != nil {
				return err
			}
		}
		seq := int(w.seq.Add(1))
		path := filepath.Join(w.cfg.OutputDir,
			fmt.Sprintf("out_%d_%04d.txt", time.Now().Unix(), seq))
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		size, err := f.Write(buf.Bytes())
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.Remove(path)
			return fmt.Errorf("write output: %w", err)
		}
		ids := make([]int64, 0, len(srcSet))
		for id := range srcSet {
			ids = append(ids, id)
		}
		of := types.OutputFile{
			Path: path, LineCount: lineCount, SizeBytes: int64(size),
			BatchSeq: seq, SourceMsgIDs: ids,
		}
		w.tracker.OutputFlushed(ids, path)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- of:
		}
		buf.Reset()
		srcSet = make(map[int64]struct{})
		lineCount = 0
		resetTimer()
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// Best-effort flush before exit. Log a shutdown-flush failure so
			// the operator notices data loss rather than silently discarding it.
			if ferr := flush(); ferr != nil {
				slog.Error("writer: shutdown flush failed", "err", ferr,
					"pending_lines", lineCount, "pending_bytes", buf.Len())
			}
			return ctx.Err()
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
		case rec, ok := <-in:
			if !ok {
				return flush()
			}
			buf.Write(rec.Email)
			buf.WriteByte(':')
			buf.Write(rec.Pass)
			buf.WriteByte('\n')
			srcSet[rec.MsgID] = struct{}{}
			lineCount++
			if buf.Len() >= threshold {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}
```

- [ ] **Step 5: Run test (expect PASS)**

Run: `go test -race -v ./internal/writer/...`
Expected: all tests PASS. Note: `TestWriter_FlushOnShutdownPreservesData` validates the graceful-shutdown behavior.

- [ ] **Step 6: Commit**

```bash
git add internal/writer/
git commit -m "feat(writer): Stage 4 — batch records into 20MB files with backpressure + source tracking"
```

---

### Task 3.2: `internal/uploader` — Stage 5 (parallel upload to Channel B)

**Files:**
- Create: `internal/uploader/uploader.go`
- Create: `internal/uploader/uploader_test.go`

The uploader streams each output file via `upload.saveFilePart` in parallel chunks, then calls `messages.sendMedia` to publish it on Channel B. On success, it removes the local file and notifies the tracker.

**Design constraints applied (CLAUDE.md §5):**
- **Stream upload, never `io.ReadAll`** — each part is read from disk INSIDE its own goroutine via `f.ReadAt` directly into a bounded buffer pool. At any moment at most `ParallelParts` part-sized buffers exist; the rest of the file stays on disk.
- **Typed gotd client** — `tg.NewClient(pool).UploadSaveFilePart(...)` returns `(bool, error)` directly. No hand-rolled `bin.Decoder` shim.
- **Target channel access hash** — `Config.TargetAccessHash` is required; resolved at startup via `internal/channels` and threaded down by the pipeline.
- **Fatal on rand failure** — `RandomID` collisions would result in duplicate-message rejection by Telegram. If `crypto/rand` fails the process must exit, not silently fall back to a timestamp.

- [ ] **Step 1: Implement `uploader.go`**

```go
package uploader

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/manh/tgpipe/internal/retry"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/types"
	"golang.org/x/sync/errgroup"
)

type Tracker interface {
	OutputUploaded(ctx context.Context, srcIDs []int64, path string) error
}

type Recorder interface {
	AddUploadBytes(int64)
	IncFloodWait()
	IncRetry()
}

type Config struct {
	Sessions         int
	ParallelParts    int
	TargetChannel    int64
	TargetAccessHash int64 // required — resolved at startup by internal/channels
}

type Uploader struct {
	pool     session.Pool
	api      *tg.Client // typed wrapper
	tracker  Tracker
	gate     *session.FloodGate
	recorder Recorder
	cfg      Config
}

func New(pool session.Pool, tracker Tracker, gate *session.FloodGate, rec Recorder, cfg Config) *Uploader {
	return &Uploader{
		pool:     pool,
		api:      tg.NewClient(pool),
		tracker:  tracker,
		gate:     gate,
		recorder: rec,
		cfg:      cfg,
	}
}

func (u *Uploader) Run(ctx context.Context, in <-chan types.OutputFile) error {
	var wg sync.WaitGroup
	for i := 0; i < u.cfg.Sessions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case of, ok := <-in:
					if !ok {
						return
					}
					if err := u.uploadOne(ctx, of); err != nil {
						if errors.Is(err, context.Canceled) {
							return
						}
						slog.Error("upload file", "stage", "uploader", "path", of.Path, "err", err)
						// File remains on disk; next run can retry.
						continue
					}
					if err := u.tracker.OutputUploaded(ctx, of.SourceMsgIDs, of.Path); err != nil {
						slog.Error("tracker notify", "stage", "uploader", "path", of.Path, "err", err)
						continue
					}
					if err := os.Remove(of.Path); err != nil {
						slog.Warn("remove uploaded file", "stage", "uploader", "path", of.Path, "err", err)
					}
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

const partSize = 512 * 1024 // 512KB — accepted by Telegram and a multiple of 1KB

// uploadOne streams the file via upload.saveFilePart in chunks, then publishes
// via messages.sendMedia. Parts are read from disk INSIDE each goroutine —
// only `ParallelParts` part-sized buffers exist in memory at any moment.
func (u *Uploader) uploadOne(ctx context.Context, of types.OutputFile) error {
	f, err := os.Open(of.Path)
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	defer f.Close()

	fileID, err := randomInt64()
	if err != nil {
		return fmt.Errorf("uploadOne: fileID: %w", err)
	}
	totalParts := int((of.SizeBytes + int64(partSize) - 1) / int64(partSize))

	// Bounded buffer pool: ParallelParts buffers, each partSize bytes.
	// Acquiring a buffer also acts as the parallelism semaphore.
	bufCh := make(chan []byte, u.cfg.ParallelParts)
	for i := 0; i < u.cfg.ParallelParts; i++ {
		bufCh <- make([]byte, partSize)
	}

	g, gctx := errgroup.WithContext(ctx)
	for partIdx := 0; partIdx < totalParts; partIdx++ {
		idx := partIdx
		// Acquire a buffer (blocks if all ParallelParts are in-flight).
		var buf []byte
		select {
		case buf = <-bufCh:
		case <-gctx.Done():
			return gctx.Err()
		}
		g.Go(func() error {
			defer func() { bufCh <- buf[:cap(buf)] }()
			// Read THIS part from disk inside the goroutine — no whole-file load.
			n, rerr := f.ReadAt(buf, int64(idx)*int64(partSize))
			if rerr != nil && !errors.Is(rerr, io.EOF) {
				return fmt.Errorf("read part %d: %w", idx, rerr)
			}
			part := buf[:n]
			if len(part) == 0 {
				return fmt.Errorf("part %d: zero bytes read", idx)
			}
			return retry.WithBackoff(gctx, 5, func() error {
				_, err := u.api.UploadSaveFilePart(gctx, &tg.UploadSaveFilePartRequest{
					FileID:   fileID,
					FilePart: idx,
					Bytes:    part,
				})
				if err != nil {
					if fw, sec := session.IsFloodWait(err); fw {
						u.recorder.IncFloodWait()
						u.gate.Trigger(time.Duration(sec+1) * time.Second)
						return retry.Retryable(err)
					}
					if tgerr.Is(err, "FILE_PARTS_INVALID", "FILE_PART_SIZE_INVALID") {
						// Permanent — caller should not retry this file.
						return err
					}
					u.recorder.IncRetry()
					return retry.Retryable(err)
				}
				return nil
			})
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	u.recorder.AddUploadBytes(of.SizeBytes)

	// Send media with caption.
	caption := fmt.Sprintf("Batch %d · %d records · %s",
		of.BatchSeq, of.LineCount, humanize(of.SizeBytes))
	media := &tg.InputMediaUploadedDocument{
		File: &tg.InputFile{
			ID:    fileID,
			Parts: totalParts,
			Name:  filepath.Base(of.Path),
		},
		MimeType:   "text/plain",
		Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: filepath.Base(of.Path)}},
	}
	sendReq := &tg.MessagesSendMediaRequest{
		Peer: &tg.InputPeerChannel{
			ChannelID:  u.cfg.TargetChannel,
			AccessHash: u.cfg.TargetAccessHash,
		},
		Media:    media,
		Message:  caption,
		RandomID: mustRandomInt64(),
	}
	return retry.WithBackoff(ctx, 5, func() error {
		_, err := u.api.MessagesSendMedia(ctx, sendReq)
		if err != nil {
			if fw, sec := session.IsFloodWait(err); fw {
				u.recorder.IncFloodWait()
				u.gate.Trigger(time.Duration(sec+1) * time.Second)
				return retry.Retryable(err)
			}
			u.recorder.IncRetry()
			return retry.Retryable(err)
		}
		return nil
	})
}

// helpers ---------------------------------------------------------------------

func randomInt64() (int64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[:])), nil
}

// mustRandomInt64 produces a cryptographically random int64 for the
// messages.sendMedia RandomID field. Telegram uses RandomID to deduplicate
// retried sends — collisions would silently drop messages. If rand fails we
// MUST NOT fall back to a timestamp (predictable, low entropy). The pipeline
// will surface this as a fatal error at the call site.
func mustRandomInt64() int64 {
	n, err := randomInt64()
	if err != nil {
		// In the rare event the OS RNG is unavailable, terminate loudly —
		// continuing would silently corrupt the output channel with duplicate
		// or dropped messages.
		panic(fmt.Sprintf("uploader: crypto/rand failed: %v", err))
	}
	return n
}

func humanize(n int64) string {
	const KB, MB, GB = 1024, 1024 * 1024, 1024 * 1024 * 1024
	switch {
	case n >= GB:
		return fmt.Sprintf("%.2f GB", float64(n)/GB)
	case n >= MB:
		return fmt.Sprintf("%.2f MB", float64(n)/MB)
	case n >= KB:
		return fmt.Sprintf("%.2f KB", float64(n)/KB)
	}
	return fmt.Sprintf("%d B", n)
}
```

- [ ] **Step 2: Write `uploader_test.go`**

```go
package uploader_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/types"
	"github.com/manh/tgpipe/internal/uploader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundTrip encodes src and decodes it into dst — mirrors a real Telegram round-trip
// so the typed client wrappers (tg.NewClient(pool).UploadSaveFilePart, …) decode
// the synthetic response correctly.
func roundTrip(src bin.Encoder, dst bin.Decoder) error {
	var b bin.Buffer
	if err := src.Encode(&b); err != nil {
		return err
	}
	return dst.Decode(&b)
}

type fakePool struct {
	mu        sync.Mutex
	saveParts int
	sendMedia int
}

func (p *fakePool) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch input.(type) {
	case *tg.UploadSaveFilePartRequest:
		p.saveParts++
		// upload.saveFilePart returns boolTrue.
		return roundTrip(&tg.BoolBox{Bool: &tg.BoolTrue{}}, output)
	case *tg.MessagesSendMediaRequest:
		p.sendMedia++
		// messages.sendMedia returns Updates — minimum non-nil to satisfy
		// the typed client's response shape.
		return roundTrip(&tg.UpdatesBox{Updates: &tg.Updates{}}, output)
	default:
		return errors.New("unhandled rpc")
	}
}
func (p *fakePool) Size() int    { return 1 }
func (p *fakePool) Close() error { return nil }

type fakeTracker struct {
	mu       sync.Mutex
	uploaded map[string][]int64
}

func (f *fakeTracker) OutputUploaded(_ context.Context, ids []int64, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.uploaded == nil {
		f.uploaded = map[string][]int64{}
	}
	f.uploaded[path] = append([]int64{}, ids...)
	return nil
}

type fakeRecorder struct{ up, flood, retry atomic.Int64 }

func (f *fakeRecorder) AddUploadBytes(n int64) { f.up.Add(n) }
func (f *fakeRecorder) IncFloodWait()          { f.flood.Add(1) }
func (f *fakeRecorder) IncRetry()              { f.retry.Add(1) }

func TestUploader_HappyPath(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out_1234_0001.txt")
	require.NoError(t, os.WriteFile(p, []byte("a@b.com:p1\nc@d.com:p2\n"), 0o644))

	pool := &fakePool{}
	tr := &fakeTracker{}
	rec := &fakeRecorder{}
	u := uploader.New(pool, tr, &session.FloodGate{}, rec, uploader.Config{
		Sessions: 1, ParallelParts: 2, TargetChannel: -100, TargetAccessHash: 999,
	})
	in := make(chan types.OutputFile, 1)
	in <- types.OutputFile{Path: p, LineCount: 2, SizeBytes: 22, BatchSeq: 1, SourceMsgIDs: []int64{42}}
	close(in)
	require.NoError(t, u.Run(context.Background(), in))

	assert.Equal(t, 1, pool.sendMedia)
	assert.GreaterOrEqual(t, pool.saveParts, 1)
	assert.Equal(t, int64(22), rec.up.Load())
	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Equal(t, []int64{42}, tr.uploaded[p])
	_, statErr := os.Stat(p)
	assert.True(t, os.IsNotExist(statErr), "file should be removed after upload")
}
```

- [ ] **Step 3: Run test (expect PASS)**

Run: `go test -race -v ./internal/uploader/...`
Expected: PASS. If gotd type mismatch occurs, double-check the gotd version's `tg.MessagesSendMediaRequest` field set and adjust accordingly (this is the integration-friction point).

- [ ] **Step 4: Commit**

```bash
git add internal/uploader/
git commit -m "feat(uploader): Stage 5 — parallel-part upload + sendMedia with caption"
```

---

## Chunk 4: Crawler + Telemetry + Pipeline orchestrator

Stage 0 (Crawler) walks the source channel and seeds the jobs table. New: `internal/channels` resolves channel access hashes once at startup. Telemetry counters + progress logger. Pipeline orchestrator wires every stage with errgroup.

### Task 4.0: `internal/channels` — channel access hash resolver

**Files:**
- Create: `internal/channels/resolver.go`
- Create: `internal/channels/resolver_test.go`

Every `tg.InputPeerChannel{ChannelID, AccessHash}` requires the channel-specific access hash. `messages.getDialogs` returns the chat objects for everything the user is a member of — we resolve both source and target channels at pipeline startup and pass the hashes down. This avoids re-querying for every job (file_reference refresh, upload publish, etc.).

- [ ] **Step 1: Implement `resolver.go`**

```go
package channels

import (
	"context"
	"errors"
	"fmt"

	"github.com/gotd/td/tg"
)

// Invoker is the subset of tg.Invoker the resolver needs. Pool satisfies it.
type Invoker interface {
	tg.Invoker
}

// Resolve walks the user's dialog list and returns the AccessHash for the
// channel whose ChannelID == chatID. Errors if the channel is not in the
// user's dialog list (the user must join/subscribe first).
func Resolve(ctx context.Context, inv Invoker, chatID int64) (int64, error) {
	api := tg.NewClient(inv)
	req := &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      500,
	}
	res, err := api.MessagesGetDialogs(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("getDialogs: %w", err)
	}
	var chats []tg.ChatClass
	switch v := res.(type) {
	case *tg.MessagesDialogs:
		chats = v.Chats
	case *tg.MessagesDialogsSlice:
		chats = v.Chats
	default:
		return 0, fmt.Errorf("unexpected dialogs response %T", res)
	}
	for _, c := range chats {
		ch, ok := c.(*tg.Channel)
		if !ok {
			continue
		}
		if ch.ID == chatID {
			return ch.AccessHash, nil
		}
	}
	return 0, errors.New("channel not found in dialogs — make sure the account has joined it")
}
```

- [ ] **Step 2: Write `resolver_test.go`**

```go
package channels_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/manh/tgpipe/internal/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeInv struct{ resp bin.Encoder }

func (f *fakeInv) Invoke(_ context.Context, _ bin.Encoder, out bin.Decoder) error {
	if f.resp == nil {
		return errors.New("no response configured")
	}
	var b bin.Buffer
	if err := f.resp.Encode(&b); err != nil {
		return err
	}
	return out.Decode(&b)
}

func TestResolve_FindsChannel(t *testing.T) {
	inv := &fakeInv{resp: &tg.MessagesDialogsBox{Dialogs: &tg.MessagesDialogs{
		Chats: []tg.ChatClass{
			&tg.Channel{ID: 100, AccessHash: 42},
			&tg.Channel{ID: 200, AccessHash: 99},
		},
	}}}
	got, err := channels.Resolve(context.Background(), inv, 200)
	require.NoError(t, err)
	assert.Equal(t, int64(99), got)
}

func TestResolve_NotFound(t *testing.T) {
	inv := &fakeInv{resp: &tg.MessagesDialogsBox{Dialogs: &tg.MessagesDialogs{}}}
	_, err := channels.Resolve(context.Background(), inv, 200)
	assert.Error(t, err)
}
```

- [ ] **Step 3: Run test (expect PASS)**

Run: `go test -race -v ./internal/channels/...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/channels/
git commit -m "feat(channels): resolve channel access hash via dialogs"
```

---

### Task 4.1: `internal/crawler` — Stage 0

**Files:**
- Create: `internal/crawler/crawler.go`

The crawler iterates messages in the source channel via `messages.getHistory`, filters those with `.txt` documents, and inserts/upserts each into the jobs table. It uses the channel access hash resolved by `internal/channels` so `tg.InputPeerChannel` is fully-formed.

- [ ] **Step 1: Implement `crawler.go`**

```go
package crawler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
)

type Store interface {
	InsertJobIfAbsent(ctx context.Context, j state.Job) error
}

type Config struct {
	SourceChannel    int64
	SourceAccessHash int64 // resolved by internal/channels at pipeline startup
	BatchSize        int   // messages per page
}

type Crawler struct {
	pool  session.Pool
	api   *tg.Client
	store Store
	cfg   Config
}

func New(pool session.Pool, store Store, cfg Config) *Crawler {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	return &Crawler{pool: pool, api: tg.NewClient(pool), store: store, cfg: cfg}
}

// Run iterates messages in the source channel from newest to oldest, inserting
// each `.txt` document message as a job. Idempotent — re-running skips existing
// rows (INSERT OR IGNORE).
func (c *Crawler) Run(ctx context.Context) error {
	if c.cfg.SourceAccessHash == 0 {
		return errors.New("crawler: SourceAccessHash is zero — call channels.Resolve before Run")
	}
	var offsetID int
	total := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		res, err := c.api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer: &tg.InputPeerChannel{
				ChannelID:  c.cfg.SourceChannel,
				AccessHash: c.cfg.SourceAccessHash,
			},
			OffsetID: offsetID,
			Limit:    c.cfg.BatchSize,
		})
		if err != nil {
			return fmt.Errorf("getHistory: %w", err)
		}
		msgs := extractMessages(res)
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			j, ok := buildJob(c.cfg.SourceChannel, c.cfg.SourceAccessHash, m)
			if !ok {
				continue
			}
			if err := c.store.InsertJobIfAbsent(ctx, j); err != nil {
				return err
			}
			total++
		}
		offsetID = lastMsgID(msgs)
		slog.Info("crawl progress", "stage", "crawler", "inserted_total", total, "offset_id", offsetID)
	}
	slog.Info("crawl done", "stage", "crawler", "total", total)
	return nil
}

func extractMessages(res tg.MessagesMessagesClass) []*tg.Message {
	var raw []tg.MessageClass
	switch v := res.(type) {
	case *tg.MessagesMessages:
		raw = v.Messages
	case *tg.MessagesMessagesSlice:
		raw = v.Messages
	case *tg.MessagesChannelMessages:
		raw = v.Messages
	default:
		return nil
	}
	out := make([]*tg.Message, 0, len(raw))
	for _, m := range raw {
		if msg, ok := m.(*tg.Message); ok {
			out = append(out, msg)
		}
	}
	return out
}

// lastMsgID returns the smallest MsgID in the page — used as the next OffsetID
// since getHistory pages newest-to-oldest.
func lastMsgID(msgs []*tg.Message) int {
	minID := msgs[0].ID
	for _, m := range msgs {
		if m.ID < minID {
			minID = m.ID
		}
	}
	return minID
}

func buildJob(chatID, chatAccessHash int64, msg *tg.Message) (state.Job, bool) {
	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		return state.Job{}, false
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		return state.Job{}, false
	}
	name := docFileName(doc)
	if !strings.HasSuffix(strings.ToLower(name), ".txt") {
		return state.Job{}, false
	}
	now := time.Now()
	return state.Job{
		MsgID:          int64(msg.ID),
		ChatID:         chatID,
		ChatAccessHash: chatAccessHash,
		FileID:         doc.ID,
		AccessHash:     doc.AccessHash,
		FileReference:  doc.FileReference,
		DCID:           doc.DCID,
		Size:           doc.Size,
		FileName:       name,
		MimeType:       doc.MimeType,
		Status:         state.StatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, true
}

func docFileName(doc *tg.Document) string {
	for _, a := range doc.Attributes {
		if fn, ok := a.(*tg.DocumentAttributeFilename); ok {
			return fn.FileName
		}
	}
	return ""
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./...`
Expected: passes.

- [ ] **Step 3: Commit**

```bash
git add internal/crawler/
git commit -m "feat(crawler): Stage 0 — walk source channel and seed jobs table"
```

---

### Task 4.2: `internal/telemetry` — counters + recorder + progress logger

**Files:**
- Create: `internal/telemetry/counters.go`
- Create: `internal/telemetry/recorder.go`
- Create: `internal/telemetry/logger.go`

- [ ] **Step 1: Implement `counters.go`**

```go
package telemetry

import "sync/atomic"

type Counters struct {
	DownloadBytes       atomic.Int64
	UploadBytes         atomic.Int64
	LinesEmitted        atomic.Int64
	DroppedInvalidLines atomic.Int64
	DroppedEdgeBytes    atomic.Int64
	FloodWaits          atomic.Int64
	Retries             atomic.Int64
	FileRefExpiredHits  atomic.Int64
	JobsDone            atomic.Int64
	JobsFailed          atomic.Int64
}

type Snapshot struct {
	DownloadBytes, UploadBytes                int64
	LinesEmitted, DroppedInvalidLines         int64
	DroppedEdgeBytes                          int64
	FloodWaits, Retries, FileRefExpiredHits   int64
	JobsDone, JobsFailed                      int64
}

func (c *Counters) Snapshot() Snapshot {
	return Snapshot{
		DownloadBytes:       c.DownloadBytes.Load(),
		UploadBytes:         c.UploadBytes.Load(),
		LinesEmitted:        c.LinesEmitted.Load(),
		DroppedInvalidLines: c.DroppedInvalidLines.Load(),
		DroppedEdgeBytes:    c.DroppedEdgeBytes.Load(),
		FloodWaits:          c.FloodWaits.Load(),
		Retries:             c.Retries.Load(),
		FileRefExpiredHits:  c.FileRefExpiredHits.Load(),
		JobsDone:            c.JobsDone.Load(),
		JobsFailed:          c.JobsFailed.Load(),
	}
}
```

- [ ] **Step 2: Implement `recorder.go` (adapter satisfying every stage's small interface)**

```go
package telemetry

// Recorder is the wide interface satisfied by *Counters and consumed by every
// stage. Stages depend on a narrow subset; we adapt with method promotion.

func (c *Counters) AddDownloadBytes(n int64) { c.DownloadBytes.Add(n) }
func (c *Counters) AddUploadBytes(n int64)   { c.UploadBytes.Add(n) }
func (c *Counters) IncLinesEmitted()         { c.LinesEmitted.Add(1) }
func (c *Counters) IncDroppedInvalidLine()   { c.DroppedInvalidLines.Add(1) }
func (c *Counters) AddDroppedEdgeBytes(n int64) { c.DroppedEdgeBytes.Add(n) }
func (c *Counters) IncFloodWait()            { c.FloodWaits.Add(1) }
func (c *Counters) IncRetry()                { c.Retries.Add(1) }
func (c *Counters) IncFileRefExpired()       { c.FileRefExpiredHits.Add(1) }
func (c *Counters) IncJobDone()              { c.JobsDone.Add(1) }
func (c *Counters) IncJobFailed()            { c.JobsFailed.Add(1) }
```

- [ ] **Step 3: Implement `logger.go` (progress logger)**

```go
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type Gauges struct {
	ChunkChan  func() (used, cap int)
	LineChan   func() (used, cap int)
	RecordChan func() (used, cap int)
	OutputChan func() (used, cap int)
}

// StatsFetcher pulls job-status counts from the state store. Returning an error
// means "stats unavailable this tick" — the logger logs zeros rather than failing.
type StatsFetcher func(ctx context.Context) (pending, inprog, done, failed int64, err error)

type Logger struct {
	counters    *Counters
	gauges      Gauges
	statsFetcher StatsFetcher
	interval    time.Duration
}

func NewLogger(c *Counters, g Gauges, sf StatsFetcher, interval time.Duration) *Logger {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Logger{counters: c, gauges: g, statsFetcher: sf, interval: interval}
}

func (l *Logger) Run(ctx context.Context) error {
	prev := l.counters.Snapshot()
	prevTime := time.Now()
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			cur := l.counters.Snapshot()
			dt := now.Sub(prevTime).Seconds()
			if dt <= 0 {
				continue
			}
			dl := float64(cur.DownloadBytes-prev.DownloadBytes) / dt / (1024 * 1024)
			up := float64(cur.UploadBytes-prev.UploadBytes) / dt / (1024 * 1024)

			pending, inprog, done, failed := int64(0), int64(0), int64(0), int64(0)
			if l.statsFetcher != nil {
				p, ip, d, f, _ := l.statsFetcher(ctx)
				pending, inprog, done, failed = p, ip, d, f
			}

			slog.Info("progress",
				"stage", "telemetry",
				"download_mbps", fmt.Sprintf("%.1f", dl),
				"upload_mbps", fmt.Sprintf("%.1f", up),
				"chunk_q", fmtQ(l.gauges.ChunkChan),
				"line_q", fmtQ(l.gauges.LineChan),
				"record_q", fmtQ(l.gauges.RecordChan),
				"output_q", fmtQ(l.gauges.OutputChan),
				"jobs_done", done,
				"jobs_inprog", inprog,
				"jobs_pending", pending,
				"jobs_failed", failed,
				"floods_delta", cur.FloodWaits-prev.FloodWaits,
				"retries_delta", cur.Retries-prev.Retries,
				"dropped_lines_delta", cur.DroppedInvalidLines-prev.DroppedInvalidLines,
			)
			prev = cur
			prevTime = now
		}
	}
}

func fmtQ(fn func() (int, int)) string {
	if fn == nil {
		return "-/-"
	}
	used, cap := fn()
	return fmt.Sprintf("%d/%d", used, cap)
}
```

- [ ] **Step 4: Verify build**

Run: `go build ./...`
Expected: passes.

- [ ] **Step 5: Commit**

```bash
git add internal/telemetry/
git commit -m "feat(telemetry): counters + recorder adapters + progress logger"
```

---

### Task 4.3: `internal/pipeline` — orchestrator

**Files:**
- Create: `internal/pipeline/pipeline.go`

- [ ] **Step 1: Implement `pipeline.go`**

```go
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/manh/tgpipe/internal/channels"
	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/fetcher"
	"github.com/manh/tgpipe/internal/processor"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/splitter"
	"github.com/manh/tgpipe/internal/state"
	"github.com/manh/tgpipe/internal/telemetry"
	"github.com/manh/tgpipe/internal/tracker"
	"github.com/manh/tgpipe/internal/types"
	"github.com/manh/tgpipe/internal/uploader"
	"github.com/manh/tgpipe/internal/writer"
	"golang.org/x/sync/errgroup"
)

type Pipeline struct {
	cfg        *config.Config
	store      *state.Store
	fetchPool  session.Pool
	uploadPool session.Pool
	gate       *session.FloodGate
	tracker    *tracker.SourceTracker
	counters   *telemetry.Counters
}

func New(cfg *config.Config, store *state.Store, fetchPool, uploadPool session.Pool, gate *session.FloodGate) *Pipeline {
	return &Pipeline{
		cfg:        cfg,
		store:      store,
		fetchPool:  fetchPool,
		uploadPool: uploadPool,
		gate:       gate,
		tracker:    tracker.New(store),
		counters:   &telemetry.Counters{},
	}
}

// Run starts the orchestrator. Assumes store.Init(ctx) has already been called
// by the caller (cmd/tgpipe/cmd_run.go). store.Init runs the resume SQL:
//
//	UPDATE jobs SET status = 'pending' WHERE status = 'in_progress'
//
// so any rows half-processed by a crashed run are picked up again here.
func (p *Pipeline) Run(ctx context.Context) error {
	// Resolve channel access hashes once at startup (via messages.getDialogs).
	// New jobs crawled later need a hash too; the cmd_run.go startup also passes
	// srcHash into the crawler, but the pipeline itself only needs dstHash to
	// upload (jobs already carry chat_access_hash for the fetch side).
	srcHash, err := channels.Resolve(ctx, p.fetchPool, p.cfg.SourceChannel)
	if err != nil {
		return fmt.Errorf("resolve source channel access hash: %w", err)
	}
	dstHash, err := channels.Resolve(ctx, p.uploadPool, p.cfg.TargetChannel)
	if err != nil {
		return fmt.Errorf("resolve target channel access hash: %w", err)
	}
	slog.Info("channels resolved", "stage", "pipeline",
		"source", p.cfg.SourceChannel, "src_hash", srcHash,
		"target", p.cfg.TargetChannel)
	// srcHash is logged for diagnostics; jobs already carry chat_access_hash
	// per-row (populated by the crawler), so the pipeline does not need to
	// thread srcHash through the fetcher.

	jobsCh := make(chan state.Job, p.cfg.Fetcher.JobChannelCap)
	chunkCh := make(chan types.Chunk, p.cfg.Fetcher.ChunkChannelCap)
	lineCh := make(chan types.Line, p.cfg.Splitter.LineChannelCap)
	recordCh := make(chan types.Record, p.cfg.Processor.RecordChannelCap)
	outputCh := make(chan types.OutputFile, p.cfg.Writer.OutputChannelCap)

	gauges := telemetry.Gauges{
		ChunkChan:  func() (int, int) { return len(chunkCh), cap(chunkCh) },
		LineChan:   func() (int, int) { return len(lineCh), cap(lineCh) },
		RecordChan: func() (int, int) { return len(recordCh), cap(recordCh) },
		OutputChan: func() (int, int) { return len(outputCh), cap(outputCh) },
	}
	statsFn := func(ctx context.Context) (int64, int64, int64, int64, error) {
		s, err := p.store.Stats(ctx)
		return s.Pending, s.InProgress, s.Done, s.Failed, err
	}
	progress := telemetry.NewLogger(p.counters, gauges, statsFn,
		time.Duration(p.cfg.Logging.ProgressIntervalSec)*time.Second)

	splWorkers := p.cfg.Splitter.Workers
	if splWorkers <= 0 {
		splWorkers = runtime.NumCPU()
	}
	procWorkers := p.cfg.Processor.Workers
	if procWorkers <= 0 {
		procWorkers = runtime.NumCPU() * 2
	}

	fetch := fetcher.New(p.fetchPool, p.store, p.tracker, p.gate, p.counters, fetcher.Config{
		Sessions:           p.cfg.Fetcher.Sessions,
		ChunkSizeBytes:     p.cfg.Fetcher.ChunkSizeBytes,
		MaxRetriesPerChunk: p.cfg.Fetcher.MaxRetriesPerChunk,
	})
	spl := splitter.New(splWorkers, p.tracker)
	proc := processor.New(procWorkers, &processor.UrlUserPassExtractor{}, p.counters)
	bp := writer.NewBackpressureGate(p.cfg.Writer.OutputDir, p.cfg.Backpressure.MaxPendingOutputFiles)
	w := writer.New(writer.Config{
		OutputDir:        p.cfg.Writer.OutputDir,
		BatchSizeMB:      p.cfg.Writer.BatchSizeMB,
		FlushIntervalSec: p.cfg.Writer.FlushIntervalSec,
		OutputChannelCap: p.cfg.Writer.OutputChannelCap,
		BatchSeqStart:    1,
	}, bp, p.tracker)
	up := uploader.New(p.uploadPool, p.tracker, p.gate, p.counters, uploader.Config{
		Sessions:         p.cfg.Uploader.Sessions,
		ParallelParts:    p.cfg.Uploader.ParallelParts,
		TargetChannel:    p.cfg.TargetChannel,
		TargetAccessHash: dstHash,
	})

	g, gctx := errgroup.WithContext(ctx)

	// Job feeder: read pending jobs from DB → jobsCh, close when no more.
	g.Go(func() error {
		defer close(jobsCh)
		const batch = 16
		for {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}
			jobs, err := p.store.PickPending(gctx, batch)
			if err != nil {
				return err
			}
			if len(jobs) == 0 {
				return nil
			}
			for _, j := range jobs {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case jobsCh <- j:
				}
			}
		}
	})
	g.Go(func() error { defer close(chunkCh); return fetch.Run(gctx, jobsCh, chunkCh) })
	g.Go(func() error { defer close(lineCh); return spl.Run(gctx, chunkCh, lineCh) })
	g.Go(func() error { defer close(recordCh); return proc.Run(gctx, lineCh, recordCh) })
	g.Go(func() error { defer close(outputCh); return w.Run(gctx, recordCh, outputCh) })
	g.Go(func() error { return up.Run(gctx, outputCh) })
	g.Go(func() error { return progress.Run(gctx) })

	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./...`
Expected: compiles. Fix any missing imports (`time`).

- [ ] **Step 3: Commit**

```bash
git add internal/pipeline/
git commit -m "feat(pipeline): orchestrator wiring all 5 stages with errgroup + access-hash resolve"
```

---

## Chunk 5: CLI commands + integration tests + polish

Wire cobra subcommands, add a full-pipeline integration test against a fake session pool, finalize README + config example.

### Task 5.1: CLI subcommands

**Files:**
- Modify: `cmd/tgpipe/main.go`
- Create: `cmd/tgpipe/cmd_auth.go`
- Create: `cmd/tgpipe/cmd_crawl.go`
- Create: `cmd/tgpipe/cmd_run.go`
- Create: `cmd/tgpipe/cmd_stats.go`
- Create: `cmd/tgpipe/cmd_retry.go`
- Create: `cmd/tgpipe/cmd_reset.go`

- [ ] **Step 1: Modify `cmd/tgpipe/main.go` to add global flag + register subcommands**

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// version is overridden at build time via:
//   go build -ldflags "-X main.version=$(git describe --tags --always)" ./cmd/tgpipe
var version = "dev"

var (
	cfgPath        string
	debugPprof     bool
	logLevelFlag   string // overrides config.logging.level when non-empty
)

var rootCmd = &cobra.Command{
	Use:     "tgpipe",
	Short:   "Telegram bulk downloader & republisher",
	Version: version,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgPath, "config", "config.yaml", "path to config YAML")
	rootCmd.PersistentFlags().BoolVar(&debugPprof, "debug-pprof", false, "enable pprof on 127.0.0.1:6060")
	rootCmd.PersistentFlags().StringVar(&logLevelFlag, "log-level", "", "override logging.level (debug|info|warn|error)")
	rootCmd.AddCommand(authCmd, crawlCmd, runCmd, statsCmd, retryCmd, resetCmd)
}

// resolveLogLevel returns the effective log level: CLI flag wins over config.
func resolveLogLevel(fromConfig string) string {
	if logLevelFlag != "" {
		return logLevelFlag
	}
	return fromConfig
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Create `cmd_auth.go` — interactive MTProto login**

The auth subcommand drives gotd's interactive auth flow. It writes the
authenticated session to `account.session_file`. Run this **once** per VPS
before `crawl` / `run`. Subsequent invocations are no-ops if the session is
still valid.

```go
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/manh/tgpipe/internal/config"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Interactive MTProto login — writes session file",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		if cfg.Account.APIID == 0 || cfg.Account.APIHash == "" {
			return fmt.Errorf("account.api_id and account.api_hash must be set in %s", cfgPath)
		}
		if dir := filepath.Dir(cfg.Account.SessionFile); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return err
			}
		}
		client := telegram.NewClient(cfg.Account.APIID, cfg.Account.APIHash, telegram.Options{
			SessionStorage: &session.FileStorage{Path: cfg.Account.SessionFile},
		})
		return client.Run(ctx, func(ctx context.Context) error {
			flow := auth.NewFlow(termAuth{}, auth.SendCodeOptions{})
			if err := client.Auth().IfNecessary(ctx, flow); err != nil {
				return err
			}
			self, err := client.Self(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("authenticated as @%s (id=%d) — session written to %s\n",
				self.Username, self.ID, cfg.Account.SessionFile)
			return nil
		})
	},
}

// termAuth implements auth.UserAuthenticator by prompting on stdin/stderr.
type termAuth struct{}

func (termAuth) Phone(_ context.Context) (string, error) {
	fmt.Fprint(os.Stderr, "Phone (+E.164, e.g. +84901234567): ")
	r := bufio.NewReader(os.Stdin)
	s, err := r.ReadString('\n')
	return strings.TrimSpace(s), err
}

func (termAuth) Password(_ context.Context) (string, error) {
	fmt.Fprint(os.Stderr, "2FA password: ")
	// int(os.Stdin.Fd()) is portable across Unix and Windows; syscall.Stdin
	// is a Handle (uintptr) on Windows and won't convert cleanly to int.
	pw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	return strings.TrimSpace(string(pw)), err
}

func (termAuth) Code(_ context.Context, _ *auth.SentCode) (string, error) {
	fmt.Fprint(os.Stderr, "Code: ")
	r := bufio.NewReader(os.Stdin)
	s, err := r.ReadString('\n')
	return strings.TrimSpace(s), err
}

func (termAuth) AcceptTermsOfService(_ context.Context, tos auth.TermsOfService) error {
	fmt.Fprintln(os.Stderr, "ToS:", tos.Text)
	return nil
}

func (termAuth) SignUp(_ context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, fmt.Errorf("sign-up not supported; use an existing account")
}
```

> **Verify:** `gotd/td` exposes `auth.Flow`, `auth.UserAuthenticator`,
> `auth.SendCodeOptions`. If signatures differ in the pinned `gotd/td` version,
> consult `pkg.go.dev/github.com/gotd/td/telegram/auth` and adjust accordingly.
> The `golang.org/x/term` dependency must be added: `go get golang.org/x/term`.

- [ ] **Step 3: Create `cmd_crawl.go`**

```go
package main

import (
	"github.com/manh/tgpipe/internal/channels"
	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/crawler"
	"github.com/manh/tgpipe/internal/logging"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
	"github.com/spf13/cobra"
)

var crawlCmd = &cobra.Command{
	Use:   "crawl",
	Short: "Walk source channel and seed jobs table",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		if _, err := logging.Setup(logging.Config{
			Level:  resolveLogLevel(cfg.Logging.Level),
			Format: cfg.Logging.Format,
		}); err != nil {
			return err
		}
		store, err := state.Open(cfg.State.DBPath)
		if err != nil {
			return err
		}
		defer store.Close()
		// Init runs migrations and the resume SQL
		// (UPDATE jobs SET status='pending' WHERE status='in_progress').
		if err := store.Init(ctx); err != nil {
			return err
		}
		gate := &session.FloodGate{}
		pool, err := session.NewFetchPool(ctx, session.Config{
			APIID: cfg.Account.APIID, APIHash: cfg.Account.APIHash,
			SessionFile: cfg.Account.SessionFile, Size: 1,
		}, gate)
		if err != nil {
			return err
		}
		defer pool.Close()
		srcHash, err := channels.Resolve(ctx, pool, cfg.SourceChannel)
		if err != nil {
			return err
		}
		c := crawler.New(pool, store, crawler.Config{
			SourceChannel:    cfg.SourceChannel,
			SourceAccessHash: srcHash,
			BatchSize:        100,
		})
		return c.Run(ctx)
	},
}
```

- [ ] **Step 4: Create `cmd_run.go`**

```go
package main

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/logging"
	"github.com/manh/tgpipe/internal/pipeline"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Start the 5-stage pipeline",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		if _, err := logging.Setup(logging.Config{
			Level:  resolveLogLevel(cfg.Logging.Level),
			Format: cfg.Logging.Format,
		}); err != nil {
			return err
		}
		if debugPprof {
			go func() { _ = http.ListenAndServe("127.0.0.1:6060", nil) }()
		}
		store, err := state.Open(cfg.State.DBPath)
		if err != nil {
			return err
		}
		defer store.Close()
		// Init runs migrations + the resume SQL
		// (UPDATE jobs SET status='pending' WHERE status='in_progress').
		// Any rows half-processed by a crashed run get picked up by PickPending
		// inside Pipeline.Run.
		if err := store.Init(ctx); err != nil {
			return err
		}
		gate := &session.FloodGate{}
		fetchPool, err := session.NewFetchPool(ctx, session.Config{
			APIID: cfg.Account.APIID, APIHash: cfg.Account.APIHash,
			SessionFile: cfg.Account.SessionFile, Size: cfg.Fetcher.Sessions,
		}, gate)
		if err != nil {
			return err
		}
		defer fetchPool.Close()
		uploadPool, err := session.NewUploadPool(ctx, session.Config{
			APIID: cfg.Account.APIID, APIHash: cfg.Account.APIHash,
			SessionFile: cfg.Account.SessionFile, Size: cfg.Uploader.Sessions,
		}, gate)
		if err != nil {
			return err
		}
		defer uploadPool.Close()
		p := pipeline.New(cfg, store, fetchPool, uploadPool, gate)
		return p.Run(ctx)
	},
}
```

- [ ] **Step 5: Create `cmd_stats.go`**

```go
package main

import (
	"fmt"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/state"
	"github.com/spf13/cobra"
)

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show DB summary",
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		store, err := state.Open(cfg.State.DBPath)
		if err != nil {
			return err
		}
		defer store.Close()
		s, err := store.Stats(cmd.Context())
		if err != nil {
			return err
		}
		fmt.Printf("pending:      %d\n", s.Pending)
		fmt.Printf("in_progress:  %d\n", s.InProgress)
		fmt.Printf("done:         %d\n", s.Done)
		fmt.Printf("failed:       %d\n", s.Failed)
		fmt.Printf("total size:   %d bytes\n", s.TotalSize)
		if s.TotalSize > 0 {
			fmt.Printf("completed:    %.1f%% (%d / %d bytes)\n",
				100*float64(s.DoneSize)/float64(s.TotalSize), s.DoneSize, s.TotalSize)
		}
		return nil
	},
}
```

- [ ] **Step 6: Create `cmd_retry.go`**

```go
package main

import (
	"context"
	"fmt"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/state"
	"github.com/spf13/cobra"
)

var retryStatusFlag string

var retryCmd = &cobra.Command{
	Use:   "retry",
	Short: "Reset jobs with the given status back to pending",
	RunE: func(cmd *cobra.Command, _ []string) error {
		if retryStatusFlag == "" {
			return fmt.Errorf("--status is required (e.g. failed)")
		}
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		store, err := state.Open(cfg.State.DBPath)
		if err != nil {
			return err
		}
		defer store.Close()
		return store.ResetStatus(context.Background(), state.JobStatus(retryStatusFlag), state.StatusPending)
	},
}

func init() {
	retryCmd.Flags().StringVar(&retryStatusFlag, "status", "", "current status to flip back to pending (e.g. failed)")
}
```

- [ ] **Step 7: Create `cmd_reset.go`**

```go
package main

import (
	"context"
	"strconv"
	"strings"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/state"
	"github.com/spf13/cobra"
)

var resetMsgIDs string

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset specific msg_ids back to pending",
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		store, err := state.Open(cfg.State.DBPath)
		if err != nil {
			return err
		}
		defer store.Close()
		ctx := context.Background()
		for _, s := range strings.Split(resetMsgIDs, ",") {
			s = strings.TrimSpace(s)
			if s == "" { continue }
			id, perr := strconv.ParseInt(s, 10, 64)
			if perr != nil { return perr }
			if err := store.ResetMsgID(ctx, id); err != nil { return err }
		}
		return nil
	},
}

func init() {
	resetCmd.Flags().StringVar(&resetMsgIDs, "msg-ids", "", "comma-separated msg_ids to reset")
}
```

- [ ] **Step 8: Add `ResetStatus` + `ResetMsgID` to `internal/state/store.go`**

```go
// Append to store.go

func (s *Store) ResetStatus(ctx context.Context, from, to JobStatus) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status=?, updated_at=?, retries=0, error_msg='' WHERE status=?`,
		string(to), time.Now().Unix(), string(from),
	)
	return err
}

func (s *Store) ResetMsgID(ctx context.Context, msgID int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status=?, updated_at=?, retries=0, error_msg='' WHERE msg_id=?`,
		string(StatusPending), time.Now().Unix(), msgID,
	)
	return err
}
```

- [ ] **Step 9: Verify build + run CLI sanity**

Run: `go build -o bin/tgpipe ./cmd/tgpipe`
Expected: produces `bin/tgpipe`.
Run: `./bin/tgpipe --help`
Expected: prints command list (auth, crawl, run, stats, retry, reset) and `--version`, `--log-level`, `--config`, `--debug-pprof` flags.

- [ ] **Step 10: Commit**

```bash
git add cmd/ internal/state/store.go
git commit -m "feat(cli): cobra subcommands crawl/run/stats/retry/reset"
```

---

### Task 5.2: Integration test (fake session pool, full pipeline)

**Files:**
- Create: `tests/integration/pipeline_test.go`

The integration test wires the real `Pipeline` orchestrator with a fake `session.Pool` that serves synthetic `.txt` files in-memory. It validates end-to-end: jobs → fetch → split → process → write → upload → done.

- [ ] **Step 1: Implement integration test**

```go
package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/pipeline"
	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundTrip encodes src to a bin.Buffer and decodes it into dst, mimicking
// what an MTProto transport would do over the wire. Used by fakePool to
// satisfy the contract of tg.Invoker (which the typed tg.NewClient wrapper
// requires).
func roundTrip(t *testing.T, src bin.Encoder, dst bin.Decoder) error {
	t.Helper()
	var buf bin.Buffer
	if err := src.Encode(&buf); err != nil {
		return err
	}
	return dst.Decode(&buf)
}

// fakePool implements session.Pool (which embeds tg.Invoker).
// It serves synthetic .txt content for upload.getFile, accepts
// upload.saveFilePart and messages.sendMedia, and emulates messages.getDialogs
// so channels.Resolve can find the source/target peers.
type fakePool struct {
	t        *testing.T
	mu       sync.Mutex
	content  map[int64][]byte // fileID → bytes
	srcID    int64
	srcHash  int64
	dstID    int64
	dstHash  int64
}

func (p *fakePool) Invoke(_ context.Context, in bin.Encoder, out bin.Decoder) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch r := in.(type) {
	case *tg.UploadGetFileRequest:
		loc, ok := r.Location.(*tg.InputDocumentFileLocation)
		if !ok {
			return fmt.Errorf("unknown loc %T", r.Location)
		}
		data := p.content[loc.ID]
		start := int(r.Offset)
		end := start + r.Limit
		if start > len(data) {
			start = len(data)
		}
		if end > len(data) {
			end = len(data)
		}
		resp := &tg.UploadFile{Bytes: data[start:end]}
		return roundTrip(p.t, resp, out)

	case *tg.UploadSaveFilePartRequest:
		return roundTrip(p.t, &tg.BoolBox{Bool: &tg.BoolTrue{}}, out)

	case *tg.MessagesSendMediaRequest:
		return roundTrip(p.t, &tg.UpdatesBox{Updates: &tg.Updates{}}, out)

	case *tg.MessagesGetDialogsRequest:
		dlg := &tg.MessagesDialogs{
			Chats: []tg.ChatClass{
				&tg.Channel{ID: p.srcID, AccessHash: p.srcHash},
				&tg.Channel{ID: p.dstID, AccessHash: p.dstHash},
			},
		}
		return roundTrip(p.t, &tg.MessagesDialogsBox{Dialogs: dlg}, out)

	default:
		return fmt.Errorf("unhandled %T", r)
	}
}

func (p *fakePool) Size() int    { return 1 }
func (p *fakePool) Close() error { return nil }

func makeContent(lines int) []byte {
	var buf []byte
	for i := 0; i < lines; i++ {
		buf = append(buf, []byte(fmt.Sprintf("https://example.com:user%d@x.com:pass%d\n", i, i))...)
	}
	return buf
}

func TestPipeline_EndToEnd(t *testing.T) {
	tmp := t.TempDir()
	dbPath := tmp + "/state.db"
	outDir := tmp + "/out"

	store, err := state.Open(dbPath)
	require.NoError(t, err)
	require.NoError(t, store.Init(context.Background()))
	defer store.Close()

	const srcID, srcHash = int64(-100), int64(42)
	const dstID, dstHash = int64(-200), int64(99)

	pool := &fakePool{
		t:       t,
		content: map[int64][]byte{
			1001: makeContent(5000),
			1002: makeContent(4000),
			1003: makeContent(3000),
		},
		srcID: srcID, srcHash: srcHash,
		dstID: dstID, dstHash: dstHash,
	}
	for i, fid := range []int64{1001, 1002, 1003} {
		require.NoError(t, store.InsertJobIfAbsent(context.Background(), state.Job{
			MsgID:          int64(i + 1),
			ChatID:         srcID,
			ChatAccessHash: srcHash,
			FileID:         fid,
			AccessHash:     1,
			FileReference:  []byte{1},
			DCID:           2,
			Size:           int64(len(pool.content[fid])),
			Status:         state.StatusPending,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		}))
	}

	cfg := &config.Config{
		SourceChannel: srcID, TargetChannel: dstID,
		Fetcher: config.FetcherConfig{
			Sessions: 2, ChunkSizeBytes: 1024, ChunkChannelCap: 16, JobChannelCap: 4,
			MaxRetriesPerChunk: 3, MaxRetriesPerJob: 3,
		},
		Splitter:  config.SplitterConfig{Workers: 1, LineChannelCap: 1024},
		Processor: config.ProcessorConfig{Workers: 2, RecordChannelCap: 1024},
		Writer: config.WriterConfig{
			OutputDir: outDir, BatchSizeMB: 1, FlushIntervalSec: 1, OutputChannelCap: 8,
		},
		Uploader:     config.UploaderConfig{Sessions: 1, ParallelParts: 2, UploadChannelCap: 4},
		Backpressure: config.BackpressureConfig{MaxPendingOutputFiles: 32},
		Logging:      config.LoggingConfig{Level: "info", Format: "text", ProgressIntervalSec: 60},
	}

	p := pipeline.New(cfg, store, pool, pool, &session.FloodGate{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, p.Run(ctx))

	stats, err := store.Stats(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, 3, stats.Done, "all 3 sources should be done")
	assert.EqualValues(t, 0, stats.Pending+stats.InProgress+stats.Failed)
}
```

- [ ] **Step 2: Run test (expect PASS)**

Run: `go test -race -v -timeout 60s ./tests/integration/...`
Expected: PASS. If failures stem from gotd-version-specific shapes (e.g., `tg.BoolBox` or `tg.MessagesDialogsBox` field names differ), inspect `go doc github.com/gotd/td/tg.MessagesDialogs` and adjust the fake's response wrappers. Possible pinning: `go mod edit -require=github.com/gotd/td@<known-good-version>`.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/
git commit -m "test(integration): end-to-end pipeline against fake session pool"
```

---

### Task 5.3: README + config example + Makefile finalize

**Files:**
- Create: `config.example.yaml`
- Create: `README.md`
- Modify: `Makefile` (add `integration` target)

- [ ] **Step 1: Create `config.example.yaml`**

```yaml
account:
  api_id: 0          # CHANGE ME — get from my.telegram.org
  api_hash: ""       # CHANGE ME
  session_file: sessions/main.session

source_channel: -1001234567890
target_channel: -1009876543210

fetcher:
  sessions: 6
  chunk_size_bytes: 1048576
  chunk_channel_cap: 64
  job_channel_cap: 32
  max_retries_per_chunk: 5
  max_retries_per_job: 3

splitter:
  workers: 0          # 0 = runtime.NumCPU()
  line_channel_cap: 4096

processor:
  workers: 0          # 0 = runtime.NumCPU() * 2
  record_channel_cap: 4096

writer:
  output_dir: ./out
  batch_size_mb: 20
  flush_interval_sec: 30
  output_channel_cap: 16

uploader:
  sessions: 2
  parallel_parts: 4
  upload_channel_cap: 4

backpressure:
  max_pending_output_files: 32

state:
  db_path: ./state.db

logging:
  level: info
  format: json
  progress_interval_sec: 30
```

- [ ] **Step 2: Create `README.md`**

```markdown
# tgpipe — Telegram bulk downloader & republisher

`tgpipe` is a Go pipeline that reads `.txt` files from one Telegram channel,
parses `url:user:pass` lines into `email:pass` records, batches them into
20 MB files, and re-publishes to another Telegram channel.

See `docs/superpowers/specs/2026-05-27-tgpipe-design.md` and `CLAUDE.md` for
architecture & constraints.

## Build

```bash
make build
```

## Configure

Copy `config.example.yaml` to `config.yaml` and set `api_id` / `api_hash`
from <https://my.telegram.org>.

## Run

```bash
# 0. One-time MTProto login — writes session file to account.session_file
./bin/tgpipe auth --config config.yaml

# 1. Build job list from source channel (one-shot)
./bin/tgpipe crawl --config config.yaml

# 2. Start pipeline
./bin/tgpipe run --config config.yaml

# 3. Check progress
./bin/tgpipe stats --config config.yaml
```

Global flags: `--config`, `--log-level` (overrides config), `--debug-pprof`, `--version`.

## Testing

```bash
make test-race        # unit + race detector
make integration      # end-to-end test against fake session pool
```

## Limits

- 1 Premium Telegram account → ~15–25 MB/s sustained throughput.
- Drops the first and last (partial) line of every fetched chunk (~0.01% loss).
- No global dedup across files.
```

- [ ] **Step 3: Update `Makefile`**

```makefile
.PHONY: build test test-race integration vet lint clean

build:
	go build -o bin/tgpipe ./cmd/tgpipe

test:
	go test ./...

test-race:
	go test -race -coverprofile=coverage.out ./...

integration:
	go test -race -timeout 60s ./tests/integration/...

vet:
	go vet ./...

lint: vet
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed"

clean:
	rm -rf bin/ coverage.out tgpipe tgpipe.exe
```

- [ ] **Step 4: Verify all tests + build**

Run:
```bash
make build && make test-race && make integration && make vet
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add config.example.yaml README.md Makefile
git commit -m "docs: README + config.example.yaml; build: Makefile integration target"
```

---

## Done criteria

After completing all chunks, the following should be true:

- [ ] `go build ./...` passes
- [ ] `go test -race ./...` passes (unit + integration)
- [ ] `go vet ./...` passes
- [ ] `./bin/tgpipe --help` shows all 6 subcommands (auth, crawl, run, stats, retry, reset)
- [ ] `./bin/tgpipe stats --config config.example.yaml` runs (with a valid DB)
- [ ] All 8 design constraints in CLAUDE.md §5 are upheld
- [ ] None of the 10 anti-patterns in CLAUDE.md §10 are present

Manual verification (requires real Telegram Premium account):

- [ ] `tgpipe crawl` populates jobs table
- [ ] `tgpipe run` sustains ≥ 10 MB/s end-to-end
- [ ] `kill -9` mid-run → restart resumes within < 60s with no lost jobs
- [ ] Telemetry tick every 30s shows expected throughput + queue depths
- [ ] Output files appear on Channel B with caption `Batch X · N records · M.MM MB`
