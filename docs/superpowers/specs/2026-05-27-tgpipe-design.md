# tgpipe — Telegram Bulk Downloader & Republisher — Design Spec

**Date:** 2026-05-27
**Status:** Draft (pending review)
**Project root:** `D:\vs code\GO - Bulk Downloader & Republisher`
**Parent doc:** `CLAUDE.md` (hiến pháp project)

---

## 0. Quyết định đã chốt khi brainstorm

Toàn bộ 9 open questions trong `CLAUDE.md §11` đã được trả lời:

| # | Câu hỏi | Quyết định |
|---|---|---|
| 1 | Stage 3 logic | Parse `url:user:pass` → extract `email:pass`. **Strict mode**: chỉ giữ line khi phần kế cuối là email hợp lệ. Split bằng cách lấy 2 phần cuối khi tách `:` (handle pass chứa `:` + URL có port). Pass rỗng → drop. Malformed line → drop silent + tăng counter `dropped_invalid_lines`. |
| 2 | Output format | **Plain text**, 1 record `<email>:<pass>` mỗi line. |
| 3 | Dedup global | **Không dedup**. |
| 4 | Mode chạy | **Concurrent only** (fetch + process + upload song song). |
| 5 | Output naming | `out_<unix_ts>_<seq>.txt` (timestamp = batch start, seq reset mỗi giây). |
| 6 | Caption Channel B | **Tối giản**: `Batch <seq> · <line_count> records · <bytes_human> MB`. |
| 7 | Audit log | **Không** audit line-level. DB chỉ lưu `output_path` đại diện (last output file). |
| 8 | Session ratio | **Config-driven**, default fetcher=6 / uploader=2 trên 1 Premium account. |
| 9 | Channel B precheck | **Có**, gọi `messages.getFullChannel` startup để verify post rights, fail-fast nếu thiếu quyền. |

### Approach decisions
- **Session sub-pool**: tái sử dụng pattern `iyear/tdl` `pool.Pool` (battle-tested, không reinvent).
- **Backpressure**: kết hợp `chan OutputFile` cap (=16) + disk gauge (`max_pending_output_files`).
- **Processor**: interface `LineProcessor` + implementation `UrlUserPassExtractor` (plug-in friendly).

---

## 1. Architecture overview

### 1.1 Pipeline topology

```
[Channel A] ─► (0) Crawler ─► SQLite jobs
                                  │
                                  ▼
                         (1) Fetcher   ──┐ chan Chunk(64)
                                         ▼
                         (2) Splitter  ──┐ chan []byte(4096)
                                         ▼
                         (3) Processor ──┐ chan Record(4096)  ← url:user:pass → email:pass
                                         ▼
                         (4) Writer    ──┐ chan OutputFile(16) ← plain text, batch 20MB
                                         ▼
                         (5) Uploader  ──► [Channel B]
                                         ▼
                                       SQLite (mark done)
```

### 1.2 Process model
- 1 OS process `tgpipe`, 2 entrypoint chính: `crawl` (one-shot) và `run` (pipeline).
- 1 Go runtime, mọi stage là goroutine pool trong cùng process. Không IPC.
- Orchestration: `errgroup.WithContext`, fatal error 1 stage = cancel toàn pipeline.
- Graceful shutdown: `signal.NotifyContext(SIGINT/SIGTERM)`.

### 1.3 Session sub-pool layout (1 Premium account)

```
sessions/main.session  (auth_key file)
            │ load by both pools
            ▼
┌──────────────────────────┐   ┌──────────────────────────┐
│  Fetch sub-pool          │   │  Upload sub-pool         │
│  6 × telegram.Client     │   │  2 × telegram.Client     │
│  ↓                       │   │  ↓                       │
│  upload.getFile          │   │  upload.saveFilePart     │
│  (parallel 1MB chunks)   │   │  + messages.sendMedia    │
└──────────────────────────┘   └──────────────────────────┘
        ↕  FLOOD_WAIT shared FloodGate (account-wide pause)  ↕
```

Mỗi sub-pool tạo N `telegram.Client` riêng cùng load 1 `.session` file (share auth_key, MTProto session riêng). KHÔNG share client instance giữa 2 vai trò (race + FLOOD_WAIT lan).

### 1.4 Module boundaries

```
cmd/tgpipe/main.go         → cobra CLI: crawl|run|stats|retry|reset
internal/config            → load + validate YAML, no business logic
internal/state             → SQLite ops (CRUD jobs), single-statement updates
internal/session           → MTProto auth, sub-pool factory + FloodGate
internal/fetcher           → Stage 1, parallel chunk + FILE_REFERENCE refresh
internal/splitter          → Stage 2, pure func (highly testable)
internal/processor         → Stage 3, interface + UrlUserPassExtractor impl
internal/writer            → Stage 4, batch by size + flush_interval
internal/uploader          → Stage 5, parallel parts + sendMedia
internal/pipeline          → orchestrator (errgroup + channels)
internal/telemetry         → counters, progress logger (30s)
internal/tracker           → SourceTracker (per-source completion accounting)
internal/retry             → exponential backoff helper
internal/logging           → slog setup
```

---

## 2. Module breakdown (package contracts)

### `internal/config`
```go
type Config struct {
    Account       AccountConfig
    SourceChannel int64
    TargetChannel int64
    Fetcher       FetcherConfig
    Splitter      SplitterConfig
    Processor     ProcessorConfig
    Writer        WriterConfig
    Uploader      UploaderConfig
    Backpressure  BackpressureConfig
    State         StateConfig
    Logging       LoggingConfig
}

func Load(path string) (*Config, error)
func (c *Config) Validate() error
```

### `internal/state`
```go
type Store interface {
    Init(ctx context.Context) error                  // migrations + PRAGMAs + reset in_progress→pending
    InsertJob(ctx context.Context, j Job) error
    PickPending(ctx context.Context, n int) ([]Job, error)
    MarkDone(ctx context.Context, msgID int64, outputPath string) error
    MarkFailed(ctx context.Context, msgID int64, errMsg string) error
    UpdateFileReference(ctx context.Context, msgID int64, ref []byte) error
    Stats(ctx context.Context) (Stats, error)
    Close() error
}
```

### `internal/session`
```go
type Pool interface {
    Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error
    Size() int
    Close() error
}
func NewFetchPool(ctx context.Context, cfg AccountConfig, size int, gate *FloodGate) (Pool, error)
func NewUploadPool(ctx context.Context, cfg AccountConfig, size int, gate *FloodGate) (Pool, error)

type FloodGate struct{ ... }
func (g *FloodGate) Wait(ctx context.Context) error
func (g *FloodGate) Trigger(d time.Duration)
```

### `internal/fetcher` — Stage 1
```go
type Fetcher struct{ ... }
func New(pool session.Pool, store state.Store, tracker *tracker.SourceTracker, cfg FetcherConfig) *Fetcher
func (f *Fetcher) Run(ctx context.Context, jobs <-chan state.Job, out chan<- Chunk) error
```

### `internal/splitter` — Stage 2 (pure func + stage)
```go
func SplitDropEdges(chunk []byte) (lines [][]byte, droppedHead, droppedTail int)

type Splitter struct{ workers int }
func (s *Splitter) Run(ctx context.Context, in <-chan Chunk, out chan<- Line, tracker *tracker.SourceTracker) error
```

### `internal/processor` — Stage 3
```go
type LineProcessor interface {
    Process(line []byte) (Record, bool, error)
}

type UrlUserPassExtractor struct{}

type Processor struct{ workers int; impl LineProcessor; metrics telemetry.Recorder }
func (p *Processor) Run(ctx context.Context, in <-chan Line, out chan<- Record) error
```

### `internal/writer` — Stage 4
```go
type Writer struct{ ... }
func New(cfg WriterConfig, bp BackpressureGate, tracker *tracker.SourceTracker) *Writer
func (w *Writer) Run(ctx context.Context, in <-chan Record, out chan<- OutputFile) error
```

### `internal/uploader` — Stage 5
```go
type Uploader struct{ ... }
func New(pool session.Pool, store state.Store, tracker *tracker.SourceTracker, cfg UploaderConfig) *Uploader
func (u *Uploader) Run(ctx context.Context, in <-chan OutputFile) error
```

### `internal/pipeline` — Orchestrator
```go
type Pipeline struct{ ... }
func New(cfg *Config, store state.Store, fetchPool, uploadPool session.Pool, tracker *tracker.SourceTracker) *Pipeline
func (p *Pipeline) Run(ctx context.Context) error
```

### `internal/telemetry`
```go
type Counters struct {
    DownloadBytes, UploadBytes        atomic.Int64
    LinesEmitted, DroppedInvalidLines atomic.Int64
    DroppedEdgeBytes                  atomic.Int64
    FloodWaits, Retries               atomic.Int64
    FileRefExpiredHits                atomic.Int64
    JobsDone, JobsFailed              atomic.Int64
}

type Logger struct{ ... }
func (l *Logger) Run(ctx context.Context) error
```

### `internal/tracker`
```go
type SourceTracker struct{ ... }
func New(store state.Store) *SourceTracker
func (t *SourceTracker) Register(msgID int64, totalChunks int)
func (t *SourceTracker) ChunkConsumed(msgID int64)
func (t *SourceTracker) OutputFlushed(srcIDs []int64, outputPath string)
func (t *SourceTracker) OutputUploaded(srcIDs []int64, outputPath string) error
```

---

## 3. Data model + types

### 3.1 Channel types

```go
type Chunk struct {
    MsgID  int64
    Seq    int
    Data   []byte
    IsLast bool
}

// Line — output Stage 2 (Splitter). Wrapper struct propagate MsgID xuống Processor → Writer
// cho phép Writer track contributing sources. Overhead 8 bytes/line ≈ 8% (line trung bình ~100B),
// nằm trong memory budget.
type Line struct {
    MsgID int64
    Data  []byte   // zero-copy slice từ chunk buffer
}

type Record struct {
    MsgID int64
    Email []byte
    Pass  []byte
}

type OutputFile struct {
    Path         string
    LineCount    int
    SizeBytes    int64
    BatchSeq     int
    SourceMsgIDs []int64   // dedup'd list contributing sources
}
```

### 3.2 Channel capacities (config-driven, không hardcode)

| Channel | Config key | Default |
|---|---|---|
| `chan Job` (DB → Fetcher) | `fetcher.job_channel_cap` | 32 |
| `chan Chunk` (Fetcher → Splitter) | `fetcher.chunk_channel_cap` | 64 |
| `chan Line` (Splitter → Processor) | `splitter.line_channel_cap` | 4096 |
| `chan Record` (Processor → Writer) | `processor.record_channel_cap` | 4096 |
| `chan OutputFile` (Writer → Uploader) | `writer.output_channel_cap` | 16 |

### 3.3 SQLite schema (theo CLAUDE.md §6, không thay đổi)

```sql
CREATE TABLE IF NOT EXISTS jobs (
    msg_id          INTEGER PRIMARY KEY,
    chat_id         INTEGER NOT NULL,
    file_id         INTEGER NOT NULL,
    access_hash     INTEGER NOT NULL,
    file_reference  BLOB    NOT NULL,
    dc_id           INTEGER NOT NULL,
    size            INTEGER NOT NULL,
    file_name       TEXT,
    mime_type       TEXT,
    status          TEXT    NOT NULL DEFAULT 'pending',
    retries         INTEGER NOT NULL DEFAULT 0,
    output_path     TEXT,
    error_msg       TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
CREATE INDEX idx_status  ON jobs(status);
CREATE INDEX idx_updated ON jobs(updated_at);
```

PRAGMAs: `WAL`, `synchronous=NORMAL`, `cache_size=-64000`, `temp_store=MEMORY`, `busy_timeout=5000`.

### 3.4 Memory budget (rough)

| Item | Bytes |
|---|---|
| Chunk channel (64 × 1MB) | 64 MB |
| Line channel (4096 × ~100B) | 400 KB |
| Record channel (4096 × ~80B) | 320 KB |
| Writer buffer | 20 MB |
| Upload parallel parts (4 × 1MB × 2 workers) | 8 MB |
| SQLite cache | 64 MB |
| Goroutine stacks (~800 × 8KB) | 6 MB |
| **Peak total** | **~165 MB** |

Cách rất xa budget 1.5 GB.

---

## 4. Error handling + recovery

### 4.1 Per-source completion tracking — `SourceTracker`

```go
type srcState struct {
    totalChunks        int
    chunksConsumed     int
    outputFilesPending int
    lastOutputPath     string
}
```

Event flow:
```
Fetcher.Dispatch      ─► tracker.Register(msgID, totalChunks)
Splitter consume      ─► tracker.ChunkConsumed(msgID)
Writer.Flush          ─► tracker.OutputFlushed(srcIDs[], path)
Uploader.UploadOK     ─► tracker.OutputUploaded(srcIDs[], path)
                          → if chunksConsumed==totalChunks && outputFilesPending==0:
                              store.MarkDone(msgID, lastOutputPath)
                              delete from map
```

**Note on `srcIDs[]` computation in Writer**: Writer giữ `map[int64]struct{}` trong buffer hiện tại. Mỗi Record append vào buffer → insert `Record.MsgID` vào map. Khi flush → convert map keys thành slice → reset map cho batch tiếp theo. Đảm bảo `OutputFile.SourceMsgIDs` dedup'd và đầy đủ.

In-memory only. Crash → tracker mất → resume reset `in_progress → pending` → redo file (CLAUDE.md constraint #6).

### 4.2 Error categories

| Loại | Detect | Handler | Counter tăng? |
|---|---|---|---|
| Network timeout / temporary RPC | `errors.Is(err, context.DeadlineExceeded)` hoặc gotd marker | Exponential backoff 500ms → 8s, max 5 lần | Yes (per-chunk) |
| `FLOOD_WAIT_X` | `tgerr.Is(err, ...)` parse X | `Sleep(X+1s)` + `FloodGate.Trigger(X+1s)` cho cả 2 pool | No |
| `FILE_REFERENCE_EXPIRED` | tgerr | Re-fetch message → `UpdateFileReference` → retry chunk | No |
| `ACCESS_HASH_INVALID`, `MEDIA_EMPTY` | tgerr | `MarkFailed`, skip job | Terminal |
| `AUTH_KEY_DUPLICATED`, `AUTH_KEY_INVALID`, `SESSION_REVOKED` | gotd auth | Fatal: log + cancel ctx, exit | — |
| DB write fail | `sqlite.Error` | `retry.WithBackoff` 3 attempts (500ms → 2s) → fatal | Internal |
| Disk full | `syscall.ENOSPC` | Fatal | — |
| `context.Canceled` | errors.Is | Clean exit, no error log | — |

### 4.3 Retry helper

```go
func WithBackoff(ctx context.Context, maxAttempts int, op func() error) error
// Exponential 500ms → 8s cap. isRetryable() whitelist Telegram codes + net errors.
```

### 4.4 FLOOD_WAIT account-wide pause — `FloodGate`

```go
type FloodGate struct{ until atomic.Int64 }  // unix nano
func (g *FloodGate) Wait(ctx context.Context) error
func (g *FloodGate) Trigger(d time.Duration)   // CAS-extend
```

Inject vào cả Fetch + Upload pool. Mỗi `Invoke` call gọi `gate.Wait(ctx)` trước RPC.

### 4.5 Crash recovery (startup)

```
1. signal.NotifyContext(SIGINT/SIGTERM)
2. config.Load() → fail-fast
3. state.NewStore(dbPath)
   ├─ run migrations
   ├─ apply PRAGMAs
   └─ UPDATE jobs SET status='pending' WHERE status='in_progress'
4. session.NewFetchPool + NewUploadPool (cùng .session file)
5. precheck Channel B (messages.getFullChannel + verify post rights) → fail-fast
6. pipeline.New(...).Run(ctx)
```

Target recovery time: **< 10s**.

### 4.6 Graceful shutdown

Signal → ctx canceled → mỗi stage drain in-flight:
- Fetcher: dừng dispatch mới, đợi in-flight, close out chan
- Splitter/Processor: drain input, close out chan
- Writer: **flush buffer hiện tại** ra file (giữ data), close out chan
- Uploader: hoàn tất file đang upload (atomic, không bỏ giữa chừng)

Hard timeout: 60s. Quá → `log.Error + os.Exit(1)`.

### 4.7 Retry counter (DB)

- Tăng khi job fail sau exhausted retries trong stage.
- `retries >= max_retries_per_job` (default **3**) → `MarkFailed`.
- KHÔNG tăng khi FLOOD_WAIT.

---

## 5. Telemetry + observability

### 5.1 Counters (atomic)

```go
type Counters struct {
    DownloadBytes, UploadBytes        atomic.Int64
    LinesEmitted, DroppedInvalidLines atomic.Int64
    DroppedEdgeBytes                  atomic.Int64
    FloodWaits, Retries               atomic.Int64
    FileRefExpiredHits                atomic.Int64
    JobsDone, JobsFailed              atomic.Int64
}
```

### 5.2 Channel depth gauges

```go
type Gauges struct {
    ChunkChan, LineChan, RecordChan, OutputChan func() (used, cap int)
}
```

### 5.3 Progress logger (30s tick, configurable)

```
level=INFO msg=progress stage=telemetry
  download_mbps=18.4 upload_mbps=16.1
  chunk_q=42/64 line_q=2103/4096 record_q=89/4096 output_q=4/16
  jobs_done=142 jobs_inprog=8 jobs_pending=350 jobs_failed=2
  floods_delta=1 retries_delta=4 dropped_lines_delta=12
```

### 5.4 Slog setup

- Format `json` hoặc `text` theo config.
- Field bắt buộc: `stage`. Optional: `msg_id`, `attempt`.
- Levels: `debug` (chunk/line — off mặc định), `info` (lifecycle + progress), `warn` (retry, flood), `error` (fatal, job failed).

### 5.5 CLI `tgpipe stats`

Single SQL query `GROUP BY status` → print bảng.

### 5.6 Optional pprof

`--debug-pprof` → listen `127.0.0.1:6060`. Default off.

---

## 6. Testing strategy

### 6.1 Unit (trọng tâm)

| Package | Coverage target | Note |
|---|---|---|
| `internal/splitter` | 100% | Pure func, fixture-driven |
| `internal/processor` | 100% | Pure func, table-driven cases (URL/port/CRLF/empty pass/non-email) |
| `internal/state` | ≥ 90% | In-memory SQLite `:memory:`, test atomic claim |
| `internal/retry` | 100% | Mock op, fake clock |
| `internal/session/floodgate` | 100% | CAS, concurrent triggers |
| `internal/tracker` | ≥ 90% | Race detector required |

### 6.2 Stage-level (mock pool/store)

- `fetcher`: parallel chunk dispatch, FILE_REFERENCE refresh, FLOOD_WAIT propagation
- `writer`: flush on size/interval, SourceMsgIDs tracking, backpressure block
- `uploader`: happy path, tracker notify on success, file kept on failure

### 6.3 Integration

`tests/integration/pipeline_test.go` — full pipeline với fake `session.Pool` serve N in-memory `.txt` files. Verify:
- Tất cả lines hợp lệ xuất hiện trong output files
- DB: jobs → `done`
- Edge line loss ≈ expected ~0.01%

### 6.4 Race detector

`go test -race ./...` mọi commit. Race detector là phòng tuyến chính cho shared state.

### 6.5 Load test (manual)

10GB+ test channel → measure throughput, RAM, goroutines, FLOOD_WAIT rate → verify `kill -9` resume < 60s.

### 6.6 CI matrix (GitHub Actions đề xuất)

```yaml
- go vet ./...
- staticcheck ./...
- go test -race -coverprofile=cover.out ./...
- go test -bench=. -benchmem ./internal/splitter ./internal/processor
```

---

## 7. CLI design (theo CLAUDE.md §13)

```bash
tgpipe crawl --config config.yaml          # one-shot, build job list
tgpipe run   --config config.yaml          # main pipeline
tgpipe stats --config config.yaml          # read-only summary
tgpipe retry --config config.yaml --status failed
tgpipe reset --config config.yaml --msg-ids 12345,12346
```

Optional global flags: `--debug-pprof`, `--log-level=debug`.

---

## 8. Config additions (mở rộng CLAUDE.md §8)

Thêm vào YAML default:
```yaml
fetcher:
  job_channel_cap: 32          # NEW: buffer DB → Fetcher
  max_retries_per_job: 3       # NEW: job-level retry trước khi mark failed (khác max_retries_per_chunk=5)

processor:
  # type: "url_user_pass"      # NEW reserved: cho future plug-in processor khác
```

---

## 9. Out of scope (explicit non-goals)

- Multi-account fan-out (target throughput 50+ MB/s) — đã loại theo CLAUDE.md.
- Phased mode (download xong hết rồi upload) — đã loại (open question #9 chọn concurrent-only).
- Global dedup — đã loại.
- Audit log line-level — đã loại.
- Stage 3 chấp nhận format khác `url:user:pass` — interface có sẵn cho future, nhưng chỉ ship `UrlUserPassExtractor`.

---

## 10. Open items (sau brainstorm — đã đóng nhưng đáng note)

- Tỉ lệ session 6/2 chỉ là default. Sau load test có thể chỉnh `fetcher.sessions` / `uploader.sessions` mà không cần build lại.
- `max_pending_output_files=32` mặc định — có thể tinh chỉnh theo disk size VPS thực tế.

---

## 11. Roadmap (theo CLAUDE.md §12, không thay đổi)

- **Phase 1 (1–2d):** config, slog, state, session pool, crawler
- **Phase 2 (2–3d):** fetcher + splitter + temp sink, throughput đo
- **Phase 3 (1–2d):** writer + uploader + DB done transition
- **Phase 4 (1–2d):** backpressure, telemetry, graceful shutdown, crash test
- **Phase 5:** Stage 3 polish (đã có pattern), CLI subcommands, README/deploy

---

## 12. References

- `CLAUDE.md` (project root) — hiến pháp
- `iyear/tdl` → `pkg/tclient`, `app/dl` — reference impl
- `gotd/td` docs — https://pkg.go.dev/github.com/gotd/td
- Telegram files API — https://core.telegram.org/api/files
- FILE_REFERENCE handling — https://core.telegram.org/api/file_reference
