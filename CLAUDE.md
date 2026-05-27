# Telegram Bulk Downloader & Republisher

> **Mục đích của file này:** Định nghĩa context, kiến trúc, ràng buộc và quy ước của project. Đây là "hiến pháp" — mọi code sinh ra phải tuân theo. Đọc kỹ trước khi viết bất kỳ code nào.

---

## 1. Mục tiêu project

Pipeline Go đọc toàn bộ file `.txt` từ một Telegram channel (**Channel A**), bóc tách dữ liệu theo từng line, gom thành các file output, rồi upload lên một Telegram channel khác (**Channel B**) dưới dạng document.

**Throughput target:** 15–25 MB/s sustained end-to-end (download + process + upload).

**Hạ tầng giả định:** VPS Singapore/EU, NVMe SSD, ≥1 Gbps network, **1 Telegram Premium account** dùng cho cả fetch và upload.

**Hệ quả quan trọng của single-account:** Account vừa fetch (Channel A) vừa upload (Channel B) → phải **chia session pool tách biệt** cho 2 vai trò (ví dụ: 6 sessions cho fetch, 2 sessions cho upload). Throughput mỗi chiều bị giới hạn bởi bandwidth share của account và rate limits tổng. **KHÔNG** đạt được 50+ MB/s như multi-account setup; bù lại đơn giản hơn về auth/session management.

---

## 2. Kiến trúc — 5 stage pipeline

```
┌──────────────────────────────────────────────────────────────────┐
│  Channel A (.txt files in Telegram)                              │
└──────────────────────────────────────────────────────────────────┘
                                ↓
┌──────────────────────────────────────────────────────────────────┐
│  Stage 0 — CRAWLER (chạy 1 lần để build job list)                │
│  • iter messages có document/.txt                                │
│  • insert vào SQLite jobs table                                  │
└──────────────────────────────────────────────────────────────────┘
                                ↓
┌──────────────────────────────────────────────────────────────────┐
│  Stage 1 — PARALLEL FETCHER (Model C, unordered)                 │
│  • Fetch session subpool (mặc định 6 sessions) trên 1 account    │
│  • Parallel chunks 1MB qua upload.getFile                        │
│  • out: chan Chunk (cap=64)                                      │
└──────────────────────────────────────────────────────────────────┘
                                ↓
┌──────────────────────────────────────────────────────────────────┐
│  Stage 2 — LINE SPLITTER (stateless, parallel workers)           │
│  • find first \n (p1) + last \n (p2)                             │
│  • DROP bytes[0..p1] và bytes[p2+1..]  ← edge lines mất ~0.01%   │
│  • emit bytes[p1+1..p2] split by \n                              │
│  • out: chan []byte (cap=4096)                                   │
└──────────────────────────────────────────────────────────────────┘
                                ↓
┌──────────────────────────────────────────────────────────────────┐
│  Stage 3 — LINE PROCESSOR (parallel workers, logic TBD)          │
│  • parse / filter / extract — chi tiết sẽ định nghĩa sau         │
│  • out: chan Record (cap=4096)                                   │
└──────────────────────────────────────────────────────────────────┘
                                ↓
┌──────────────────────────────────────────────────────────────────┐
│  Stage 4 — BATCH WRITER                                          │
│  • accumulate records vào buffer                                 │
│  • flush ra file out_<batch>.txt khi đạt size threshold          │
│  • out: chan string (path file output) (cap=16)                  │
└──────────────────────────────────────────────────────────────────┘
                                ↓
┌──────────────────────────────────────────────────────────────────┐
│  Stage 5 — PARALLEL UPLOADER → Channel B                         │
│  • Upload session subpool (mặc định 2 sessions) trên cùng account│
│  • upload.saveFilePart song song theo chunks                     │
│  • messages.sendMedia với document                               │
│  • on success: update DB status='done'                           │
└──────────────────────────────────────────────────────────────────┘
                                ↓
┌──────────────────────────────────────────────────────────────────┐
│  Channel B (.txt files đã xử lý)                                 │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. Tech stack

| Mục | Lựa chọn | Lý do |
|---|---|---|
| MTProto client | `github.com/gotd/td` | Mature nhất trong Go, đã có pattern parallel chunk |
| State store | `modernc.org/sqlite` | Pure Go (no CGO), WAL mode đủ nhanh |
| Logging | `log/slog` (stdlib) | Structured, sẵn trong Go 1.21+ |
| Config | YAML + `gopkg.in/yaml.v3` | Đơn giản, đủ dùng |
| CLI | `github.com/spf13/cobra` | Standard Go CLI lib |
| Errors | `errors.Join`, `%w` wrapping | Stdlib đủ |
| Async runtime | Goroutines + `golang.org/x/sync/errgroup` | Native |

**Reference đọc bắt buộc trước khi code:** [`iyear/tdl`](https://github.com/iyear/tdl) — đặc biệt phần `pkg/tclient` và `app/dl`. Pattern parallel chunk + session pool đã có sẵn, **học và tái sử dụng**, đừng phát minh lại.

**Go version:** ≥1.22 (cho `for range int` và `slices` package).

---

## 4. Cấu trúc thư mục

```
.
├── CLAUDE.md                  # File này
├── README.md                  # User-facing docs
├── go.mod
├── go.sum
├── Makefile
├── config.example.yaml
├── cmd/
│   └── tgpipe/
│       └── main.go            # CLI entry
├── internal/
│   ├── config/                # Load + validate config
│   ├── state/                 # SQLite ops (jobs CRUD)
│   ├── session/               # MTProto auth + session pool
│   ├── fetcher/               # Stage 1
│   ├── splitter/              # Stage 2 (pure function, dễ test)
│   ├── processor/             # Stage 3 (interface — plug logic later)
│   ├── writer/                # Stage 4
│   ├── uploader/              # Stage 5
│   ├── pipeline/              # Orchestrator (errgroup, channels)
│   └── telemetry/             # Progress logger, metrics
├── migrations/
│   └── 001_init.sql
└── sessions/                  # MTProto .session files (gitignored)
```

---

## 5. Design constraints — KHÔNG được vi phạm

1. **Model C, KHÔNG stitcher.** Edge lines bị drop. Đã có quyết định, không quay lại.
2. **Bounded channels everywhere.** Không có unbuffered hoặc unbounded channel giữa các stage. Backpressure là bắt buộc.
3. **1 account, 2 session subpool tách biệt.** Cùng `.session` file/auth_key, nhưng tạo 2 nhóm MTProto session riêng — 1 cho fetch, 1 cho upload. KHÔNG dùng chung session instance cho cả 2 vai trò (sẽ race + FLOOD_WAIT lan).
4. **FILE_REFERENCE_EXPIRED phải refresh inline.** Khi gặp lỗi này, re-fetch message để lấy `file_reference` mới và retry chunk đó. KHÔNG restart cả file.
5. **FLOOD_WAIT là tín hiệu toàn account.** Với 1 account, FLOOD_WAIT trên 1 session thường ngụ ý cả account đang bị throttle → tạm dừng các session khác cùng account vài giây (không phá vỡ pipeline, chỉ giảm aggression).
6. **Status `in_progress → done` chỉ commit SAU KHI upload Channel B xong.** Đây là idempotency boundary. Crash giữa chừng → restart redo cả file (an toàn).
7. **Stream upload, đừng `io.ReadAll`.** File output có thể vài chục MB, đừng load hết vào RAM. Dùng `*os.File` làm `io.Reader`.
8. **Mỗi stage là 1 goroutine pool độc lập** với input/output channel. Không gọi trực tiếp giữa stages.

---

## 6. State management

### Schema SQLite (`migrations/001_init.sql`)

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
                    -- pending | in_progress | done | failed
    retries         INTEGER NOT NULL DEFAULT 0,
    output_path     TEXT,
    error_msg       TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE INDEX idx_status ON jobs(status);
CREATE INDEX idx_updated ON jobs(updated_at);
```

### PRAGMAs

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -64000;     -- 64 MB page cache
PRAGMA temp_store = MEMORY;
PRAGMA busy_timeout = 5000;
```

### Resume on crash

```sql
-- Chạy 1 lần khi startup, trước khi pipeline bắt đầu
UPDATE jobs SET status = 'pending' WHERE status = 'in_progress';
```

Pipeline pick job theo:
```sql
SELECT * FROM jobs WHERE status = 'pending' ORDER BY msg_id LIMIT ?;
```

### Status transition

```
pending ──pick──► in_progress ──upload OK──► done
                       │
                       └──retries >= max──► failed
```

Mọi update phải single statement, không transaction phức tạp (tránh contention).

---

## 7. Performance targets

| Metric | Target | Acceptable |
|---|---|---|
| Throughput end-to-end | 20–25 MB/s | ≥ 10 MB/s |
| Throughput download burst (khi upload idle) | 30–40 MB/s | ≥ 20 MB/s |
| RAM peak | < 1.5 GB | < 3 GB |
| Goroutines steady-state | < 800 | < 3000 |
| Edge line loss | ~0.01% | < 0.1% |
| Recovery time sau crash | < 10s | < 60s |
| SQLite write/sec | < 50 | < 500 |

---

## 8. Cấu hình (YAML)

```yaml
account:
  api_id: 1234567
  api_hash: "abc..."
  session_file: sessions/main.session
  # Premium account — bandwidth/file-size limit gấp đôi free

source_channel: -1001234567890
target_channel: -1009876543210

fetcher:
  sessions: 6                  # session subpool cho fetch
  chunk_size_bytes: 1048576    # 1 MB (max Telegram)
  chunk_channel_cap: 64
  max_retries_per_chunk: 5

splitter:
  workers: 0                   # 0 = runtime.NumCPU()
  line_channel_cap: 4096

processor:
  workers: 0                   # 0 = runtime.NumCPU() * 2
  record_channel_cap: 4096

writer:
  output_dir: ./out
  batch_size_mb: 20
  flush_interval_sec: 30
  output_channel_cap: 16

uploader:
  sessions: 2                  # session subpool cho upload
  parallel_parts: 4
  upload_channel_cap: 4

# Flow control: khi upload backlog quá lớn, throttle fetch để
# tránh disk phình. 0 = không giới hạn.
backpressure:
  max_pending_output_files: 32  # nếu out/ có > 32 file chờ upload, fetch tạm dừng

state:
  db_path: ./state.db

logging:
  level: info                  # debug | info | warn | error
  format: json                 # json | text
  progress_interval_sec: 30
```

**Tinh chỉnh tỉ lệ session 6/2:** Nếu Channel B có file lớn (Premium tận dụng 4GB limit), tăng `uploader.sessions` lên 3–4 và giảm `fetcher.sessions` tương ứng. Tổng số session đồng thời trên 1 account nên giữ ≤ 8–10 để tránh FLOOD_WAIT thường xuyên.

---

## 9. Conventions code

### Concurrency

- Mỗi component implement: `Run(ctx context.Context) error`
- Pipeline orchestrator dùng `errgroup.WithContext`
- Cancel toàn pipeline khi 1 stage trả error fatal
- Graceful shutdown qua `signal.NotifyContext` (SIGINT, SIGTERM)

### Error handling

| Loại lỗi | Hành xử |
|---|---|
| Network timeout, temporary | Retry với exponential backoff trong stage |
| `FLOOD_WAIT_X` | `time.Sleep(X+1)` rồi retry, không tăng retry counter |
| `FILE_REFERENCE_EXPIRED` | Refresh message → update DB → retry chunk |
| `ACCESS_HASH_INVALID`, `MEDIA_EMPTY` | Mark file `failed`, skip |
| `AUTH_KEY_DUPLICATED` | Fatal, dừng pipeline (cấu hình sai) |
| `AUTH_KEY_INVALID` | Fatal, cần re-auth manual |
| Init DB / config sai | Fatal, exit early |

### Logging

- Structured `slog.LogAttrs`
- Bắt buộc field: `stage`, `msg_id` (khi áp dụng), `account` (khi áp dụng)
- Mức `debug`: từng chunk, từng line
- Mức `info`: progress 30s/lần, file done/failed
- Mức `warn`: retry, FLOOD_WAIT
- Mức `error`: fatal, file failed

### Progress logger (mỗi 30s)

```
[telemetry] download=78.4 MB/s upload=72.1 MB/s
           queue: chunk=58/64 line=3801/4096 record=210/4096
           jobs: done=142 in_progress=8 pending=350 failed=2
           floods/min=3 retries/min=11
```

### Naming

- File output: `out_<unix_ts>_<seq>.txt` (ví dụ `out_1735603200_0001.txt`)
- Có thể đính kèm caption khi upload (TBD theo open question #5)

### Testing

- **Unit:** splitter là pure function — test với fixture bytes
- **Unit:** state package — test với in-memory SQLite (`file::memory:`)
- **Integration:** end-to-end với mini Telegram channel test (vài MB)
- **Load:** chạy thực 1 channel ~10 GB rồi đo

---

## 10. Anti-patterns — KHÔNG làm

| ❌ Không làm | Lý do |
|---|---|
| Stitch edge lines | Đã quyết drop, đừng quay lại |
| Unbuffered/unbounded channel giữa stages | Deadlock dưới load |
| Ghi DB sau mỗi chunk | SQLite contention |
| `io.ReadAll` file output trước upload | Phình RAM |
| Share session instance giữa fetch & upload | Race + FLOOD_WAIT lan. Tách 2 subpool. |
| Chia chunk theo line count | Misalign với MTProto |
| Retry vô hạn | Hang khi data corrupt |
| Goroutine per chunk không giới hạn | Spawn loạn, crash |
| Đặt `api_hash` trực tiếp trong code | Phải qua config |
| Block trên `ctx.Done()` không nhả channel | Leak goroutine |

---

## 11. Open questions — cần Manh chốt trước khi code chi tiết

- [ ] **Stage 3 logic:** regex pattern? Cấu trúc record output? Filter rules? Dedup yêu cầu?
- [ ] **Output file naming:** theo timestamp, batch seq, hay theo source msg_id?
- [ ] **Caption khi upload Channel B:** có cần meta (số line, source msg_id, batch id)?
- [ ] **Audit log:** có cần track mapping `source_msg → record → output_file` không?
- [ ] **Dedup global** giữa các file: cần không? Nếu có → Bloom filter / Redis Set ở Stage 3.
- [ ] **Channel B đã tồn tại?** Account đã có quyền post chưa?
- [ ] **Định dạng output:** giữ nguyên format input (1 record/line), hay structured (JSONL/CSV)?
- [ ] **Session ratio 6/2 (fetch/upload)** có hợp lý không, hay ưu tiên 1 chiều? (Có thể tune sau khi đo thực tế.)
- [ ] **Mode chạy:** concurrent (fetch + upload song song) là default. Có cần thêm **phased mode** (download xong hết rồi mới upload) cho trường hợp disk rộng + muốn tối đa download speed?

---

## 12. Roadmap implementation

### Phase 1 — Foundation (1–2 ngày)

- [ ] Setup module, config loader, slog
- [ ] SQLite migrations + state package
- [ ] Session pool với gotd/td (1 account trước)
- [ ] Crawler: lấy được danh sách file từ Channel A → ghi DB

### Phase 2 — Download path (2–3 ngày)

- [ ] Fetcher: parallel chunks 1 account
- [ ] Splitter: drop edges, emit lines
- [ ] Sink tạm: ghi lines ra stdout / file để verify
- [ ] Đo throughput, fix bottleneck

### Phase 3 — Upload path (1–2 ngày)

- [ ] Writer: batch records → file
- [ ] Uploader: parallel parts → Channel B
- [ ] DB transition `in_progress → done`

### Phase 4 — Backpressure + telemetry (1–2 ngày)

- [ ] Backpressure giữa writer ↔ uploader (pause fetch khi `out/` đầy)
- [ ] Dynamic session ratio (optional): auto-rebalance fetch/upload sessions nếu queue lệch
- [ ] Telemetry: progress logger, FLOOD_WAIT counter, latency histogram
- [ ] Graceful shutdown
- [ ] Resume on crash test (kill -9)

### Phase 5 — Polish

- [ ] Stage 3 processor logic (sau khi Manh chốt)
- [ ] CLI subcommands: `crawl`, `run`, `stats`, `retry-failed`
- [ ] README + deploy script

---

## 13. CLI dự kiến

```bash
# 1. Crawl metadata (1 lần)
tgpipe crawl --config config.yaml

# 2. Chạy pipeline
tgpipe run --config config.yaml

# 3. Xem tổng quan từ DB
tgpipe stats --config config.yaml
# Output:
#   pending:      350
#   in_progress:  8
#   done:         142
#   failed:       2
#   total size:   18.4 GB
#   completed:    71% (13.1 GB / 18.4 GB)

# 4. Retry các file failed (sau khi điều tra)
tgpipe retry --config config.yaml --status failed

# 5. Reset job về pending (manual recovery)
tgpipe reset --config config.yaml --msg-ids 12345,12346
```

---

## 14. References

- gotd/td: https://github.com/gotd/td
- gotd/td docs: https://pkg.go.dev/github.com/gotd/td
- tdl (reference impl): https://github.com/iyear/tdl
- Telegram MTProto: https://core.telegram.org/mtproto
- Telegram files API: https://core.telegram.org/api/files
- FILE_REFERENCE handling: https://core.telegram.org/api/file_reference

---

**Tóm gọn philosophy:** 1 Premium account chia 2 session subpool (fetch + upload) chạy concurrent. Đơn giản hóa logic bằng cách drop edge lines. Predictable memory qua bounded channels + backpressure giữa writer/uploader. Idempotent qua "commit only after Channel B upload OK". Resume free qua single SQL update lúc startup. Đánh đổi tốc độ tổng (15–25 MB/s thay vì 50+) lấy đơn giản về auth/session/rate-limit management.