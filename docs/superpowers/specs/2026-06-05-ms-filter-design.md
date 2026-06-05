# Design: `ms-filter` — Microsoft consumer email filter → HOTMAIL_COMBO

> Date: 2026-06-05
> Status: approved for implementation

## 1. Goal

Add a second, isolated pipeline that reads the `.txt` credential files from the
**TTT LINK:LOGPASS CLONE** channel, keeps **only** Microsoft consumer emails
(`outlook.*`, `hotmail.*`, `live.*`, `msn.com`, `windowslive.com`), and uploads
them as `email:pass` files to a new channel **HOTMAIL_COMBO**.

This is the exact inverse of the existing drop policy: the main `run` pipeline
*drops* Microsoft consumer addresses (they are useless to a plain-IMAP scanner —
see `internal/processor/no_plain_imap.go`); here we *salvage* precisely those
dropped addresses into a dedicated channel.

### Channel IDs (resolved via `tgpipe dialogs`)

| Role | Channel | Raw ID |
|---|---|---|
| Input (source) | TTT LINK:LOGPASS CLONE | `4298264668` |
| Output (target) | HOTMAIL_COMBO | `3585587034` |

## 2. Requirements (confirmed)

- Output format: `email:pass`, one per line.
- Full pipeline like `run` (download → split → parse → batch → upload), with
  crash resume — **not** a fire-and-forget stream.
- Batch threshold: **accumulate 1,000,000 lines per output file** before
  flushing + uploading (line-count based, not MB based).
- Two separate commands: `ms-crawl` (seed) + `ms-run` (process), mirroring the
  existing `crawl`/`run` split. Avoids re-walking the input channel on every run.

## 3. Architecture

Reuse the existing 5-stage orchestrator unchanged in spirit. Only four things
differ between `run` and `ms-run`: the **source/target channels**, the
**processor impl**, the **batch threshold mode**, and the **state DB file**.

```
TTT LINK:LOGPASS CLONE (4298264668)
        │  ms-crawl  → seed ms_state.db jobs table
        ▼
   fetcher → splitter → MicrosoftOnlyExtractor → writer(1M lines) → uploader
                                                                       │
                                                                       ▼
                                                          HOTMAIL_COMBO (3585587034)
```

### Why a separate state DB

The `jobs` table PRIMARY KEY is `msg_id`. The input channel has its own
per-channel msg_id sequence (1, 2, 3 …) that will collide with the main `run`
jobs already in `state.db`. The cleanest isolation is a distinct DB **file**
(`ms_state.db`), which reuses the identical schema + migrations with zero schema
change.

## 4. Components

### 4.1 Config — new optional `ms_filter` section

```yaml
ms_filter:
  source_channel: 4298264668    # TTT LINK:LOGPASS CLONE (input)
  target_channel: 3585587034    # HOTMAIL_COMBO (output)
  db_path: ./ms_state.db        # separate jobs DB — avoids msg_id collision
  batch_lines: 1000000          # flush + upload every 1M lines
```

- New struct `MSFilterConfig` added to `config.Config` as `MSFilter MSFilterConfig \`yaml:"ms_filter"\``.
- The global `Config.Validate()` is **not** changed to require these fields
  (so `run` still works without an `ms_filter` block). Instead the `ms-crawl` /
  `ms-run` commands validate their own required fields at startup
  (`source_channel`, `target_channel`, `db_path`, `batch_lines > 0`).

### 4.2 Processor — `MicrosoftOnlyExtractor` (new)

New file `internal/processor/microsoft_only.go`.

- Parses each line into an `email:pass` anchor using the **same** right-to-left
  colon-scan currently embedded in `UrlUserPassExtractor`.
- Keeps the record **iff** the email domain ∈ Microsoft consumer set.
- Drops everything else (non-email, empty pass, non-Microsoft domain).
- Emits `types.Record{Email, Pass}` exactly like the existing extractor, so the
  writer/uploader need no special-casing.

**Shared-parse refactor.** The parse loop ("find the first valid `email:pass`
anchor scanning colons from the right, skipping empty-pass anchors") is
currently inlined in `UrlUserPassExtractor.Process`. Extract it into a shared
unexported helper, e.g.:

```go
// extractEmailPass returns the first valid email:pass anchor scanned from the
// right, or ok=false. Trailing '\r' stripped from pass.
func extractEmailPass(line []byte) (email, pass []byte, ok bool)
```

Then:

- `UrlUserPassExtractor`: `extractEmailPass` → if `!ok` drop → if
  `isPlainIMAPDisabled(email)` drop → keep. (Behaviour unchanged; covered by the
  existing `url_user_pass_test.go`.)
- `MicrosoftOnlyExtractor`: `extractEmailPass` → if `!ok` drop → if
  `!isMicrosoftConsumer(email)` drop → keep.

**Microsoft consumer domain set — single source of truth.** Today the Microsoft
block lives inside the `noPlainIMAP` map in `no_plain_imap.go`. Introduce a
dedicated `microsoftConsumer` map (the existing Microsoft entries: `outlook.*`,
`hotmail.*`, `live.*`, `msn.com`, `windowslive.com`) and have `isPlainIMAPDisabled`
treat membership in `microsoftConsumer` as disabled too, so the Microsoft domain
list is defined **once**. `isMicrosoftConsumer(email)` does an exact lowercase
domain match against that map.

### 4.3 Writer — add line-count batching

`internal/writer/writer.go` currently flushes on `BatchSizeMB` or
`FlushIntervalSec`. Add `BatchSizeLines int` to `writer.Config`:

- When `BatchSizeLines > 0`, the writer also flushes once the in-memory buffer
  reaches that many lines.
- Flush trigger = MB threshold (if set) **OR** line threshold (if set) **OR**
  timer — whichever fires first.
- `run` keeps `BatchSizeMB` set and `BatchSizeLines = 0` (unchanged).
- `ms-run` sets `BatchSizeLines = 1_000_000`; `BatchSizeMB` may stay set as a
  safety ceiling (e.g. keep config default) or be left to the line cap. Decision:
  keep the MB ceiling active so a pathological run can't buffer unbounded RAM —
  whichever of {1M lines, MB ceiling} hits first flushes.

> Size note: 1M `email:pass` lines ≈ 30–40 MB/file, larger than the 15 MB
> guidance in project memory (large files lengthen each upload on a single
> account). Accepted per explicit requirement; the MB ceiling stays as a guard.

### 4.4 Pipeline — parameterize source/target/processor/batch

`pipeline.New` gains an `Options` struct (or equivalent) carrying:

```go
type Options struct {
    SourceChannel  int64
    TargetChannel  int64
    Processor      processor.LineProcessor
    BatchSizeMB    int
    BatchSizeLines int
}
```

The orchestrator uses `opts.SourceChannel` / `opts.TargetChannel` instead of
`cfg.SourceChannel` / `cfg.TargetChannel`, constructs the processor from
`opts.Processor`, and threads the batch settings into `writer.Config`.

`cmd_run.go` is updated to pass `Options` built from the main config + a
`&processor.UrlUserPassExtractor{}` + `BatchSizeMB` (current behaviour). No
behavioural change to `run`.

### 4.5 Commands — `ms-crawl` and `ms-run`

New files `cmd/tgpipe/cmd_ms_crawl.go` and `cmd/tgpipe/cmd_ms_run.go`,
registered in `main.go`'s `AddCommand`.

- **`ms-crawl`**: open `ms_filter.db_path`, `store.Init`, resolve
  `ms_filter.source_channel`, run the existing `crawler` seeding the ms DB.
  Structurally identical to `cmd_crawl.go` but pointed at the ms config.
- **`ms-run`**: open `ms_filter.db_path`, `store.Init`, build fetch+upload
  pools (reuse `cfg.Fetcher.Sessions` / `cfg.Uploader.Sessions`), then
  `pipeline.New(... Options{SourceChannel: ms.source, TargetChannel: ms.target,
  Processor: &MicrosoftOnlyExtractor{}, BatchSizeLines: ms.batch_lines,
  BatchSizeMB: cfg.Writer.BatchSizeMB})` and `Run`.

Both validate the `ms_filter` block is present and complete before doing any RPC.

## 5. Data flow & semantics

- Idempotency boundary unchanged: a job flips `in_progress → done` only after its
  output file uploads to HOTMAIL_COMBO. Crash mid-flight → resume SQL flips
  `in_progress → pending` on next `ms-run` startup → reprocessed.
- FLOOD_WAIT / FILE_REFERENCE_EXPIRED / backpressure: inherited unchanged from
  the shared stages.
- Edge-line drop (Model C): inherited unchanged.

## 6. Error handling

Same matrix as CLAUDE.md §9 (shared stages). New command-level fatals:

- `ms_filter` block missing or any required field unset/zero → fatal config error
  before any RPC.
- `ms_filter.source_channel == ms_filter.target_channel` → fatal (would loop a
  channel onto itself).

## 7. Testing

- **Unit (new):** `MicrosoftOnlyExtractor` — table test: outlook/hotmail/live/msn
  kept with correct `email:pass`; gmail/yahoo/corporate/non-email dropped;
  `url:email:pass` with embedded colons in pass; trailing `\r`.
- **Unit (refactor guard):** existing `url_user_pass_test.go` must stay green
  after extracting `extractEmailPass` (proves behaviour preserved).
- **Unit (new):** writer line-count flush — feed N records with
  `BatchSizeLines = k`, assert a file is emitted every k lines, remainder flushed
  on close.
- **Unit:** `isMicrosoftConsumer` domain matching (case-insensitivity, exact
  match, subdomain non-match) consistent with the single source of truth.
- **Manual:** `ms-crawl` then `ms-run` against the real channels with a small
  input, verify HOTMAIL_COMBO receives `email:pass` files containing only
  Microsoft consumer addresses.

## 8. Out of scope

- Global dedup across output files (not requested).
- Stats/retry/reset subcommands for the ms DB (can reuse `--config` + a future
  `--db` flag later; not part of this change).
- Changing the main `run` behaviour beyond the mechanical `Options` plumbing.

## 9. Files touched

New:
- `cmd/tgpipe/cmd_ms_crawl.go`
- `cmd/tgpipe/cmd_ms_run.go`
- `internal/processor/microsoft_only.go`
- `internal/processor/microsoft_only_test.go`

Modified:
- `internal/config/config.go` (+`MSFilterConfig`)
- `internal/processor/url_user_pass.go` (extract shared `extractEmailPass`)
- `internal/processor/no_plain_imap.go` (single-source Microsoft set)
- `internal/writer/writer.go` (+`BatchSizeLines`)
- `internal/pipeline/pipeline.go` (+`Options`)
- `cmd/tgpipe/cmd_run.go` (pass `Options`)
- `cmd/tgpipe/main.go` (register commands)
- `config.example.yaml`, `config.yaml` (add `ms_filter` block)
```