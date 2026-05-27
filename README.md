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
