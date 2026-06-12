-- Tracks source message IDs already forwarded to the target channel by the
-- `forward` command. Separate from `jobs` (the download pipeline) — the
-- forward path never downloads, it only mirrors .txt documents A→B and uses
-- this table for idempotent resume.
CREATE TABLE IF NOT EXISTS forwarded (
    msg_id       INTEGER PRIMARY KEY,
    forwarded_at INTEGER NOT NULL
);
