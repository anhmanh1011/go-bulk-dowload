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
