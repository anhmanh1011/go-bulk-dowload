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
	assert.Contains(t, err.Error(), "account.api_id")
	assert.Contains(t, err.Error(), "account.api_hash")
	assert.Contains(t, err.Error(), "account.session_file")
}
