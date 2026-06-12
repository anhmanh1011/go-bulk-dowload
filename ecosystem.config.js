// pm2 process definition for the tgpipe pipeline.
//   pm2 start ecosystem.config.js     # start
//   pm2 logs tgpipe-run               # follow logs
//   pm2 stop tgpipe-run               # graceful stop (SIGINT → graceful shutdown)
//
// tgpipe is a Go binary (interpreter 'none'). `run` is idempotent and resumes
// on restart, so restart-on-crash is safe. stop_exit_codes:[0] means a clean
// completion (all jobs done → exit 0) does NOT trigger a restart loop.
module.exports = {
  apps: [
    {
      name: 'tgpipe-run',
      script: './bin/tgpipe',
      args: 'run --config config.yaml',
      cwd: '/root/extract_new/go-bulk-dowload',
      interpreter: 'none',
      exec_mode: 'fork',
      instances: 1,

      autorestart: true,
      stop_exit_codes: [0], // clean finish → stop, don't restart
      max_restarts: 10,
      min_uptime: '15s',
      restart_delay: 5000,

      // pm2 sends SIGINT on stop; tgpipe traps it for graceful flush/upload.
      // Give it time before SIGKILL.
      kill_timeout: 20000,

      out_file: './logs/tgpipe-out.log',
      error_file: './logs/tgpipe-err.log',
      merge_logs: true,
      time: true,
    },

    // Forward mirror: copies .txt docs source→target via forwardMessages.
    // Idempotent (resume via `forwarded` table), so restart-on-crash is safe.
    // --log-level debug → full logging captured into ./logs/tgpipe-forward-*.
    {
      name: 'tgpipe-forward',
      script: './bin/tgpipe',
      args: 'forward --config config.yaml --log-level debug',
      cwd: '/root/extract_new/go-bulk-dowload',
      interpreter: 'none',
      exec_mode: 'fork',
      instances: 1,

      autorestart: true,
      stop_exit_codes: [0], // clean finish (all forwarded → exit 0) → stop
      max_restarts: 10,
      min_uptime: '15s',
      restart_delay: 5000,

      // forward traps SIGINT via signal.NotifyContext for a clean stop.
      kill_timeout: 20000,

      out_file: './logs/tgpipe-forward-out.log',
      error_file: './logs/tgpipe-forward-err.log',
      merge_logs: true,
      time: true,
    },

    // Microsoft-consumer filter pipeline: reads ms_filter.source_channel
    // (TTT LINK:LOGPASS CLONE), keeps only Microsoft consumer emails, uploads
    // email:pass (1M lines/file) to ms_filter.target_channel (HOTMAIL_COMBO).
    // Uses a SEPARATE state DB (ms_filter.db_path = ./ms_state.db).
    //
    // PRECONDITION: seed the ms jobs DB once before starting this app:
    //   ./bin/tgpipe ms-crawl --config config.yaml
    // (mirror of how `crawl` precedes `run` — ms-run on an empty DB finds no
    // pending jobs and exits 0 immediately.) Re-run ms-crawl any time to pick
    // up new files in the source channel; it is idempotent.
    {
      name: 'tgpipe-ms-run',
      script: './bin/tgpipe',
      args: 'ms-run --config config.yaml',
      cwd: '/root/extract_new/go-bulk-dowload',
      interpreter: 'none',
      exec_mode: 'fork',
      instances: 1,

      autorestart: true,
      stop_exit_codes: [0], // clean finish (all jobs done → exit 0) → stop
      max_restarts: 10,
      min_uptime: '15s',
      restart_delay: 5000,

      // ms-run traps SIGINT via signal.NotifyContext; the writer flushes any
      // buffered (<1M) tail and the uploader finishes in-flight parts.
      kill_timeout: 20000,

      out_file: './logs/tgpipe-ms-run-out.log',
      error_file: './logs/tgpipe-ms-run-err.log',
      merge_logs: true,
      time: true,
    },
  ],
};
