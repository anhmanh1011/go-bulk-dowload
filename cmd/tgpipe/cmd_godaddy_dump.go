package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/gotd/td/tg"
	"github.com/spf13/cobra"

	"github.com/manh/tgpipe/internal/channels"
	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/logging"
	"github.com/manh/tgpipe/internal/session"
)

var (
	godaddyDumpOut    string
	godaddyDumpFilter string
)

// godaddyDumpCmd downloads every .txt file from godaddy_filter.source_channel,
// filters each line for a substring (default: "donotreply@godaddy.com"), and
// appends matching lines to a single local output file.
// No SQLite DB, no Telegram upload — pure download → filter → write.
var godaddyDumpCmd = &cobra.Command{
	Use:   "godaddy-dump",
	Short: "Download all .txt files from godaddy_filter.source_channel, filter lines, write to a local file",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		if cfg.GodaddyFilter.SourceChannel == 0 {
			return fmt.Errorf("godaddy_filter.source_channel must be set in config")
		}
		if _, err := logging.Setup(logging.Config{
			Level:  resolveLogLevel(cfg.Logging.Level),
			Format: cfg.Logging.Format,
		}); err != nil {
			return err
		}

		filterToken := []byte(godaddyDumpFilter)

		gate := &session.FloodGate{}
		pool, err := session.NewFetchPool(ctx, session.Config{
			APIID:       cfg.Account.APIID,
			APIHash:     cfg.Account.APIHash,
			SessionFile: cfg.Account.SessionFile,
			Size:        cfg.Fetcher.Sessions,
		}, gate)
		if err != nil {
			return err
		}
		defer pool.Close()

		chatID := cfg.GodaddyFilter.SourceChannel
		accessHash, err := channels.Resolve(ctx, pool, chatID)
		if err != nil {
			return err
		}
		api := tg.NewClient(pool)

		// Collect ALL .txt documents in the channel (limit high enough to exhaust it).
		docs, err := listRecentTxtDocs(ctx, api, chatID, accessHash, 1_000_000)
		if err != nil {
			return err
		}
		if len(docs) == 0 {
			return fmt.Errorf("no .txt documents found in channel %d", chatID)
		}
		// Process oldest → newest for a chronological output file.
		sort.Slice(docs, func(i, j int) bool { return docs[i].msgID < docs[j].msgID })
		slog.Info("godaddy-dump starting",
			"stage", "godaddy-dump",
			"docs", len(docs),
			"filter", godaddyDumpFilter,
			"out", godaddyDumpOut,
		)

		chunkSize := cfg.Fetcher.ChunkSizeBytes
		if chunkSize <= 0 {
			chunkSize = 1 << 20
		}

		outFile, err := os.Create(godaddyDumpOut)
		if err != nil {
			return fmt.Errorf("create output %q: %w", godaddyDumpOut, err)
		}
		defer outFile.Close()
		bw := bufio.NewWriterSize(outFile, 4<<20) // 4 MB write buffer

		matched := 0
		started := time.Now()
		for i, d := range docs {
			if ctx.Err() != nil {
				break
			}
			lf := &lineFilter{w: bw, token: filterToken, matched: &matched}
			_, dlErr := streamDocToWriter(ctx, api, gate, d, chunkSize, lf)
			lf.flushRemainder() // handle last line without trailing newline
			if dlErr != nil {
				slog.Warn("download error — skipping file",
					"stage", "godaddy-dump",
					"msg_id", d.msgID,
					"file", d.fileName,
					"err", dlErr,
				)
				continue
			}
			slog.Info("file done",
				"stage", "godaddy-dump",
				"idx", fmt.Sprintf("%d/%d", i+1, len(docs)),
				"msg_id", d.msgID,
				"file", d.fileName,
				"matched_total", matched,
			)
		}

		if err := bw.Flush(); err != nil {
			return fmt.Errorf("flush output: %w", err)
		}
		if err := outFile.Sync(); err != nil {
			return fmt.Errorf("sync output: %w", err)
		}
		slog.Info("godaddy-dump done",
			"stage", "godaddy-dump",
			"files", len(docs),
			"matched_lines", matched,
			"out", godaddyDumpOut,
			"elapsed", time.Since(started).Round(time.Millisecond).String(),
		)
		fmt.Printf("\nDone. %d matching lines written to %s\n", matched, godaddyDumpOut)
		return nil
	},
}

func init() {
	godaddyDumpCmd.Flags().StringVar(&godaddyDumpOut, "out", "./godaddy_results.txt", "output file path")
	godaddyDumpCmd.Flags().StringVar(&godaddyDumpFilter, "filter", "donotreply@godaddy.com", "substring to match in each line (case-sensitive)")
}

// lineFilter is an io.Writer that buffers incoming data, splits on '\n', and
// forwards only lines containing token to the underlying writer. No allocation
// per line beyond the append — buf is reused across Write calls.
type lineFilter struct {
	w       io.Writer
	token   []byte
	buf     []byte
	matched *int
}

func (lf *lineFilter) Write(p []byte) (int, error) {
	lf.buf = append(lf.buf, p...)
	for {
		idx := bytes.IndexByte(lf.buf, '\n')
		if idx < 0 {
			break
		}
		line := lf.buf[:idx]
		lf.buf = lf.buf[idx+1:]
		if err := lf.emit(line); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// flushRemainder processes any bytes left in buf after the last newline
// (final line of a file that has no trailing newline).
func (lf *lineFilter) flushRemainder() {
	if len(lf.buf) > 0 {
		_ = lf.emit(lf.buf)
		lf.buf = lf.buf[:0]
	}
}

func (lf *lineFilter) emit(line []byte) error {
	if !bytes.Contains(line, lf.token) {
		return nil
	}
	// Strip trailing \r from CRLF input.
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	if _, err := lf.w.Write(line); err != nil {
		return err
	}
	if _, err := lf.w.Write([]byte{'\n'}); err != nil {
		return err
	}
	*lf.matched++
	return nil
}
