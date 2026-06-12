package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/spf13/cobra"

	"github.com/manh/tgpipe/internal/channels"
	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/logging"
	"github.com/manh/tgpipe/internal/session"
)

var (
	pullChannel int64
	pullLimit   int
	pullOut     string
)

// pullCmd downloads the N most recent .txt documents from a channel and
// concatenates them, oldest-first, into a single local file. One-shot
// utility — does not touch the jobs table or the rest of the pipeline.
var pullCmd = &cobra.Command{
	Use:   "pull",
	Short: "Download N most recent .txt files from a channel and concat into one file",
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
		chatID := pullChannel
		if chatID == 0 {
			chatID = cfg.TargetChannel
		}
		if chatID == 0 {
			return errors.New("no channel: pass --channel or set target_channel in config")
		}
		if pullLimit <= 0 {
			return errors.New("--limit must be > 0")
		}

		gate := &session.FloodGate{}
		pool, err := session.NewFetchPool(ctx, session.Config{
			APIID:       cfg.Account.APIID,
			APIHash:     cfg.Account.APIHash,
			SessionFile: cfg.Account.SessionFile,
			Size:        1,
		}, gate)
		if err != nil {
			return err
		}
		defer pool.Close()

		accessHash, err := channels.Resolve(ctx, pool, chatID)
		if err != nil {
			return err
		}
		api := tg.NewClient(pool)

		docs, err := listRecentTxtDocs(ctx, api, chatID, accessHash, pullLimit)
		if err != nil {
			return err
		}
		if len(docs) == 0 {
			return fmt.Errorf("no .txt documents found in channel %d", chatID)
		}
		// Oldest → newest so the concatenated file is chronological.
		sort.Slice(docs, func(i, j int) bool { return docs[i].msgID < docs[j].msgID })

		chunkSize := cfg.Fetcher.ChunkSizeBytes
		if chunkSize <= 0 {
			chunkSize = 1 << 20
		}

		f, err := os.Create(pullOut)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		defer f.Close()

		var totalBytes int64
		started := time.Now()
		for i, d := range docs {
			n, err := streamDocToWriter(ctx, api, gate, d, chunkSize, f)
			if err != nil {
				return fmt.Errorf("download msg=%d (%s): %w", d.msgID, d.fileName, err)
			}
			totalBytes += n
			slog.Info("pulled",
				"stage", "pull",
				"idx", i+1,
				"of", len(docs),
				"msg_id", d.msgID,
				"file", d.fileName,
				"bytes", n,
			)
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync output: %w", err)
		}
		slog.Info("pull done",
			"stage", "pull",
			"files", len(docs),
			"total_bytes", totalBytes,
			"out", pullOut,
			"elapsed", time.Since(started).Round(time.Millisecond).String(),
		)
		return nil
	},
}

func init() {
	pullCmd.Flags().Int64Var(&pullChannel, "channel", 0, "raw channel ID (defaults to config target_channel)")
	pullCmd.Flags().IntVar(&pullLimit, "limit", 50, "number of most recent .txt files to download")
	pullCmd.Flags().StringVar(&pullOut, "out", "./combined.txt", "path to combined output file")
}

// docRef is the minimal subset of a Telegram document needed to download it
// via upload.getFile.
type docRef struct {
	msgID         int
	fileID        int64
	accessHash    int64
	fileReference []byte
	size          int64
	fileName      string
}

// listRecentTxtDocs pages messages.getHistory newest-to-oldest until it has
// collected `want` .txt documents (or the channel is exhausted).
func listRecentTxtDocs(ctx context.Context, api *tg.Client, chatID, accessHash int64, want int) ([]docRef, error) {
	out := make([]docRef, 0, want)
	var offsetID int
	for len(out) < want {
		res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer: &tg.InputPeerChannel{
				ChannelID:  chatID,
				AccessHash: accessHash,
			},
			OffsetID: offsetID,
			Limit:    100,
		})
		if err != nil {
			return nil, fmt.Errorf("getHistory: %w", err)
		}
		var raw []tg.MessageClass
		switch v := res.(type) {
		case *tg.MessagesMessages:
			raw = v.Messages
		case *tg.MessagesMessagesSlice:
			raw = v.Messages
		case *tg.MessagesChannelMessages:
			raw = v.Messages
		default:
			return nil, fmt.Errorf("unexpected dialogs response %T", res)
		}
		if len(raw) == 0 {
			break
		}
		minID := 0
		for _, m := range raw {
			msg, ok := m.(*tg.Message)
			if !ok {
				continue
			}
			if minID == 0 || msg.ID < minID {
				minID = msg.ID
			}
			d, ok := docFromMessage(msg)
			if !ok {
				continue
			}
			out = append(out, d)
			if len(out) >= want {
				break
			}
		}
		if minID == 0 || minID == offsetID {
			break
		}
		offsetID = minID
	}
	return out, nil
}

func docFromMessage(msg *tg.Message) (docRef, bool) {
	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		return docRef{}, false
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		return docRef{}, false
	}
	name := ""
	for _, a := range doc.Attributes {
		if fn, ok := a.(*tg.DocumentAttributeFilename); ok {
			name = fn.FileName
			break
		}
	}
	if !strings.HasSuffix(strings.ToLower(name), ".txt") {
		return docRef{}, false
	}
	return docRef{
		msgID:         msg.ID,
		fileID:        doc.ID,
		accessHash:    doc.AccessHash,
		fileReference: doc.FileReference,
		size:          doc.Size,
		fileName:      name,
	}, true
}

// streamDocToWriter pulls one document chunk-by-chunk and writes each chunk
// straight to w. Handles FLOOD_WAIT by sleeping; FILE_REFERENCE_EXPIRED is
// not refreshed here — getHistory just returned a fresh reference, so a stale
// one mid-download is unlikely for a one-shot pull. If it happens, surface
// the error and let the user retry.
func streamDocToWriter(ctx context.Context, api *tg.Client, gate *session.FloodGate, d docRef, chunkSize int, w io.Writer) (int64, error) {
	loc := &tg.InputDocumentFileLocation{
		ID:            d.fileID,
		AccessHash:    d.accessHash,
		FileReference: d.fileReference,
	}
	req := &tg.UploadGetFileRequest{Location: loc, Limit: chunkSize}
	var written int64
	for offset := int64(0); offset < d.size; offset += int64(chunkSize) {
		req.Offset = offset
		var data []byte
		for {
			if err := gate.Wait(ctx); err != nil {
				return written, err
			}
			res, err := api.UploadGetFile(ctx, req)
			if err == nil {
				file, ok := res.(*tg.UploadFile)
				if !ok {
					return written, fmt.Errorf("unexpected upload response %T", res)
				}
				data = file.Bytes
				break
			}
			if ok, sec := session.IsFloodWait(err); ok {
				slog.Warn("FLOOD_WAIT", "stage", "pull", "sec", sec, "msg_id", d.msgID)
				gate.Trigger(time.Duration(sec+1) * time.Second)
				continue
			}
			if tgerr.Is(err, "FILE_REFERENCE_EXPIRED") {
				return written, fmt.Errorf("file reference expired mid-pull (re-run the command): %w", err)
			}
			return written, err
		}
		if len(data) == 0 {
			break
		}
		n, err := w.Write(data)
		if err != nil {
			return written, fmt.Errorf("write output: %w", err)
		}
		written += int64(n)
		if len(data) < chunkSize {
			break
		}
	}
	return written, nil
}
