// Package crawler implements Stage 0 of the pipeline: a one-shot walk of the
// source Telegram channel that seeds the jobs table.
//
// Run iterates messages newest-to-oldest via messages.getHistory, filters to
// .txt document messages, and persists each as a state.Job. The operation is
// idempotent (INSERT OR IGNORE on msg_id), so re-running picks up new
// messages without duplicating existing rows.
package crawler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gotd/td/tg"

	"github.com/manh/tgpipe/internal/session"
	"github.com/manh/tgpipe/internal/state"
)

// Store is the subset of state.Store the crawler needs.
type Store interface {
	InsertJobIfAbsent(ctx context.Context, j state.Job) error
}

// Compile-time assertion: state.Store satisfies the crawler.Store interface.
var _ Store = (*state.Store)(nil)

// Config carries tunables for the crawler.
type Config struct {
	SourceChannel    int64
	SourceAccessHash int64 // resolved by internal/channels at pipeline startup
	BatchSize        int   // messages per page
}

// Crawler seeds the jobs table from a source channel.
type Crawler struct {
	pool  session.Pool
	api   *tg.Client
	store Store
	cfg   Config
}

// New constructs a Crawler. BatchSize defaults to 100 when ≤ 0.
func New(pool session.Pool, store Store, cfg Config) *Crawler {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	return &Crawler{pool: pool, api: tg.NewClient(pool), store: store, cfg: cfg}
}

// Run iterates messages in the source channel from newest to oldest, inserting
// each `.txt` document message as a job. Idempotent — re-running skips existing
// rows (INSERT OR IGNORE).
func (c *Crawler) Run(ctx context.Context) error {
	if c.cfg.SourceAccessHash == 0 {
		return errors.New("crawler: SourceAccessHash is zero — call channels.Resolve before Run")
	}
	var offsetID int
	total := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		res, err := c.api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer: &tg.InputPeerChannel{
				ChannelID:  c.cfg.SourceChannel,
				AccessHash: c.cfg.SourceAccessHash,
			},
			OffsetID: offsetID,
			Limit:    c.cfg.BatchSize,
		})
		if err != nil {
			return fmt.Errorf("getHistory: %w", err)
		}
		msgs := extractMessages(res)
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			j, ok := buildJob(c.cfg.SourceChannel, c.cfg.SourceAccessHash, m)
			if !ok {
				continue
			}
			if err := c.store.InsertJobIfAbsent(ctx, j); err != nil {
				return err
			}
			total++
		}
		offsetID = lastMsgID(msgs)
		slog.Info("crawl progress", "stage", "crawler", "inserted_total", total, "offset_id", offsetID)
	}
	slog.Info("crawl done", "stage", "crawler", "total", total)
	return nil
}

func extractMessages(res tg.MessagesMessagesClass) []*tg.Message {
	var raw []tg.MessageClass
	switch v := res.(type) {
	case *tg.MessagesMessages:
		raw = v.Messages
	case *tg.MessagesMessagesSlice:
		raw = v.Messages
	case *tg.MessagesChannelMessages:
		raw = v.Messages
	default:
		return nil
	}
	out := make([]*tg.Message, 0, len(raw))
	for _, m := range raw {
		if msg, ok := m.(*tg.Message); ok {
			out = append(out, msg)
		}
	}
	return out
}

// lastMsgID returns the smallest MsgID in the page — used as the next OffsetID
// since getHistory pages newest-to-oldest.
func lastMsgID(msgs []*tg.Message) int {
	minID := msgs[0].ID
	for _, m := range msgs {
		if m.ID < minID {
			minID = m.ID
		}
	}
	return minID
}

func buildJob(chatID, chatAccessHash int64, msg *tg.Message) (state.Job, bool) {
	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		return state.Job{}, false
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		return state.Job{}, false
	}
	name := docFileName(doc)
	if !strings.HasSuffix(strings.ToLower(name), ".txt") {
		return state.Job{}, false
	}
	now := time.Now()
	return state.Job{
		MsgID:          int64(msg.ID),
		ChatID:         chatID,
		ChatAccessHash: chatAccessHash,
		FileID:         doc.ID,
		AccessHash:     doc.AccessHash,
		FileReference:  doc.FileReference,
		DCID:           doc.DCID,
		Size:           doc.Size,
		FileName:       name,
		MimeType:       doc.MimeType,
		Status:         state.StatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, true
}

func docFileName(doc *tg.Document) string {
	for _, a := range doc.Attributes {
		if fn, ok := a.(*tg.DocumentAttributeFilename); ok {
			return fn.FileName
		}
	}
	return ""
}
