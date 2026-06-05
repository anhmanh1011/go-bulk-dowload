// Package forwarder implements the `forward` command: a one-shot mirror of
// .txt document messages from a source Telegram channel to a target channel
// using messages.forwardMessages. It never downloads file bytes — Telegram
// copies the document server-side. Forwards are sent as copies (DropAuthor)
// so the target shows no "Forwarded from" header.
//
// Resume is idempotent: each forwarded source msg_id is recorded in the
// `forwarded` SQLite table, and already-recorded ids are skipped on re-run.
package forwarder

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/manh/tgpipe/internal/session"
)

// Store is the subset of state.Store the forwarder needs for resume.
type Store interface {
	FilterUnforwarded(ctx context.Context, ids []int64) ([]int64, error)
	MarkForwarded(ctx context.Context, ids []int64, at int64) error
}

// Config carries the resolved peers and tunables.
type Config struct {
	SourceChannel    int64
	SourceAccessHash int64
	TargetChannel    int64
	TargetAccessHash int64
	PageSize         int // messages.getHistory page limit; default 100
	ForwardBatch     int // ids per forwardMessages call (≤100); default 100
}

// Forwarder mirrors .txt documents from source to target.
type Forwarder struct {
	pool  session.Pool
	api   *tg.Client
	store Store
	gate  *session.FloodGate
	cfg   Config
}

// New constructs a Forwarder. PageSize and ForwardBatch default to 100 (the
// Telegram max for both getHistory and forwardMessages) when ≤ 0.
func New(pool session.Pool, store Store, gate *session.FloodGate, cfg Config) *Forwarder {
	if cfg.PageSize <= 0 {
		cfg.PageSize = 100
	}
	if cfg.ForwardBatch <= 0 || cfg.ForwardBatch > 100 {
		cfg.ForwardBatch = 100
	}
	return &Forwarder{pool: pool, api: tg.NewClient(pool), store: store, gate: gate, cfg: cfg}
}

// Run pages the source channel newest→oldest, forwarding every .txt document
// not already recorded. Idempotent across runs; safe to re-run after a crash.
func (f *Forwarder) Run(ctx context.Context) error {
	if f.cfg.SourceAccessHash == 0 || f.cfg.TargetAccessHash == 0 {
		return errors.New("forwarder: source/target access hash is zero — call channels.Resolve before Run")
	}
	var offsetID int
	total := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var res tg.MessagesMessagesClass
		err := f.withFloodRetry(ctx, func() error {
			var e error
			res, e = f.api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
				Peer: &tg.InputPeerChannel{
					ChannelID:  f.cfg.SourceChannel,
					AccessHash: f.cfg.SourceAccessHash,
				},
				OffsetID: offsetID,
				Limit:    f.cfg.PageSize,
			})
			return e
		})
		if err != nil {
			return fmt.Errorf("getHistory: %w", err)
		}

		msgs := extractMessages(res)
		if len(msgs) == 0 {
			break
		}

		// Only .txt documents are eligible; everything else (text, photos,
		// service messages) is paged over but never forwarded.
		fresh, err := f.store.FilterUnforwarded(ctx, ids64(txtMessageIDs(msgs)))
		if err != nil {
			return err
		}
		for _, batch := range chunkIDs(toInt(fresh), f.cfg.ForwardBatch) {
			if err := f.forwardBatch(ctx, batch); err != nil {
				return err
			}
			if err := f.store.MarkForwarded(ctx, ids64(batch), time.Now().Unix()); err != nil {
				return err
			}
			total += len(batch)
		}

		offsetID = lastMsgID(msgs)
		slog.Info("forward progress", "stage", "forwarder",
			"forwarded_total", total, "offset_id", offsetID)
	}
	slog.Info("forward done", "stage", "forwarder", "total", total)
	return nil
}

// forwardBatch forwards up to 100 message ids as copies. RandomID is generated
// once and reused across FLOOD_WAIT retries so a retried call dedups
// server-side rather than producing duplicate target messages.
func (f *Forwarder) forwardBatch(ctx context.Context, ids []int) error {
	rnd, err := randomIDs(len(ids))
	if err != nil {
		return fmt.Errorf("random id: %w", err)
	}
	req := &tg.MessagesForwardMessagesRequest{
		FromPeer: &tg.InputPeerChannel{
			ChannelID:  f.cfg.SourceChannel,
			AccessHash: f.cfg.SourceAccessHash,
		},
		ToPeer: &tg.InputPeerChannel{
			ChannelID:  f.cfg.TargetChannel,
			AccessHash: f.cfg.TargetAccessHash,
		},
		ID:         ids,
		RandomID:   rnd,
		DropAuthor: true, // copy: no "Forwarded from" header on target
	}
	return f.withFloodRetry(ctx, func() error {
		_, e := f.api.MessagesForwardMessages(ctx, req)
		if e != nil && tgerr.Is(e, "CHAT_FORWARDS_RESTRICTED") {
			// Source has protected content ("Restrict saving content") —
			// forwarding is impossible, retrying won't help. Fail fast.
			return fmt.Errorf("forward blocked: source channel restricts forwarding (protected content): %w", e)
		}
		return e
	})
}

// withFloodRetry runs fn, honouring the account-wide FloodGate before each
// attempt and retrying (without limit) on FLOOD_WAIT — per CLAUDE.md §9,
// FLOOD_WAIT is not a permanent failure.
func (f *Forwarder) withFloodRetry(ctx context.Context, fn func() error) error {
	for {
		if err := f.gate.Wait(ctx); err != nil {
			return err
		}
		err := fn()
		if err == nil {
			return nil
		}
		if fw, sec := session.IsFloodWait(err); fw {
			slog.Warn("forwarder: flood wait", "stage", "forwarder", "seconds", sec)
			f.gate.Trigger(time.Duration(sec) * time.Second)
			continue
		}
		return err
	}
}

// --- pure helpers (unit-tested) ---

// txtMessageIDs returns the ids of messages carrying a .txt document, in input
// order. Non-document and non-.txt messages are skipped.
func txtMessageIDs(msgs []*tg.Message) []int {
	out := make([]int, 0, len(msgs))
	for _, m := range msgs {
		if isTxtDoc(m) {
			out = append(out, m.ID)
		}
	}
	return out
}

func isTxtDoc(m *tg.Message) bool {
	media, ok := m.Media.(*tg.MessageMediaDocument)
	if !ok {
		return false
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		return false
	}
	return strings.HasSuffix(strings.ToLower(docFileName(doc)), ".txt")
}

func docFileName(doc *tg.Document) string {
	for _, a := range doc.Attributes {
		if fn, ok := a.(*tg.DocumentAttributeFilename); ok {
			return fn.FileName
		}
	}
	return ""
}

// chunkIDs splits ids into sub-slices of at most `size` elements, preserving
// order. size ≤ 0 defaults to 100.
func chunkIDs(ids []int, size int) [][]int {
	if size <= 0 {
		size = 100
	}
	var out [][]int
	for i := 0; i < len(ids); i += size {
		end := i + size
		if end > len(ids) {
			end = len(ids)
		}
		out = append(out, ids[i:end])
	}
	return out
}

// randomIDs generates n cryptographically-random int64 values for the
// forwardMessages random_id field (used by Telegram for dedup).
func randomIDs(n int) ([]int64, error) {
	out := make([]int64, n)
	var b [8]byte
	for i := range out {
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		out[i] = int64(binary.LittleEndian.Uint64(b[:]))
	}
	return out, nil
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

// lastMsgID returns the smallest id in the page — the next OffsetID, since
// getHistory pages newest-to-oldest.
func lastMsgID(msgs []*tg.Message) int {
	minID := msgs[0].ID
	for _, m := range msgs {
		if m.ID < minID {
			minID = m.ID
		}
	}
	return minID
}

func ids64(ids []int) []int64 {
	out := make([]int64, len(ids))
	for i, id := range ids {
		out[i] = int64(id)
	}
	return out
}

func toInt(ids []int64) []int {
	out := make([]int, len(ids))
	for i, id := range ids {
		out[i] = int(id)
	}
	return out
}
