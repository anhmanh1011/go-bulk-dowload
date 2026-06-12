package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	"github.com/spf13/cobra"

	"github.com/manh/tgpipe/internal/config"
	"github.com/manh/tgpipe/internal/logging"
	"github.com/manh/tgpipe/internal/session"
)

var (
	pullFileUsername string
	pullFileName     string
	pullFileOut      string
	pullFileScan     int
	pullFileList     bool
)

// pullFileCmd downloads a single document identified by exact filename from a
// chat resolved via @username. One-shot utility — does not touch the jobs
// table.
var pullFileCmd = &cobra.Command{
	Use:   "pull-file",
	Short: "Download a single document by filename from a chat (resolved via @username)",
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
		if pullFileUsername == "" {
			return errors.New("--username required")
		}
		if !pullFileList && pullFileName == "" {
			return errors.New("--name required (or pass --list to inspect the chat)")
		}
		outPath := pullFileOut
		if outPath == "" {
			outPath = "./" + pullFileName
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

		api := tg.NewClient(pool)

		peer, err := resolveUsernameToPeer(ctx, api, strings.TrimPrefix(pullFileUsername, "@"))
		if err != nil {
			return err
		}

		if pullFileList {
			return listDocs(ctx, api, peer, pullFileScan)
		}

		doc, err := findDocByName(ctx, api, peer, pullFileName, pullFileScan)
		if err != nil {
			return err
		}

		chunkSize := cfg.Fetcher.ChunkSizeBytes
		if chunkSize <= 0 {
			chunkSize = 1 << 20
		}

		f, err := os.Create(outPath)
		if err != nil {
			return fmt.Errorf("create output: %w", err)
		}
		defer f.Close()

		started := time.Now()
		n, err := streamDocToWriter(ctx, api, gate, doc, chunkSize, f)
		if err != nil {
			return fmt.Errorf("download msg=%d (%s): %w", doc.msgID, doc.fileName, err)
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync output: %w", err)
		}
		slog.Info("pull-file done",
			"stage", "pull-file",
			"msg_id", doc.msgID,
			"file", doc.fileName,
			"bytes", n,
			"out", outPath,
			"elapsed", time.Since(started).Round(time.Millisecond).String(),
		)
		return nil
	},
}

func init() {
	pullFileCmd.Flags().StringVar(&pullFileUsername, "username", "", "chat username (e.g. @example)")
	pullFileCmd.Flags().StringVar(&pullFileName, "name", "", "exact filename to download (case-insensitive)")
	pullFileCmd.Flags().StringVar(&pullFileOut, "out", "", "output path (defaults to ./<name>)")
	pullFileCmd.Flags().IntVar(&pullFileScan, "scan", 2000, "max messages to scan newest-first looking for the file")
	pullFileCmd.Flags().BoolVar(&pullFileList, "list", false, "list documents found in the chat instead of downloading")
}

// listDocs prints every document (msg_id, size, filename) found in the chat
// scanning newest-first up to scan messages. Diagnostic helper for --list.
func listDocs(ctx context.Context, api *tg.Client, peer tg.InputPeerClass, scan int) error {
	var offsetID int
	seen, found := 0, 0
	for seen < scan {
		res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     peer,
			OffsetID: offsetID,
			Limit:    100,
		})
		if err != nil {
			return fmt.Errorf("getHistory: %w", err)
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
			return fmt.Errorf("unexpected history response %T", res)
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
			seen++
			if d, ok := anyDocFromMessage(msg); ok {
				fmt.Printf("msg=%-8d size=%-10d name=%s\n", d.msgID, d.size, d.fileName)
				found++
			}
		}
		if minID == 0 || minID == offsetID {
			break
		}
		offsetID = minID
	}
	fmt.Printf("\nscanned %d messages, %d documents\n", seen, found)
	return nil
}

// resolveUsernameToPeer turns @foo into a fully-qualified InputPeer. Supports
// channel, user, and basic-chat peers — anything else surfaces as an error.
func resolveUsernameToPeer(ctx context.Context, api *tg.Client, username string) (tg.InputPeerClass, error) {
	res, err := api.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{
		Username: username,
	})
	if err != nil {
		return nil, fmt.Errorf("resolveUsername %q: %w", username, err)
	}
	switch p := res.Peer.(type) {
	case *tg.PeerChannel:
		for _, c := range res.Chats {
			ch, ok := c.(*tg.Channel)
			if !ok || ch.ID != p.ChannelID {
				continue
			}
			return &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash}, nil
		}
		return nil, fmt.Errorf("channel %d not found in resolveUsername chats", p.ChannelID)
	case *tg.PeerUser:
		for _, u := range res.Users {
			us, ok := u.(*tg.User)
			if !ok || us.ID != p.UserID {
				continue
			}
			return &tg.InputPeerUser{UserID: us.ID, AccessHash: us.AccessHash}, nil
		}
		return nil, fmt.Errorf("user %d not found in resolveUsername users", p.UserID)
	case *tg.PeerChat:
		return &tg.InputPeerChat{ChatID: p.ChatID}, nil
	default:
		return nil, fmt.Errorf("unsupported peer %T", res.Peer)
	}
}

// findDocByName walks the chat history newest-first up to `scan` messages
// and returns the first document whose filename matches wantName
// case-insensitively.
func findDocByName(ctx context.Context, api *tg.Client, peer tg.InputPeerClass, wantName string, scan int) (docRef, error) {
	target := strings.ToLower(wantName)
	var offsetID int
	seen := 0
	for seen < scan {
		res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     peer,
			OffsetID: offsetID,
			Limit:    100,
		})
		if err != nil {
			return docRef{}, fmt.Errorf("getHistory: %w", err)
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
			return docRef{}, fmt.Errorf("unexpected history response %T", res)
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
			seen++
			d, ok := anyDocFromMessage(msg)
			if !ok {
				continue
			}
			if strings.ToLower(d.fileName) == target {
				return d, nil
			}
		}
		if minID == 0 || minID == offsetID {
			break
		}
		offsetID = minID
	}
	return docRef{}, fmt.Errorf("file %q not found in last %d messages of chat", wantName, seen)
}

// anyDocFromMessage extracts a docRef from any document message (no .txt
// filter, unlike docFromMessage in cmd_pull.go).
func anyDocFromMessage(msg *tg.Message) (docRef, bool) {
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
	if name == "" {
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
