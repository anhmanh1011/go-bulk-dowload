package channels_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manh/tgpipe/internal/channels"
)

type fakeInv struct{ resp bin.Encoder }

func (f *fakeInv) Invoke(_ context.Context, _ bin.Encoder, out bin.Decoder) error {
	if f.resp == nil {
		return errors.New("no response configured")
	}
	var b bin.Buffer
	if err := f.resp.Encode(&b); err != nil {
		return err
	}
	return out.Decode(&b)
}

func TestResolve_FindsChannel(t *testing.T) {
	inv := &fakeInv{resp: &tg.MessagesDialogsBox{Dialogs: &tg.MessagesDialogs{
		Chats: []tg.ChatClass{
			&tg.Channel{ID: 100, AccessHash: 42, Photo: &tg.ChatPhotoEmpty{}},
			&tg.Channel{ID: 200, AccessHash: 99, Photo: &tg.ChatPhotoEmpty{}},
		},
	}}}
	got, err := channels.Resolve(context.Background(), inv, 200)
	require.NoError(t, err)
	assert.Equal(t, int64(99), got)
}

func TestResolve_NotFound(t *testing.T) {
	inv := &fakeInv{resp: &tg.MessagesDialogsBox{Dialogs: &tg.MessagesDialogs{}}}
	_, err := channels.Resolve(context.Background(), inv, 200)
	assert.Error(t, err)
}
