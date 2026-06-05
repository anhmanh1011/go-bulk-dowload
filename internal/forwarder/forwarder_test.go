package forwarder

import (
	"testing"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func txtMsg(id int, name string) *tg.Message {
	return &tg.Message{
		ID: id,
		Media: &tg.MessageMediaDocument{
			Document: &tg.Document{
				Attributes: []tg.DocumentAttributeClass{
					&tg.DocumentAttributeFilename{FileName: name},
				},
			},
		},
	}
}

func TestTxtMessageIDs(t *testing.T) {
	msgs := []*tg.Message{
		txtMsg(10, "data.txt"),
		txtMsg(11, "photo.jpg"),          // wrong extension
		txtMsg(12, "UPPER.TXT"),          // case-insensitive
		{ID: 13, Media: nil},             // no media
		{ID: 14, Message: "hello world"}, // plain text message
		txtMsg(15, "archive.txt"),
	}
	got := txtMessageIDs(msgs)
	assert.Equal(t, []int{10, 12, 15}, got)
}

func TestTxtMessageIDs_NonDocumentMedia(t *testing.T) {
	msgs := []*tg.Message{
		{ID: 1, Media: &tg.MessageMediaPhoto{}},
		{ID: 2, Media: &tg.MessageMediaDocument{Document: &tg.DocumentEmpty{}}},
	}
	assert.Empty(t, txtMessageIDs(msgs))
}

func TestChunkIDs(t *testing.T) {
	assert.Nil(t, chunkIDs(nil, 100))
	assert.Equal(t, [][]int{{1, 2}}, chunkIDs([]int{1, 2}, 100))
	assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, chunkIDs([]int{1, 2, 3, 4, 5}, 2))
	// size ≤ 0 falls back to 100 → single chunk for a small slice.
	assert.Equal(t, [][]int{{1, 2, 3}}, chunkIDs([]int{1, 2, 3}, 0))
}

func TestRandomIDs(t *testing.T) {
	ids, err := randomIDs(5)
	require.NoError(t, err)
	require.Len(t, ids, 5)
	// Collisions across 5 64-bit draws are astronomically unlikely; assert uniqueness.
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		seen[id] = struct{}{}
	}
	assert.Len(t, seen, 5)

	empty, err := randomIDs(0)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestLastMsgID(t *testing.T) {
	msgs := []*tg.Message{{ID: 50}, {ID: 30}, {ID: 70}, {ID: 31}}
	assert.Equal(t, 30, lastMsgID(msgs))
}

func TestIDConversions(t *testing.T) {
	assert.Equal(t, []int64{1, 2, 3}, ids64([]int{1, 2, 3}))
	assert.Equal(t, []int{4, 5}, toInt([]int64{4, 5}))
	assert.Empty(t, ids64(nil))
	assert.Empty(t, toInt(nil))
}
