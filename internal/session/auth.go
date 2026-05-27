package session

import (
	"github.com/gotd/td/session"
)

// newSessionStorage returns gotd's file-backed session storage at path.
// Multiple clients pointing at the same path share the same auth_key;
// concurrent writes to the .session file are serialised internally by gotd.
func newSessionStorage(path string) session.Storage {
	return &session.FileStorage{Path: path}
}
