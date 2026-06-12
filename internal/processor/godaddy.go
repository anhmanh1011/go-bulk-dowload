package processor

import (
	"bytes"

	"github.com/manh/tgpipe/internal/types"
)

// GodaddyFilter keeps lines that contain "godaddy.com" — matching both
// raw-email content (e.g. "From: donotreply@godaddy.com") and combo-list
// entries where the service URL is a GoDaddy-hosted domain.
//
// For combo-format lines (url:email:pass or email:pass) the parser extracts
// email:pass and emits that pair. For lines that don't fit the combo format
// (e.g. raw e-mail headers / log lines) the full raw line is kept in Email
// and Pass is left nil — the writer will omit the trailing colon.
type GodaddyFilter struct{}

// Compile-time interface assertion.
var _ LineProcessor = (*GodaddyFilter)(nil)

var godaddyToken = []byte("godaddy.com")

func (f *GodaddyFilter) Process(line []byte) (types.Record, bool, error) {
	// Fast path: reject lines that don't mention godaddy.com at all.
	// The token is always lowercase in URLs and e-mail addresses, so a
	// plain Contains is correct and avoids an allocation.
	if !bytes.Contains(line, godaddyToken) {
		return types.Record{}, false, nil
	}
	// Try to parse as url:email:pass or email:pass. If successful, emit
	// just the credential pair (same contract as UrlUserPassExtractor).
	if email, pass, ok := extractEmailPass(line); ok {
		return types.Record{Email: email, Pass: pass}, true, nil
	}
	// Non-combo line (raw e-mail header, log entry, …): keep the full line
	// so no information is lost. Pass is nil → writer emits "line\n".
	return types.Record{Email: line, Pass: nil}, true, nil
}
