package processor

import (
	"bytes"

	"github.com/manh/tgpipe/internal/types"
)

// UrlUserPassExtractor parses lines of the form url:user:pass and emits
// email:pass records. Passwords may legally contain ':' (e.g. "p:a:ss"),
// so a naive "split on last two colons" misclassifies them. We instead
// anchor on the user segment: scan colons from the right and pick the
// first split whose left-adjacent segment looks like an email. URLs
// containing ':' (scheme, port) fall to the left of that anchor and are
// discarded; everything to the right of the anchor's colon is the pass.
//
// Strict: user must look like an email. Empty user, empty pass, or
// non-email user → drop. Emails on providers that have disabled basic-auth
// IMAP login (Gmail, Yahoo, Microsoft consumer, Apple, Proton, …) are also
// dropped — see no_plain_imap.go — because they're unusable for a plain
// email:password IMAP scanner downstream.
type UrlUserPassExtractor struct{}

// Compile-time interface assertion.
var _ LineProcessor = (*UrlUserPassExtractor)(nil)

func (e *UrlUserPassExtractor) Process(line []byte) (types.Record, bool, error) {
	email, pass, ok := extractEmailPass(line)
	if !ok {
		return types.Record{}, false, nil
	}
	// Valid email but the provider blocks basic-auth IMAP → useless to a plain
	// email:password scanner, so drop.
	if isPlainIMAPDisabled(email) {
		return types.Record{}, false, nil
	}
	// Email and Pass alias `line`; splitter delivers a fresh copy per line, so
	// retaining the sub-slices is safe.
	return types.Record{Email: email, Pass: pass}, true, nil
}

// extractEmailPass scans line for the first valid email:pass anchor, searching
// colons from the right. For each colon it treats everything after as the pass
// and the segment between the previous colon and this one as the candidate user;
// it accepts the first candidate whose user is a valid email and whose pass is
// non-empty (skipping empty-pass anchors by moving further left). A trailing
// CR on pass (from CRLF input) is stripped. Returns ok=false when no anchor
// qualifies. The returned slices alias line.
func extractEmailPass(line []byte) (email, pass []byte, ok bool) {
	n := len(line)
	if n == 0 {
		return nil, nil, false
	}
	lastColon := bytes.LastIndexByte(line, ':')
	if lastColon <= 0 || lastColon == n-1 {
		return nil, nil, false
	}
	right := n
	for {
		i := bytes.LastIndexByte(line[:right], ':')
		if i < 0 {
			return nil, nil, false
		}
		p := line[i+1:]
		if len(p) > 0 && p[len(p)-1] == '\r' {
			p = p[:len(p)-1]
		}
		if len(p) == 0 {
			right = i
			continue
		}
		prev := bytes.LastIndexByte(line[:i], ':')
		u := line[prev+1 : i]
		if isValidEmail(u) {
			return u, p, true
		}
		right = i
	}
}

// isValidEmail is a deliberately minimal check (no regex):
//   - exactly one '@'
//   - non-empty local part with no whitespace
//   - domain contains '.' and no whitespace
func isValidEmail(b []byte) bool {
	at := bytes.IndexByte(b, '@')
	if at <= 0 || at == len(b)-1 {
		return false
	}
	// Check there's exactly one '@' — no later occurrence.
	if bytes.IndexByte(b[at+1:], '@') >= 0 {
		return false
	}
	local := b[:at]
	domain := b[at+1:]
	if hasWhitespace(local) || hasWhitespace(domain) {
		return false
	}
	if bytes.IndexByte(domain, '.') < 0 {
		return false
	}
	return true
}

func hasWhitespace(b []byte) bool {
	for _, c := range b {
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			return true
		}
	}
	return false
}
