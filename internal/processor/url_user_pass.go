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
// non-email user → drop.
type UrlUserPassExtractor struct{}

// Compile-time interface assertion.
var _ LineProcessor = (*UrlUserPassExtractor)(nil)

func (e *UrlUserPassExtractor) Process(line []byte) (types.Record, bool, error) {
	n := len(line)
	if n == 0 {
		return types.Record{}, false, nil
	}
	// Drop trivially malformed: no colon, or trailing colon (empty pass).
	lastColon := bytes.LastIndexByte(line, ':')
	if lastColon <= 0 || lastColon == n-1 {
		return types.Record{}, false, nil
	}

	// Scan candidate split points from the right. For each colon c at
	// position i, the candidate user segment is line[prev+1 : i] where
	// prev is the next colon to the left (or -1 if none). The candidate
	// pass is line[i+1:]. Accept the first candidate whose user is a
	// valid email and whose pass is non-empty.
	right := n // exclusive upper bound for the current "left half" search
	for {
		i := bytes.LastIndexByte(line[:right], ':')
		if i < 0 {
			break
		}
		pass := line[i+1:]
		if len(pass) == 0 {
			// Empty pass for this anchor — try a further-left colon.
			right = i
			continue
		}
		prev := bytes.LastIndexByte(line[:i], ':')
		user := line[prev+1 : i]
		if isValidEmail(user) {
			// Email and Pass are sub-slices of `line`; splitter delivers a fresh copy
			// per line, so they are safe to retain without our own copy.
			return types.Record{Email: user, Pass: pass}, true, nil
		}
		right = i
	}
	return types.Record{}, false, nil
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
	if bytes.Count(b, []byte{'@'}) != 1 {
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
