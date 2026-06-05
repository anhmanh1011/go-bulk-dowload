package processor

import "github.com/manh/tgpipe/internal/types"

// MicrosoftOnlyExtractor is the inverse of UrlUserPassExtractor's drop policy:
// it parses url:user:pass (or email:pass) lines and KEEPS only emails on
// Microsoft consumer domains (outlook / hotmail / live / msn / windowslive),
// emitting email:pass records. Everything else — non-email users, empty pass,
// and non-Microsoft domains — is dropped. Used by the `ms-run` pipeline to
// salvage exactly the addresses the main pipeline discards.
type MicrosoftOnlyExtractor struct{}

// Compile-time interface assertion.
var _ LineProcessor = (*MicrosoftOnlyExtractor)(nil)

func (e *MicrosoftOnlyExtractor) Process(line []byte) (types.Record, bool, error) {
	email, pass, ok := extractEmailPass(line)
	if !ok {
		return types.Record{}, false, nil
	}
	if !isMicrosoftConsumer(email) {
		return types.Record{}, false, nil
	}
	// Email and Pass alias `line`; splitter delivers a fresh copy per line.
	return types.Record{Email: email, Pass: pass}, true, nil
}
