package processor

import (
	"bytes"
	"strings"
)

// Microsoft consumer email domains are identified by their first label rather
// than an exhaustive TLD list: Microsoft serves consumer mail on outlook.*,
// hotmail.*, and live.* across dozens of regional TLDs (outlook.com, outlook.sa,
// hotmail.be, live.com.pt, outlook.cz, …). Enumerating every TLD is hopeless and
// misses the long tail — real combolists are full of regional variants — so we
// match the leading "<brand>." prefix instead, plus the two brandless consumer
// domains (msn.com, windowslive.com).
//
// This is the single source of truth, read by two opposite policies:
//   - isPlainIMAPDisabled (no_plain_imap.go) DROPS these — they block plain IMAP.
//   - isMicrosoftConsumer (below) KEEPS these — the ms-run pipeline salvages them.
//
// microsoftConsumerPrefixes matches any domain beginning with one of these
// labels followed by a '.', so "outlook." matches outlook.com and outlook.sa but
// not "myoutlook.com" or "olive.com". Callers lowercase the domain first.
var microsoftConsumerPrefixes = []string{
	"outlook.",
	"hotmail.",
	"live.",
}

// microsoftConsumerExact covers Microsoft consumer domains whose brand label has
// no TLD pattern of its own.
var microsoftConsumerExact = map[string]struct{}{
	"msn.com":         {},
	"windowslive.com": {},
	"passport.com":    {},
}

// isMicrosoftConsumerDomain reports whether the lowercase domain is a Microsoft
// consumer domain (any outlook.*/hotmail.*/live.* regional variant, or one of the
// brandless consumer domains).
func isMicrosoftConsumerDomain(domain string) bool {
	for _, p := range microsoftConsumerPrefixes {
		if strings.HasPrefix(domain, p) {
			return true
		}
	}
	_, found := microsoftConsumerExact[domain]
	return found
}

// isMicrosoftConsumer reports whether the email's domain is a Microsoft consumer
// domain. Callers should already have validated the email shape (one '@').
func isMicrosoftConsumer(email []byte) bool {
	at := bytes.IndexByte(email, '@')
	if at < 0 {
		return false
	}
	return isMicrosoftConsumerDomain(string(bytes.ToLower(email[at+1:])))
}
