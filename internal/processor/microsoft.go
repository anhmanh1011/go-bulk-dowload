package processor

import "bytes"

// microsoftConsumer is the single source of truth for Microsoft consumer email
// domains (outlook / hotmail / live / msn / windowslive). It is read by two
// opposite policies:
//   - isPlainIMAPDisabled (no_plain_imap.go) DROPS these — they block plain IMAP.
//   - isMicrosoftConsumer (below) KEEPS these — the ms-run pipeline salvages them.
//
// All keys are lowercase ASCII; callers lowercase the domain before lookup.
// Domains are exact-match.
var microsoftConsumer = map[string]struct{}{
	"outlook.com":     {},
	"outlook.co.uk":   {},
	"outlook.fr":      {},
	"outlook.de":      {},
	"outlook.es":      {},
	"outlook.it":      {},
	"outlook.jp":      {},
	"outlook.com.br":  {},
	"outlook.com.au":  {},
	"hotmail.com":     {},
	"hotmail.co.uk":   {},
	"hotmail.fr":      {},
	"hotmail.de":      {},
	"hotmail.es":      {},
	"hotmail.it":      {},
	"hotmail.com.br":  {},
	"hotmail.com.ar":  {},
	"hotmail.com.au":  {},
	"live.com":        {},
	"live.co.uk":      {},
	"live.fr":         {},
	"live.de":         {},
	"live.it":         {},
	"live.nl":         {},
	"live.dk":         {},
	"live.se":         {},
	"live.no":         {},
	"live.ca":         {},
	"live.in":         {},
	"live.jp":         {},
	"live.com.au":     {},
	"live.com.mx":     {},
	"msn.com":         {},
	"windowslive.com": {},
}

// isMicrosoftConsumerDomain reports whether the lowercase domain is a Microsoft
// consumer domain.
func isMicrosoftConsumerDomain(domain string) bool {
	_, found := microsoftConsumer[domain]
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
