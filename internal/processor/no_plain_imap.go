package processor

import "bytes"

// noPlainIMAP is the set of email-provider domains that have disabled
// basic-auth (plaintext) IMAP login. Records targeting these domains are
// useless for a downstream IMAP scanner that only has email:password — the
// provider requires OAuth2 or app-specific passwords, so dropping them at
// parse time saves the entire scanner round-trip.
//
// Sources (provider announcements 2022–2024):
//   - Gmail            disabled 2022-05  (Less Secure Apps shutdown)
//   - Yahoo + AOL      disabled 2022-10  (app passwords required)
//   - Microsoft (M365) disabled 2022-10  for consumer; staged for tenant
//   - Apple iCloud     always required app-specific passwords
//   - Proton           never offered standard IMAP (Bridge, paid only)
//
// All keys are lowercase ASCII; callers must lowercase the domain before
// lookup. Domains are exact-match — `subdomain.gmail.com` would not hit
// (in practice these providers do not serve mail on subdomains).
var noPlainIMAP = map[string]struct{}{
	// Gmail
	"gmail.com":      {},
	"googlemail.com": {},

	// Yahoo (regional + legacy brands)
	"yahoo.com":     {},
	"ymail.com":     {},
	"rocketmail.com": {},
	"yahoo.co.uk":   {},
	"yahoo.fr":      {},
	"yahoo.de":      {},
	"yahoo.es":      {},
	"yahoo.it":      {},
	"yahoo.co.jp":   {},
	"yahoo.co.kr":   {},
	"yahoo.co.in":   {},
	"yahoo.ca":      {},
	"yahoo.in":      {},
	"yahoo.com.br":  {},
	"yahoo.com.au":  {},
	"yahoo.com.sg":  {},
	"yahoo.com.vn":  {},
	"yahoo.com.tw":  {},
	"yahoo.com.hk":  {},
	"yahoo.com.mx":  {},
	"yahoo.com.ar":  {},
	"yahoo.com.ph":  {},
	"yahoo.com.tr":  {},
	"yahoo.com.cn":  {},

	// AOL (now under Yahoo)
	"aol.com":   {},
	"aol.co.uk": {},
	"aim.com":   {},

	// Microsoft consumer (outlook.com / hotmail / live / msn)
	"outlook.com":    {},
	"outlook.co.uk":  {},
	"outlook.fr":     {},
	"outlook.de":     {},
	"outlook.es":     {},
	"outlook.it":     {},
	"outlook.jp":     {},
	"outlook.com.br": {},
	"outlook.com.au": {},
	"hotmail.com":    {},
	"hotmail.co.uk":  {},
	"hotmail.fr":     {},
	"hotmail.de":     {},
	"hotmail.es":     {},
	"hotmail.it":     {},
	"hotmail.com.br": {},
	"hotmail.com.ar": {},
	"hotmail.com.au": {},
	"live.com":       {},
	"live.co.uk":     {},
	"live.fr":        {},
	"live.de":        {},
	"live.it":        {},
	"live.nl":        {},
	"live.dk":        {},
	"live.se":        {},
	"live.no":        {},
	"live.ca":        {},
	"live.in":        {},
	"live.jp":        {},
	"live.com.au":    {},
	"live.com.mx":    {},
	"msn.com":        {},
	"windowslive.com": {},

	// Apple iCloud
	"icloud.com": {},
	"me.com":     {},
	"mac.com":    {},

	// Proton (no standard IMAP — requires Proton Bridge, paid plans only)
	"protonmail.com": {},
	"protonmail.ch":  {},
	"proton.me":      {},
	"pm.me":          {},
}

// isPlainIMAPDisabled reports whether the email's domain belongs to a
// provider that has disabled basic-auth IMAP login. Callers must have
// already passed the bytes through isValidEmail (exactly one '@', no
// whitespace).
func isPlainIMAPDisabled(email []byte) bool {
	at := bytes.IndexByte(email, '@')
	if at < 0 {
		return false
	}
	domain := bytes.ToLower(email[at+1:])
	_, found := noPlainIMAP[string(domain)]
	return found
}
