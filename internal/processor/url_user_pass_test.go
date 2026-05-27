package processor_test

import (
	"testing"

	"github.com/manh/tgpipe/internal/processor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUrlUserPassExtractor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in        string
		wantKeep  bool
		wantEmail string
		wantPass  string
	}{
		// Happy path
		{"https://site.com:user@x.com:pass123", true, "user@x.com", "pass123"},
		{"http://a.b.c:foo@bar.io:p:a:ss", true, "foo@bar.io", "p:a:ss"},
		{"site.com:8080:e@x.com:pass", true, "e@x.com", "pass"},
		{"user@x.com:pass", true, "user@x.com", "pass"}, // 1 colon = 2 parts
		// Reject — user is not email
		{"https://site.com:johnsmith:1234", false, "", ""},
		{"https://site.com:user@:1234", false, "", ""},
		{"https://site.com:@x.com:1234", false, "", ""},
		// Malformed
		{"", false, "", ""},
		{"abc", false, "", ""},
		{"only:one_colon", false, "", ""},
		// Edge — empty pass → drop
		{"https://site:user@x.com:", false, "", ""},
		// Edge — whitespace in email → reject
		{"https://site.com: user@x.com :pass", false, "", ""},
		// IMAP disabled (basic auth blocked by provider) → drop
		{"https://site.com:user@gmail.com:pass", false, "", ""},
		{"https://site.com:user@YAHOO.com:pass", false, "", ""}, // case-insensitive
		{"https://site.com:foo@hotmail.co.uk:bar", false, "", ""},
		{"https://site.com:foo@outlook.com:bar", false, "", ""},
		{"https://site.com:foo@icloud.com:bar", false, "", ""},
		{"https://site.com:foo@proton.me:bar", false, "", ""},
		// Providers that still allow plain IMAP → keep
		{"https://site.com:foo@mail.ru:bar", true, "foo@mail.ru", "bar"},
		{"https://site.com:foo@yandex.ru:bar", true, "foo@yandex.ru", "bar"},
		{"https://site.com:foo@gmx.de:bar", true, "foo@gmx.de", "bar"},
		// Custom / corporate domain → keep
		{"https://site.com:user@corp.example:secret", true, "user@corp.example", "secret"},
		// Defense: not gmail.com just because suffix matches
		{"https://site.com:user@notgmail.com:secret", true, "user@notgmail.com", "secret"},
	}
	p := &processor.UrlUserPassExtractor{}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			rec, keep, err := p.Process([]byte(tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.wantKeep, keep)
			if tc.wantKeep {
				assert.Equal(t, tc.wantEmail, string(rec.Email))
				assert.Equal(t, tc.wantPass, string(rec.Pass))
			}
		})
	}
}
