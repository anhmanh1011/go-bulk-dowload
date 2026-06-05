package processor_test

import (
	"testing"

	"github.com/manh/tgpipe/internal/processor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMicrosoftOnlyExtractor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in        string
		wantKeep  bool
		wantEmail string
		wantPass  string
	}{
		// Microsoft consumer → keep
		{"https://site.com:user@outlook.com:pass123", true, "user@outlook.com", "pass123"},
		{"https://site.com:foo@hotmail.co.uk:bar", true, "foo@hotmail.co.uk", "bar"},
		{"https://site.com:foo@LIVE.com:bar", true, "foo@LIVE.com", "bar"}, // case-insensitive domain
		{"user@msn.com:secret", true, "user@msn.com", "secret"},
		{"http://a.b.c:foo@outlook.com:p:a:ss", true, "foo@outlook.com", "p:a:ss"}, // colons in pass
		// Non-Microsoft → drop
		{"https://site.com:user@gmail.com:pass", false, "", ""},
		{"https://site.com:foo@yahoo.com:bar", false, "", ""},
		{"https://site.com:user@corp.example:secret", false, "", ""},
		{"https://site.com:user@notoutlook.com:secret", false, "", ""}, // suffix must not match
		// Malformed → drop
		{"", false, "", ""},
		{"abc", false, "", ""},
		{"https://site:user@outlook.com:", false, "", ""},  // empty pass
		{"https://site.com:johnsmith:1234", false, "", ""}, // user not email
	}
	p := &processor.MicrosoftOnlyExtractor{}
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
