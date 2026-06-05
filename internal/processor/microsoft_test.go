package processor

import "testing"

func TestIsMicrosoftConsumer(t *testing.T) {
	t.Parallel()
	cases := []struct {
		email string
		want  bool
	}{
		{"foo@outlook.com", true},
		{"foo@hotmail.co.uk", true},
		{"foo@live.com", true},
		{"foo@msn.com", true},
		{"foo@windowslive.com", true},
		{"FOO@OUTLOOK.COM", true}, // case-insensitive
		// Regional variants — must match by prefix, not an enumerated TLD list.
		// These real-combolist domains were missed by the old exact-match set.
		{"foo@outlook.sa", true},
		{"foo@outlook.cz", true},
		{"foo@hotmail.be", true},
		{"foo@live.com.pt", true},
		{"foo@gmail.com", false},      // non-MS provider
		{"foo@corp.example", false},   // corporate domain
		{"foo@notoutlook.com", false}, // leading-label guard: must not false-match
		{"foo@olive.com", false},      // "live." prefix must not match "olive.com"
		{"foo@drive.google.com", false},
		{"not-an-email", false},
	}
	for _, tc := range cases {
		if got := isMicrosoftConsumer([]byte(tc.email)); got != tc.want {
			t.Errorf("isMicrosoftConsumer(%q) = %v, want %v", tc.email, got, tc.want)
		}
	}
}
