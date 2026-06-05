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
		{"FOO@OUTLOOK.COM", true},     // case-insensitive
		{"foo@gmail.com", false},      // non-MS provider
		{"foo@corp.example", false},   // corporate domain
		{"foo@notoutlook.com", false}, // suffix must not false-match
		{"not-an-email", false},
	}
	for _, tc := range cases {
		if got := isMicrosoftConsumer([]byte(tc.email)); got != tc.want {
			t.Errorf("isMicrosoftConsumer(%q) = %v, want %v", tc.email, got, tc.want)
		}
	}
}
