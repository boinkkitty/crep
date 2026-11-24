package cassandra

import "testing"

func TestArchivedURLDropsCredentials(t *testing.T) {
	cases := map[string]string{
		"https://user:pw@example.com/hook?token=abc#frag": "https://example.com/hook",
		"https://example.com:8443/a/b?":                   "https://example.com:8443/a/b",
		"http://example.com":                              "http://example.com",
		"://bad":                                          "",
	}
	for in, want := range cases {
		if got := archivedURL(in); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}
