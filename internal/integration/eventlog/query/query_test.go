package query

import (
	"net/url"
	"strings"
	"testing"
)

func TestSoqlQuery(t *testing.T) {
	soql := MakeSoqlQuery("MyTable", "one")
	if got, want := soql.Build(), "SELECT one FROM MyTable"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	soql = MakeSoqlQuery("MyTable", "one", "two", "three")
	soql.AndWhere("one = Hello")
	soql.OrWhere("two = Bye")
	soql.AndOrWhere("three = 0", "three = 1")
	want := "SELECT one,two,three FROM MyTable WHERE one = Hello OR two = Bye AND ( three = 0 OR three = 1 )"
	if got := soql.Build(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	// Empty clauses must not produce dangling AND/OR.
	soql = MakeSoqlQuery("T", "Id")
	soql.AndWhere("")
	soql.AndWhere("A = 1")
	soql.Tail("ORDER BY Id ASC")
	if got, want := soql.Build(), "SELECT Id FROM T WHERE A = 1 ORDER BY Id ASC"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// Values containing +, &, # or % must survive the round trip to the server.
func TestQueryURLEncodesSpecialCharacters(t *testing.T) {
	soql := MakeSoqlQuery("Contact", "Id", "Phone")
	soql.AndWhere("Phone = '+61 400 000 000'")
	soql.AndWhere("Name LIKE '%smith%'")
	soql.AndWhere("Note = 'a&b#c'")
	built := soql.Build()

	raw := queryURL("https://example.my.salesforce.com/services/data/v64.0/query", built)
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("query URL is not parseable: %v", err)
	}
	got := u.Query().Get("q")
	if got != built {
		t.Errorf("server would receive\n  %q\nwant\n  %q", got, built)
	}
	if u.Fragment != "" {
		t.Errorf("'#' in a value must not become a fragment: %q", u.Fragment)
	}
	if strings.Contains(u.RawQuery, "+61") {
		t.Errorf("a literal + must be percent-encoded, got %q", u.RawQuery)
	}
}

// Only statuses that can never succeed may be permanent: a permanent error
// lets a file be tombstoned and skipped.
func TestHTTPErrorPermanent(t *testing.T) {
	cases := map[int]bool{
		400: true, 404: true, 410: true,
		401: false, // re-login
		403: false, // REQUEST_LIMIT_EXCEEDED resets; permissions can be fixed
		408: false, 429: false, 500: false, 502: false, 503: false,
	}
	for code, want := range cases {
		if got := (&HTTPError{StatusCode: code}).Permanent(); got != want {
			t.Errorf("status %d: Permanent() = %v, want %v", code, got, want)
		}
	}
}
