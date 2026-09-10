package router

import (
	"net/url"
	"regexp"
)

// urlInText matches an absolute http(s) URL inside free-form text.
//
// It stops at whitespace and at the quote characters the router's own messages wrap
// URLs in, so a %q-quoted URL is matched without swallowing the closing quote, and a
// URL at the end of a sentence keeps its trailing punctuation outside the match.
var urlInText = regexp.MustCompile(`https?://[^\s"'` + "`" + `<>]+`)

// redactURLCredentials removes credentials from every http(s) URL in s, keeping the
// scheme, host and path so the message still says which endpoint failed.
//
// Destination URLs routinely authenticate in the query string (?api_key=...,
// ?access_token=...) and occasionally in userinfo (https://user:pass@host). Any
// message this package composes for a failed request can therefore carry a live
// credential, and those messages do not stay internal: they become the job's error
// response, which the failed-records feature persists verbatim into a table in the
// customer's own warehouse and renders in the UI, where it outlives the request by
// the configured retention.
//
// It is applied to the COMPOSED message rather than to the URL alone because the URL
// reaches the text by two routes. One is the explicit interpolation. The other is the
// error value: net/http returns *url.Error from Do, whose Error() is
// `%s %q: %s` over Op, URL and the cause, so the URL is embedded again inside %v — and
// that copy is the request URL after the query parameters this package appends, so it
// is not even the same string as the interpolated one. Redacting the inputs separately
// would leave that second copy behind.
//
// The precedent is enterprise/reporting/error_extractor.go, which deletes URLs from
// error messages outright before reporting aggregates them. This keeps the endpoint
// because a failed-records row exists to tell an operator which destination rejected
// their data, and "https://api.example.com/v1/events" answers that where a blank does
// not.
func redactURLCredentials(s string) string {
	return urlInText.ReplaceAllStringFunc(s, redactURL)
}

// redactedQueryMarker replaces a removed query string. It is deliberately visible: an
// operator reading a failed-records row needs to tell "this endpoint takes no
// parameters" apart from "its parameters were withheld".
const redactedQueryMarker = "?[redacted]"

// redactURL strips userinfo, query and fragment from one URL.
//
// An unparseable URL is replaced wholesale rather than passed through: this function
// cannot show that such a string is credential-free, and the safe failure for a value
// that is about to be written to a customer's warehouse is to withhold it.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "[redacted url]"
	}
	// Fragments are dropped alongside the query: the OAuth implicit flow returns
	// access tokens there, and nothing in a destination URL needs one.
	hadSecrets := u.RawQuery != "" || u.User != nil || u.Fragment != ""
	u.RawQuery = ""
	u.User = nil
	u.Fragment = ""
	if !hadSecrets {
		return u.String()
	}
	return u.String() + redactedQueryMarker
}
