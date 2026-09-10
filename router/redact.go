package router

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

// urlInText matches an absolute http(s) URL inside free-form text.
//
// It stops at whitespace and at the quote characters the router's own messages wrap
// URLs in, so a %q-quoted URL is matched without swallowing the closing quote, and a
// URL at the end of a sentence keeps its trailing punctuation outside the match.
var urlInText = regexp.MustCompile(`https?://[^\s"'` + "`" + `<>]+`)

// Credentials are kept out of these messages on three levels, because the URL reaches
// them by more than one route and only some of those routes are known at the call site.
//
// Destination URLs routinely authenticate in the query string (?api_key=...,
// ?access_token=...) and occasionally in userinfo (https://user:pass@host). Any
// message this package composes for a failed request can therefore carry a live
// credential, and those messages do not stay internal: they become the job's error
// response, which the failed-records feature persists verbatim into a table in the
// customer's own warehouse and renders in the UI, where it outlives the request by
// the configured retention.
//
//   - redactURL is the structural pass over a URL the caller HAS. It parses rather
//     than pattern-matches, so it holds for any shape url.Parse accepts — including a
//     scheme-less URL, which a pattern looking for "https://" does not see at all.
//   - redactErrorText is the structural pass over an error's URL. net/http returns
//     *url.Error from Do, and its Error() is `%s %q: %s` over Op, URL and cause, so the
//     URL is embedded a second time inside %v — and that copy is the request URL after
//     the query parameters this package appends, so it is not even the same string as
//     the one the caller interpolated.
//   - redactURLCredentials is the pattern backstop over the composed message, for a URL
//     that arrives in text this package did not build from a value it holds.
//
// The precedent is enterprise/reporting/error_extractor.go, which deletes URLs from
// error messages outright before reporting aggregates them. These keep the endpoint
// because a failed-records row exists to tell an operator which destination rejected
// their data, and "https://api.example.com/v1/events" answers that where a blank does
// not.

// redactURLCredentials removes credentials from every http(s) URL it can find in s.
//
// It is the last line rather than the first: finding a URL by pattern is inherently
// partial — a scheme-less or malformed URL is invisible to it — so a caller holding
// the URL or the error should redact those structurally and leave this to catch what
// it does not know about. Running it over already-redacted text is a no-op, because
// redactURL is idempotent.
func redactURLCredentials(s string) string {
	return urlInText.ReplaceAllStringFunc(s, redactURL)
}

// redactErrorText renders err with the URL that net/http embeds in *url.Error redacted,
// leaving the rest of the message — including any outer wrapping and the underlying
// cause — exactly as it was.
//
// The URL is replaced by substring rather than by reformatting the error so that a
// *url.Error found deeper in a chain does not cost the text wrapped around it, and so
// the cause reads as it always did.
func redactErrorText(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.URL != "" {
		text = strings.ReplaceAll(text, urlErr.URL, redactURL(urlErr.URL))
	}
	return text
}

// redactedQueryMarker replaces a removed query string. It is deliberately visible: an
// operator reading a failed-records row needs to tell "this endpoint takes no
// parameters" apart from "its parameters were withheld".
const redactedQueryMarker = "?[redacted]"

// redactURL strips userinfo, query and fragment from one URL. A scheme is not
// required: url.Parse reads a scheme-less URL as a path with a query, which is exactly
// the case a pattern match misses, so this is what callers holding a URL should use.
//
// It is idempotent — the marker it appends parses back as a query and is removed and
// re-appended unchanged — so layering it under redactURLCredentials is safe.
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
