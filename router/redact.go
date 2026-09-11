package router

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

const (
	// urlStopChars cannot appear anywhere in a URL matched in free-form text:
	// whitespace, and the quote characters these messages wrap URLs in.
	urlStopChars = `\s"'` + "`" + `<>`
	// urlTrailingPunctuation additionally cannot be the LAST character of a match, so a
	// URL ending a sentence or a parenthetical does not swallow the punctuation.
	// Square and curly brackets are absent deliberately: redactedQueryMarker ends in
	// `]`, and excluding it here would truncate an already-redacted URL and append a
	// second marker.
	urlTrailingPunctuation = `.,;:!?)`
)

// urlInText matches an absolute http(s) URL inside free-form text.
var urlInText = regexp.MustCompile(
	`https?://[^` + urlStopChars + `]*[^` + urlStopChars + urlTrailingPunctuation + `]`)

// Credentials in a destination URL must not reach these messages: they become the job's
// error response, which the failed-records feature stores and displays. Three passes,
// most reliable first:
//
//   - redactURL, for a URL the caller holds.
//   - redactErrorText, for the URL net/http embeds in *url.Error.
//   - redactURLCredentials, a pattern backstop over the composed message.

// redactURLCredentials redacts every http(s) URL it can find in s.
//
// Pattern matching is partial — a scheme-less URL is invisible to it — so a caller
// holding a URL or an error should use redactURL or redactErrorText and leave this as a
// backstop.
func redactURLCredentials(s string) string {
	return urlInText.ReplaceAllStringFunc(s, redactURL)
}

// redactErrorText returns err.Error() with the URL that *url.Error carries redacted,
// leaving any outer wrapping and the underlying cause unchanged.
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

// redactedQueryMarker replaces a removed query string, so that a withheld query is
// distinguishable from no query at all.
const redactedQueryMarker = "?[redacted]"

// redactURL strips userinfo, query and fragment from raw, keeping scheme, host and path.
// A scheme is not required. It is idempotent, so it may be layered under
// redactURLCredentials.
//
// An unparseable URL is replaced wholesale with the [redacted url] literal.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "[redacted url]"
	}
	// The fragment goes with the query: the OAuth implicit flow returns access tokens
	// there.
	hadSecrets := u.RawQuery != "" || u.User != nil || u.Fragment != ""
	u.RawQuery = ""
	u.User = nil
	u.Fragment = ""
	if !hadSecrets {
		return u.String()
	}
	return u.String() + redactedQueryMarker
}
