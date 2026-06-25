package snakecase

import (
	"strings"
	"unicode/utf8"

	"github.com/dlclark/regexp2"
	"github.com/samber/lo"
)

const (
	// Used to compose unicode character classes.
	astralRange               = "\\ud800-\\udfff"
	comboMarksRange           = "\\u0300-\\u036f"
	comboHalfMarksRange       = "\\ufe20-\\ufe2f"
	comboSymbolsRange         = "\\u20d0-\\u20ff"
	comboMarksExtendedRange   = "\\u1ab0-\\u1aff"
	comboMarksSupplementRange = "\\u1dc0-\\u1dff"
	comboRange                = comboMarksRange + comboHalfMarksRange + comboSymbolsRange + comboMarksExtendedRange + comboMarksSupplementRange
	dingbatRange              = "\\u2700-\\u27bf"
	lowerRange                = "a-z\\xdf-\\xf6\\xf8-\\xff"
	mathOpRange               = "\\xac\\xb1\\xd7\\xf7"
	nonCharRange              = "\\x00-\\x2f\\x3a-\\x40\\x5b-\\x60\\x7b-\\xbf"
	punctuationRange          = "\\u2000-\\u206f"
	spaceRange                = " \\t\\x0b\\f\\xa0\\ufeff\\n\\r\\u2028\\u2029\\u1680\\u180e\\u2000\\u2001\\u2002\\u2003\\u2004\\u2005\\u2006\\u2007\\u2008\\u2009\\u200a\\u202f\\u205f\\u3000"
	upperRange                = "A-Z\\xc0-\\xd6\\xd8-\\xde"
	varRange                  = "\\ufe0e\\ufe0f"
	breakRange                = mathOpRange + nonCharRange + punctuationRange + spaceRange

	// Used to compose unicode capture groups
	apos      = "['\u2019]"
	breakExp  = "[" + breakRange + "]"
	combo     = "[" + comboRange + "]"
	digit     = "\\d"
	dingbat   = "[" + dingbatRange + "]"
	lower     = "[" + lowerRange + "]"
	misc      = "[^" + astralRange + breakRange + digit + dingbatRange + lowerRange + upperRange + "]"
	fitz      = "\\ud83c[\\udffb-\\udfff]"
	modifier  = "(?:" + combo + "|" + fitz + ")"
	nonAstral = "[^" + astralRange + "]"
	regional  = "(?:\\ud83c[\\udde6-\\uddff]){2}"
	surrPair  = "[\\ud800-\\udbff][\\udc00-\\udfff]"
	upper     = "[" + upperRange + "]"
	zwj       = "\\u200d"

	// Used to compose unicode regexes
	miscLower     = "(?:" + lower + "|" + misc + ")"
	miscUpper     = "(?:" + upper + "|" + misc + ")"
	optContrLower = "(?:" + apos + "(?:d|ll|m|re|s|t|ve))?"
	optContrUpper = "(?:" + apos + "(?:D|LL|M|RE|S|T|VE))?"
	optMod        = modifier + "?"
	optVar        = "[" + varRange + "]?"
	optJoin       = "(?:" + zwj + "(?:" + nonAstral + "|" + regional + "|" + surrPair + ")" + optVar + optMod + ")*"
	ordLower      = "\\d*(?:1st|2nd|3rd|(?![123])\\dth)(?=\\b|[A-Z_])"
	ordUpper      = "\\d*(?:1ST|2ND|3RD|(?![123])\\dTH)(?=\\b|[a-z_])"
	seq           = optVar + optMod + optJoin
	emoji         = "(?:" + dingbat + "|" + regional + "|" + surrPair + ")" + seq
)

var (
	reUnicodeWords = regexp2.MustCompile(
		strings.Join(
			[]string{
				upper + "?" + lower + "+" + optContrLower + "(?=" + breakExp + "|" + upper + "|" + "$)",   // Regular words, lowercase letters followed by optional contractions
				miscUpper + "+" + optContrUpper + "(?=" + breakExp + "|" + upper + miscLower + "|" + "$)", // Miscellaneous uppercase characters with optional contractions
				upper + "?" + miscLower + "+" + optContrLower,                                             // Miscellaneous lowercase sequences with optional contractions
				upper + "+" + optContrUpper, // All uppercase words with optional contractions (e.g., "THIS")
				ordUpper,                    // Ordinals for uppercase (e.g., "1ST", "2ND")
				ordLower,                    // Ordinals for lowercase (e.g., "1st", "2nd")
				digit + "+",                 // Pure digits (e.g., "123")
				emoji,                       // Emojis (e.g., 😀, ❤️)
			},
			"|",
		),
		regexp2.None,
	)
	reUnicodeWordsWithNumbers = regexp2.MustCompile(
		strings.Join(
			[]string{
				upper + "?" + lower + "+" + digit + "+", // Lowercase letters followed by digits (e.g., "abc123")
				upper + "+" + digit + "+",               // Uppercase letters followed by digits (e.g., "ABC123")
				digit + "+" + upper + "?" + lower + "+", // Digits followed by lowercase letters (e.g., "123abc")
				digit + "+" + upper + "+",               // Digits followed by uppercase letters (e.g., "123ABC")
				upper + "?" + lower + "+" + optContrLower + "(?=" + breakExp + "|" + upper + "|" + "$)",   // Regular words, lowercase letters followed by optional contractions
				miscUpper + "+" + optContrUpper + "(?=" + breakExp + "|" + upper + miscLower + "|" + "$)", // Miscellaneous uppercase characters with optional contractions
				upper + "?" + miscLower + "+" + optContrLower,                                             // Miscellaneous lowercase sequences with optional contractions
				upper + "+" + optContrUpper, // All uppercase words with optional contractions (e.g., "THIS")
				ordUpper,                    // Ordinals for uppercase (e.g., "1ST", "2ND")
				ordLower,                    // Ordinals for lowercase (e.g., "1st", "2nd")
				digit + "+",                 // Pure digits (e.g., "123")
				emoji,                       // Emojis (e.g., 😀, ❤️)
			},
			"|",
		),
		regexp2.None,
	)
	reAsciiWord      = regexp2.MustCompile(`[^\x00-\x2f\x3a-\x40\x5b-\x60\x7b-\x7f]+`, regexp2.None)
	reHasUnicodeWord = regexp2.MustCompile(
		`[a-z][A-Z]|[A-Z]{2}[a-z]|[0-9][a-zA-Z]|[a-zA-Z][0-9]|[^a-zA-Z0-9 ]`, regexp2.None,
	)
)

// ToSnakeCase converts a string to snake_case using regular word separation.
func ToSnakeCase(s string) string {
	if out, ok := fastASCIISnakeCase(s, false); ok {
		return out
	}
	return snakeCase(s, extractWords)
}

// ToSnakeCaseWithNumbers converts a string to snake_case, preserving numbers.
func ToSnakeCaseWithNumbers(s string) string {
	if out, ok := fastASCIISnakeCase(s, true); ok {
		return out
	}
	return snakeCase(s, extractWordsWithNumbers)
}

// fastASCIISnakeCase is an allocation-light fast path for the common case of
// column names that consist of plain ASCII letters, digits and separators. It
// returns (result, true) when it fully handles the input, and ("", false) when
// the input must be deferred to the regexp2-backed reference implementation.
//
// It deliberately bails out (ok == false) on:
//   - any non-ASCII byte: unicode word splitting (combining marks, emoji,
//     diacritics) is intricate and left to the reference implementation;
//   - the apostrophe ' (U+0027): contraction stripping is left to the
//     reference implementation (the curly apostrophe U+2019 is non-ASCII and
//     is already covered by the first rule).
//
// For the inputs it does handle, it reproduces the exact word boundaries of the
// regexp2 alternations: it walks the string finding the leftmost match of the
// ordered alternatives (matchWord), emitting each matched word lowercased and
// underscore-separated, and skipping unmatched separators. The withNumbers flag
// selects between the ToSnakeCase and ToSnakeCaseWithNumbers alternation sets,
// which differ only in how letters and digits are grouped. Correctness is
// proven byte-for-byte against the frozen regexp2 implementation by the
// differential test harness (differential_test.go).
func fastASCIISnakeCase(s string, withNumbers bool) (string, bool) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= utf8.RuneSelf || c == '\'' {
			return "", false
		}
	}

	var b strings.Builder
	b.Grow(len(s) + 8)
	wroteWord := false

	for p := 0; p < len(s); {
		end := matchWord(s, p, withNumbers)
		if end <= p { // separator / no match: skip one byte
			p++
			continue
		}
		if wroteWord {
			b.WriteByte('_')
		}
		for k := p; k < end; k++ {
			c := s[k]
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			b.WriteByte(c)
		}
		wroteWord = true
		p = end
	}
	return b.String(), true
}

// matchWord returns the end index of the leftmost word starting at p, trying the
// regexp2 alternatives in their exact order, or p if none match. The order
// mirrors reUnicodeWords / reUnicodeWordsWithNumbers reduced to ASCII (where the
// unicode "misc" class is empty and contractions never appear because we bail on
// apostrophes).
func matchWord(s string, p int, withNumbers bool) int {
	if withNumbers {
		// [A-Z]?[a-z]+\d+ | [A-Z]+\d+ | \d+[A-Z]?[a-z]+ | \d+[A-Z]+
		if e := matchLowerDigits(s, p); e > p {
			return e
		}
		if e := matchUpperDigits(s, p); e > p {
			return e
		}
		if e := matchDigitsLower(s, p); e > p {
			return e
		}
		if e := matchDigitsUpper(s, p); e > p {
			return e
		}
	}
	if e := matchRegularLower(s, p); e > p { // [A-Z]?[a-z]+ (?=break|[A-Z]|$)
		return e
	}
	if e := matchUpperRun(s, p); e > p { // [A-Z]+ (?=break|[A-Z][a-z]|$)
		return e
	}
	if e := matchLowerWord(s, p); e > p { // [A-Z]?[a-z]+
		return e
	}
	if e := matchUpperWord(s, p); e > p { // [A-Z]+
		return e
	}
	if e := matchOrdinal(s, p, true); e > p { // ordUpper
		return e
	}
	if e := matchOrdinal(s, p, false); e > p { // ordLower
		return e
	}
	if e := matchDigitRun(s, p); e > p { // \d+
		return e
	}
	return p
}

// matchRegularLower matches [A-Z]?[a-z]+ (?=break|[A-Z]|$): an optional leading
// uppercase, one or more lowercase, that must not be followed by a digit.
func matchRegularLower(s string, p int) int {
	i := p
	if i < len(s) && isUpper(s[i]) && i+1 < len(s) && isLower(s[i+1]) {
		i++
	}
	ls := i
	for i < len(s) && isLower(s[i]) {
		i++
	}
	if i == ls { // no lowercase consumed
		return p
	}
	if i == len(s) || isSep(s[i]) || isUpper(s[i]) { // break | [A-Z] | $
		return i
	}
	return p // followed by a digit: lookahead fails
}

// matchUpperRun matches [A-Z]+ (?=break|[A-Z][a-z]|$): a run of uppercase that
// stops before an uppercase letter which itself precedes a lowercase letter
// (the XMLHttp -> XML, Http rule), or at a separator / end of string.
func matchUpperRun(s string, p int) int {
	i := p
	for i < len(s) && isUpper(s[i]) {
		i++
	}
	if i == p {
		return p
	}
	for end := i; end > p; end-- { // backtrack to satisfy the lookahead
		if end == len(s) || isSep(s[end]) {
			return end // $ | break (note: a digit is not a break)
		}
		if isUpper(s[end]) && end+1 < len(s) && isLower(s[end+1]) {
			return end // [A-Z][a-z]
		}
	}
	return p
}

// matchLowerWord matches [A-Z]?[a-z]+ with no lookahead.
func matchLowerWord(s string, p int) int {
	i := p
	if i < len(s) && isUpper(s[i]) && i+1 < len(s) && isLower(s[i+1]) {
		i++
	}
	ls := i
	for i < len(s) && isLower(s[i]) {
		i++
	}
	if i == ls {
		return p
	}
	return i
}

// matchUpperWord matches [A-Z]+ with no lookahead.
func matchUpperWord(s string, p int) int {
	i := p
	for i < len(s) && isUpper(s[i]) {
		i++
	}
	return i
}

// matchLowerDigits matches [A-Z]?[a-z]+\d+ (e.g. "walk500", "Abc123").
func matchLowerDigits(s string, p int) int {
	i := p
	if i < len(s) && isUpper(s[i]) && i+1 < len(s) && isLower(s[i+1]) {
		i++
	}
	ls := i
	for i < len(s) && isLower(s[i]) {
		i++
	}
	if i == ls {
		return p
	}
	ds := i
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	if i == ds {
		return p
	}
	return i
}

// matchUpperDigits matches [A-Z]+\d+ (e.g. "ABC123").
func matchUpperDigits(s string, p int) int {
	i := p
	for i < len(s) && isUpper(s[i]) {
		i++
	}
	if i == p {
		return p
	}
	ds := i
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	if i == ds {
		return p
	}
	return i
}

// matchDigitsLower matches \d+[A-Z]?[a-z]+ (e.g. "123abc", "1stPlace").
func matchDigitsLower(s string, p int) int {
	i := p
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	if i == p {
		return p
	}
	if i < len(s) && isUpper(s[i]) && i+1 < len(s) && isLower(s[i+1]) {
		i++
	}
	ls := i
	for i < len(s) && isLower(s[i]) {
		i++
	}
	if i == ls {
		return p
	}
	return i
}

// matchDigitsUpper matches \d+[A-Z]+ (e.g. "123ABC").
func matchDigitsUpper(s string, p int) int {
	i := p
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	if i == p {
		return p
	}
	us := i
	for i < len(s) && isUpper(s[i]) {
		i++
	}
	if i == us {
		return p
	}
	return i
}

// matchDigitRun matches \d+.
func matchDigitRun(s string, p int) int {
	i := p
	for i < len(s) && isDigit(s[i]) {
		i++
	}
	return i
}

// matchOrdinal matches the ordinal alternatives:
//
//	ordUpper: \d*(?:1ST|2ND|3RD|(?![123])\dTH)(?=\b|[a-z_])
//	ordLower: \d*(?:1st|2nd|3rd|(?![123])\dth)(?=\b|[A-Z_])
//
// i.e. a run of digits whose final digit, together with the two letters that
// follow, forms a valid ordinal suffix (st/nd/rd/th in the requested case),
// provided the suffix is not immediately followed by a same-case letter or a
// digit.
func matchOrdinal(s string, p int, upper bool) int {
	if p >= len(s) || !isDigit(s[p]) {
		return p
	}
	d := p
	for d < len(s) && isDigit(s[d]) {
		d++
	}
	if d+2 > len(s) { // need two trailing suffix letters
		return p
	}
	a, b := s[d], s[d+1]
	var ok bool
	switch s[d-1] { // the final digit selects the expected suffix
	case '1':
		ok = ordinalSuffix(a, b, 'S', 'T', upper)
	case '2':
		ok = ordinalSuffix(a, b, 'N', 'D', upper)
	case '3':
		ok = ordinalSuffix(a, b, 'R', 'D', upper)
	default: // (?![123])\d -> 0,4-9 take TH
		ok = ordinalSuffix(a, b, 'T', 'H', upper)
	}
	if !ok {
		return p
	}
	end := d + 2
	// lookahead: (?=\b|[a-z_]) for ordUpper, (?=\b|[A-Z_]) for ordLower.
	if end == len(s) || isSep(s[end]) || s[end] == '_' {
		return end
	}
	if upper && isLower(s[end]) {
		return end
	}
	if !upper && isUpper(s[end]) {
		return end
	}
	return p
}

// ordinalSuffix reports whether a,b equal the suffix letters U,V (given as
// uppercase), matched in the requested case.
func ordinalSuffix(a, b, upperU, upperV byte, upper bool) bool {
	if upper {
		return a == upperU && b == upperV
	}
	return a == upperU+('a'-'A') && b == upperV+('a'-'A')
}

func isLower(c byte) bool { return c >= 'a' && c <= 'z' }
func isUpper(c byte) bool { return c >= 'A' && c <= 'Z' }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func isSep(c byte) bool   { return !isLower(c) && !isUpper(c) && !isDigit(c) }

func extractWords(s string) []string {
	if hasUnicodeWord(s) {
		return unicodeWords(s)
	}
	return asciiWords(s)
}

func hasUnicodeWord(s string) bool {
	isMatch, _ := reHasUnicodeWord.MatchString(s)
	return isMatch
}

func extractWordsWithNumbers(s string) []string {
	if hasUnicodeWord(s) {
		return unicodeWordsWithNumbers(s)
	}
	return asciiWords(s)
}

func unicodeWords(s string) []string {
	return regexp2FindAllString(reUnicodeWords, s)
}

func unicodeWordsWithNumbers(s string) []string {
	return regexp2FindAllString(reUnicodeWordsWithNumbers, s)
}

func asciiWords(s string) []string {
	return regexp2FindAllString(reAsciiWord, s)
}

func regexp2FindAllString(re *regexp2.Regexp, s string) []string {
	var matches []string
	m, _ := re.FindStringMatch(s)
	for m != nil {
		matches = append(matches, m.String())
		m, _ = re.FindNextMatch(m)
	}
	return matches
}

// snakeCase converts a string to snake_case based on a word extraction function.
func snakeCase(s string, wordExtractor func(s string) []string) string {
	s = strings.NewReplacer("'", "", "\u2019", "").Replace(s)
	words := wordExtractor(s)
	words = lo.Map(words, func(word string, _ int) string {
		return strings.ToLower(word)
	})
	return strings.Join(words, "_")
}
