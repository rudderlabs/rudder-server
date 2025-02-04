package snakecase

import (
	"strings"

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
	return snakeCase(s, extractWords)
}

// ToSnakeCaseWithNumbers converts a string to snake_case, preserving numbers.
func ToSnakeCaseWithNumbers(s string) string {
	return snakeCase(s, extractWordsWithNumbers)
}

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
