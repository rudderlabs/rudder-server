package snakecase

import (
	"strings"

	"github.com/dlclark/regexp2"
	"github.com/samber/lo"
)

const (
	// Used to compose unicode character classes.
	rsAstralRange               = "\\ud800-\\udfff"
	rsComboMarksRange           = "\\u0300-\\u036f"
	reComboHalfMarksRange       = "\\ufe20-\\ufe2f"
	rsComboSymbolsRange         = "\\u20d0-\\u20ff"
	rsComboMarksExtendedRange   = "\\u1ab0-\\u1aff"
	rsComboMarksSupplementRange = "\\u1dc0-\\u1dff"
	rsComboRange                = rsComboMarksRange + reComboHalfMarksRange + rsComboSymbolsRange + rsComboMarksExtendedRange + rsComboMarksSupplementRange
	rsDingbatRange              = "\\u2700-\\u27bf"
	rsLowerRange                = "a-z\\xdf-\\xf6\\xf8-\\xff"
	rsMathOpRange               = "\\xac\\xb1\\xd7\\xf7"
	rsNonCharRange              = "\\x00-\\x2f\\x3a-\\x40\\x5b-\\x60\\x7b-\\xbf"
	rsPunctuationRange          = "\\u2000-\\u206f"
	rsSpaceRange                = " \\t\\x0b\\f\\xa0\\ufeff\\n\\r\\u2028\\u2029\\u1680\\u180e\\u2000\\u2001\\u2002\\u2003\\u2004\\u2005\\u2006\\u2007\\u2008\\u2009\\u200a\\u202f\\u205f\\u3000"
	rsUpperRange                = "A-Z\\xc0-\\xd6\\xd8-\\xde"
	rsVarRange                  = "\\ufe0e\\ufe0f"
	rsBreakRange                = rsMathOpRange + rsNonCharRange + rsPunctuationRange + rsSpaceRange

	// Used to compose unicode capture groups
	rsApos      = "['\u2019]"
	rsBreak     = "[" + rsBreakRange + "]"
	rsCombo     = "[" + rsComboRange + "]"
	rsDigit     = "\\d"
	rsDingbat   = "[" + rsDingbatRange + "]"
	rsLower     = "[" + rsLowerRange + "]"
	rsMisc      = "[^" + rsAstralRange + rsBreakRange + rsDigit + rsDingbatRange + rsLowerRange + rsUpperRange + "]"
	rsFitz      = "\\ud83c[\\udffb-\\udfff]"
	rsModifier  = "(?:" + rsCombo + "|" + rsFitz + ")"
	rsNonAstral = "[^" + rsAstralRange + "]"
	rsRegional  = "(?:\\ud83c[\\udde6-\\uddff]){2}"
	rsSurrPair  = "[\\ud800-\\udbff][\\udc00-\\udfff]"
	rsUpper     = "[" + rsUpperRange + "]"
	rsZWJ       = "\\u200d"

	// Used to compose unicode regexes
	rsMiscLower     = "(?:" + rsLower + "|" + rsMisc + ")"
	rsMiscUpper     = "(?:" + rsUpper + "|" + rsMisc + ")"
	rsOptContrLower = "(?:" + rsApos + "(?:d|ll|m|re|s|t|ve))?"
	rsOptContrUpper = "(?:" + rsApos + "(?:D|LL|M|RE|S|T|VE))?"
	reOptMod        = rsModifier + "?"
	rsOptVar        = "[" + rsVarRange + "]?"
	rsOptJoin       = "(?:" + rsZWJ + "(?:" + rsNonAstral + "|" + rsRegional + "|" + rsSurrPair + ")" + rsOptVar + reOptMod + ")*"
	rsOrdLower      = "\\d*(?:1st|2nd|3rd|(?![123])\\dth)(?=\\b|[A-Z_])"
	rsOrdUpper      = "\\d*(?:1ST|2ND|3RD|(?![123])\\dTH)(?=\\b|[a-z_])"
	rsSeq           = rsOptVar + reOptMod + rsOptJoin
	rsEmoji         = "(?:" + rsDingbat + "|" + rsRegional + "|" + rsSurrPair + ")" + rsSeq
)

var (
	reUnicodeWords = regexp2.MustCompile(
		strings.Join(
			[]string{
				rsUpper + "?" + rsLower + "+" + rsOptContrLower + "(?=" + rsBreak + "|" + rsUpper + "|" + "$)",   // Regular words, lowercase letters followed by optional contractions
				rsMiscUpper + "+" + rsOptContrUpper + "(?=" + rsBreak + "|" + rsUpper + rsMiscLower + "|" + "$)", // Miscellaneous uppercase characters with optional contractions
				rsUpper + "?" + rsMiscLower + "+" + rsOptContrLower,                                              // Miscellaneous lowercase sequences with optional contractions
				rsUpper + "+" + rsOptContrUpper,                                                                  // All uppercase words with optional contractions (e.g., "THIS")
				rsOrdUpper,                                                                                       // Ordinals for uppercase (e.g., "1ST", "2ND")
				rsOrdLower,                                                                                       // Ordinals for lowercase (e.g., "1st", "2nd")
				rsDigit + "+",                                                                                    // Pure digits (e.g., "123")
				rsEmoji,                                                                                          // Emojis (e.g., 😀, ❤️)
			},
			"|",
		),
		regexp2.None,
	)
	reUnicodeWordsWithNumbers = regexp2.MustCompile(
		strings.Join(
			[]string{
				rsUpper + "?" + rsLower + "+" + rsDigit + "+", // Lowercase letters followed by digits (e.g., "abc123")
				rsUpper + "+" + rsDigit + "+",                 // Uppercase letters followed by digits (e.g., "ABC123")
				rsDigit + "+" + rsUpper + "?" + rsLower + "+", // Digits followed by lowercase letters (e.g., "123abc")
				rsDigit + "+" + rsUpper + "+",                 // Digits followed by uppercase letters (e.g., "123ABC")
				rsUpper + "?" + rsLower + "+" + rsOptContrLower + "(?=" + rsBreak + "|" + rsUpper + "|" + "$)",   // Regular words, lowercase letters followed by optional contractions
				rsMiscUpper + "+" + rsOptContrUpper + "(?=" + rsBreak + "|" + rsUpper + rsMiscLower + "|" + "$)", // Miscellaneous uppercase characters with optional contractions
				rsUpper + "?" + rsMiscLower + "+" + rsOptContrLower,                                              // Miscellaneous lowercase sequences with optional contractions
				rsUpper + "+" + rsOptContrUpper,                                                                  // All uppercase words with optional contractions (e.g., "THIS")
				rsOrdUpper,                                                                                       // Ordinals for uppercase (e.g., "1ST", "2ND")
				rsOrdLower,                                                                                       // Ordinals for lowercase (e.g., "1st", "2nd")
				rsDigit + "+",                                                                                    // Pure digits (e.g., "123")
				rsEmoji,                                                                                          // Emojis (e.g., 😀, ❤️)
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
