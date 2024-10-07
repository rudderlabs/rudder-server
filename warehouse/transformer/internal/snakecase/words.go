package snakecase

import (
	"github.com/dlclark/regexp2"
)

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
