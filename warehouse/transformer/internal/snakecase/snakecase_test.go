package snakecase

import (
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

var burredLetters = []rune{
	// Latin-1 Supplement letters.
	'\xc0', '\xc1', '\xc2', '\xc3', '\xc4', '\xc5', '\xc6', '\xc7',
	'\xc8', '\xc9', '\xca', '\xcb', '\xcc', '\xcd', '\xce', '\xcf',
	'\xd0', '\xd1', '\xd2', '\xd3', '\xd4', '\xd5', '\xd6', '\xd8',
	'\xd9', '\xda', '\xdb', '\xdc', '\xdd', '\xde', '\xdf', '\xe0',
	'\xe1', '\xe2', '\xe3', '\xe4', '\xe5', '\xe6', '\xe7', '\xe8',
	'\xe9', '\xea', '\xeb', '\xec', '\xed', '\xee', '\xef', '\xf0',
	'\xf1', '\xf2', '\xf3', '\xf4', '\xf5', '\xf6', '\xf8', '\xf9',
	'\xfa', '\xfb', '\xfc', '\xfd', '\xfe', '\xff',

	// Latin Extended-A letters.
	'\u0100', '\u0101', '\u0102', '\u0103', '\u0104', '\u0105', '\u0106', '\u0107',
	'\u0108', '\u0109', '\u010a', '\u010b', '\u010c', '\u010d', '\u010e', '\u010f',
	'\u0110', '\u0111', '\u0112', '\u0113', '\u0114', '\u0115', '\u0116', '\u0117',
	'\u0118', '\u0119', '\u011a', '\u011b', '\u011c', '\u011d', '\u011e', '\u011f',
	'\u0120', '\u0121', '\u0122', '\u0123', '\u0124', '\u0125', '\u0126', '\u0127',
	'\u0128', '\u0129', '\u012a', '\u012b', '\u012c', '\u012d', '\u012e', '\u012f',
	'\u0130', '\u0131', '\u0132', '\u0133', '\u0134', '\u0135', '\u0136', '\u0137',
	'\u0138', '\u0139', '\u013a', '\u013b', '\u013c', '\u013d', '\u013e', '\u013f',
	'\u0140', '\u0141', '\u0142', '\u0143', '\u0144', '\u0145', '\u0146', '\u0147',
	'\u0148', '\u0149', '\u014a', '\u014b', '\u014c', '\u014d', '\u014e', '\u014f',
	'\u0150', '\u0151', '\u0152', '\u0153', '\u0154', '\u0155', '\u0156', '\u0157',
	'\u0158', '\u0159', '\u015a', '\u015b', '\u015c', '\u015d', '\u015e', '\u015f',
	'\u0160', '\u0161', '\u0162', '\u0163', '\u0164', '\u0165', '\u0166', '\u0167',
	'\u0168', '\u0169', '\u016a', '\u016b', '\u016c', '\u016d', '\u016e', '\u016f',
	'\u0170', '\u0171', '\u0172', '\u0173', '\u0174', '\u0175', '\u0176', '\u0177',
	'\u0178', '\u0179', '\u017a', '\u017b', '\u017c', '\u017d', '\u017e', '\u017f',
}

func TestToSnakeCase(t *testing.T) {
	t.Run("extractWords", func(t *testing.T) {
		t.Run("should match words containing Latin Unicode letters", func(t *testing.T) {
			for _, letter := range burredLetters {
				require.Equal(t, []string{string(letter)}, extractWords(string(letter)))
			}
		})
		t.Run("should work with compound words", func(t *testing.T) {
			require.Equal(t, []string{"12", "ft"}, extractWords("12ft"))
			require.Equal(t, []string{"aeiou", "Are", "Vowels"}, extractWords("aeiouAreVowels"))
			require.Equal(t, []string{"enable", "6", "h", "format"}, extractWords("enable 6h format"))
			require.Equal(t, []string{"enable", "24", "H", "format"}, extractWords("enable 24H format"))
			require.Equal(t, []string{"is", "ISO", "8601"}, extractWords("isISO8601"))
			require.Equal(t, []string{"LETTERS", "Aeiou", "Are", "Vowels"}, extractWords("LETTERSAeiouAreVowels"))
			require.Equal(t, []string{"too", "Legit", "2", "Quit"}, extractWords("tooLegit2Quit"))
			require.Equal(t, []string{"walk", "500", "Miles"}, extractWords("walk500Miles"))
			require.Equal(t, []string{"xhr", "2", "Request"}, extractWords("xhr2Request"))
			require.Equal(t, []string{"XML", "Http"}, extractWords("XMLHttp"))
			require.Equal(t, []string{"Xml", "HTTP"}, extractWords("XmlHTTP"))
			require.Equal(t, []string{"Xml", "Http"}, extractWords("XmlHttp"))
		})
		t.Run("should work with compound words containing diacritical marks", func(t *testing.T) {
			require.Equal(t, []string{"LETTERS", "Æiou", "Are", "Vowels"}, extractWords("LETTERSÆiouAreVowels"))
			require.Equal(t, []string{"æiou", "Are", "Vowels"}, extractWords("æiouAreVowels"))
			require.Equal(t, []string{"æiou", "2", "Consonants"}, extractWords("æiou2Consonants"))
		})
		t.Run("should not treat contractions as separate words", func(t *testing.T) {
			for _, apos := range []string{"'", string('\u2019')} {
				t.Run("ToLower", func(t *testing.T) {
					for _, postfix := range []string{"d", "ll", "m", "re", "s", "t", "ve"} {
						input := "a b" + apos + postfix + " c"
						actual := extractWords(strings.ToLower(input))
						expected := lo.Map([]string{"a", "b" + apos + postfix, "c"}, func(item string, index int) string {
							return strings.ToLower(item)
						})
						require.Equal(t, expected, actual)
					}
				})
				t.Run("ToUpper", func(t *testing.T) {
					for _, postfix := range []string{"d", "ll", "m", "re", "s", "t", "ve"} {
						input := "a b" + apos + postfix + " c"
						actual := extractWords(strings.ToUpper(input))
						expected := lo.Map([]string{"a", "b" + apos + postfix, "c"}, func(item string, index int) string {
							return strings.ToUpper(item)
						})
						require.Equal(t, expected, actual)
					}
				})
			}
		})
		t.Run("should not treat ordinal numbers as separate words", func(t *testing.T) {
			ordinals := []string{"1st", "2nd", "3rd", "4th"}
			for _, ordinal := range ordinals {
				expected := []string{strings.ToLower(ordinal)}
				actual := extractWords(strings.ToLower(ordinal))
				require.Equal(t, expected, actual)

				expected = []string{strings.ToUpper(ordinal)}
				actual = extractWords(strings.ToUpper(ordinal))
				require.Equal(t, expected, actual)
			}
		})
		t.Run("should prevent ReDoS", func(t *testing.T) {
			largeWordLen := 50000
			largeWord := strings.Repeat("A", largeWordLen)
			maxMs := 1000
			startTime := time.Now()

			expected := []string{largeWord, "Æiou", "Are", "Vowels"}
			actual := extractWords(largeWord + "ÆiouAreVowels")
			require.Equal(t, expected, actual)

			endTime := time.Now()
			timeSpent := endTime.Sub(startTime)
			require.Less(t, timeSpent.Milliseconds(), int64(maxMs))
		})
	})
}
