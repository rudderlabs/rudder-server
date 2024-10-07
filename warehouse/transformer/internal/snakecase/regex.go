package snakecase

import (
	"strings"

	"github.com/dlclark/regexp2"
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
