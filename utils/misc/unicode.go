package misc

import (
	"strings"
	"unicode"

	"golang.org/x/text/unicode/rangetable"
)

// unicode.IsPrint does not include all invisible characters,
// so I got this list from https://invisible-characters.com/
var InvisibleRunes = []rune{
	'\u0000', // NULL
	'\u0009', // CHARACTER TABULATION
	// '\u0020', // SPACE <- this is not trimmed
	'\u00A0', // NO-BREAK SPACE
	'\u00AD', // SOFT HYPHEN
	'\u034F', // COMBINING GRAPHEME JOINER
	'\u061C', // ARABIC LETTER MARK
	'\u115F', // HANGUL CHOSEONG FILLER
	'\u1160', // HANGUL JUNGSEONG FILLER
	'\u17B4', // KHMER VOWEL INHERENT AQ
	'\u17B5', // KHMER VOWEL INHERENT AA
	'\u180E', // MONGOLIAN VOWEL SEPARATOR
	'\u2000', // EN QUAD
	'\u2001', // EM QUAD
	'\u2002', // EN SPACE
	'\u2003', // EM SPACE
	'\u2004', // THREE-PER-EM SPACE
	'\u2005', // FOUR-PER-EM SPACE
	'\u2006', // SIX-PER-EM SPACE
	'\u2007', // FIGURE SPACE
	'\u2008', // PUNCTUATION SPACE
	'\u2009', // THIN SPACE
	'\u200A', // HAIR SPACE
	'\u200B', // ZERO WIDTH SPACE
	'\u200C', // ZERO WIDTH NON-JOINER
	'\u200D', // ZERO WIDTH JOINER
	'\u200E', // LEFT-TO-RIGHT MARK
	'\u200F', // RIGHT-TO-LEFT MARK
	'\u202F', // NARROW NO-BREAK SPACE
	'\u205F', // MEDIUM MATHEMATICAL SPACE
	'\u2060', // WORD JOINER
	'\u2061', // FUNCTION APPLICATION
	'\u2062', // INVISIBLE TIMES
	'\u2063', // INVISIBLE SEPARATOR
	'\u2064', // INVISIBLE PLUS
	'\u206A', // INHIBIT SYMMETRIC SWAPPING
	'\u206B', // ACTIVATE SYMMETRIC SWAPPING
	'\u206C', // INHIBIT ARABIC FORM SHAPING
	'\u206D', // ACTIVATE ARABIC FORM SHAPING
	'\u206E', // NATIONAL DIGIT SHAPES
	'\u206F', // NOMINAL DIGIT SHAPES
	'\u3000', // IDEOGRAPHIC SPACE
	'\u2800', // BRAILLE PATTERN BLANK
	'\u3164', // HANGUL FILLER
	'\uFEFF', // ZERO WIDTH NO-BREAK SPACE
	'\uFFA0', // HALFWIDTH HANGUL FILLER
}

var invisibleRangeTable *unicode.RangeTable

func init() {
	invisibleRangeTable = rangetable.New(InvisibleRunes...)
}

// SanitizeUnicode removes irregularly invisible characters from a string.
//
//	 Non-printable characters as defined in Go's unicode package (unicode.IsPrint),
//		 plus characters in the InvisibleRunes list (https://invisible-characters.com/).
//
// Note: regular ascii space (0x20) is not removed.
func SanitizeUnicode(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.Is(invisibleRangeTable, r) {
			return -1
		}

		if !unicode.IsPrint(r) {
			return -1
		}

		return r
	}, str)
}
