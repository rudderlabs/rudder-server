package snakecase

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
