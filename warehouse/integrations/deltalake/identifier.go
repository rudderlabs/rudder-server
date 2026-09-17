package deltalake

import "strings"

func quoteIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func quoteQualifiedIdentifier(identifiers ...string) string {
	quotedIdentifiers := make([]string, 0, len(identifiers))
	for _, identifier := range identifiers {
		quotedIdentifiers = append(quotedIdentifiers, quoteIdentifier(identifier))
	}
	return strings.Join(quotedIdentifiers, ".")
}

func quoteIdentifiers(identifiers []string) string {
	quotedIdentifiers := make([]string, 0, len(identifiers))
	for _, identifier := range identifiers {
		quotedIdentifiers = append(quotedIdentifiers, quoteIdentifier(identifier))
	}
	return strings.Join(quotedIdentifiers, ",")
}

// stringLiteralEscaper escapes characters in a Spark SQL single-quoted string literal.
// Spark SQL uses backslash escape sequences inside string literals and does not treat a
// doubled single quote as an escaped quote, so both the backslash and the quote are
// escaped with a backslash.
var stringLiteralEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

func quoteStringLiteral(value string) string {
	return "'" + stringLiteralEscaper.Replace(value) + "'"
}
