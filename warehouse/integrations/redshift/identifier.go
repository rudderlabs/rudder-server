package redshift

import (
	"strings"

	"github.com/lib/pq"
)

func quoteIdentifier(identifier string) string {
	return pq.QuoteIdentifier(identifier)
}

func quoteQualifiedIdentifier(identifiers ...string) string {
	quotedIdentifiers := make([]string, 0, len(identifiers))
	for _, identifier := range identifiers {
		quotedIdentifiers = append(quotedIdentifiers, quoteIdentifier(identifier))
	}
	return strings.Join(quotedIdentifiers, ".")
}

func quoteColumnList(columns string) string {
	parts := strings.Split(columns, ",")
	for i, column := range parts {
		parts[i] = quoteIdentifier(strings.TrimSpace(column))
	}
	return strings.Join(parts, ", ")
}

func quoteIdentifiers(identifiers []string) string {
	quotedIdentifiers := make([]string, 0, len(identifiers))
	for _, identifier := range identifiers {
		quotedIdentifiers = append(quotedIdentifiers, quoteIdentifier(identifier))
	}
	return strings.Join(quotedIdentifiers, ",")
}
