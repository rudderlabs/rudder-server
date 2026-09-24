package redshift

import (
	"fmt"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// TestColumnsWithDataTypesNeutralizesSQLInjection proves the reason for the
// identifier-quoting change: a malicious event property whose name is crafted to
// break out of the double-quoted identifier in the generated CREATE TABLE column
// list must be neutralized. The embedded double quote has to be doubled so the
// injected `);drop table ...` / `copy ... to program ...` payload stays inert
// inside the identifier instead of terminating it.
func TestColumnsWithDataTypesNeutralizesSQLInjection(t *testing.T) {
	// Exact payloads from the reported warehouse identifier-injection: a DDL-drop
	// primitive and the COPY-TO-PROGRAM remote-code-execution escalation.
	payloads := map[string]string{
		"drop_table":          `x" text);drop table rudder_secrets;--`,
		"rce_copy_to_program": `x" text);copy (select '') to program 'id>/tmp/rce';--`,
	}

	for name, columnName := range payloads {
		t.Run(name, func(t *testing.T) {
			fragment := ColumnsWithDataTypes(model.TableSchema{
				columnName: model.StringDataType,
			}, "")

			// The column name is emitted as a properly double-quoted identifier
			// with the embedded `"` doubled.
			require.Contains(t, fragment, pq.QuoteIdentifier(columnName))
			// The raw, unescaped payload must not survive verbatim - if it did, it
			// would close the identifier early and the trailing SQL would execute.
			require.NotContains(t, fragment, columnName)
		})
	}
}

// TestCopyCredentialsRegex covers the COPY statement's credential masking and the
// escaping of the values around it. The credentials themselves are interpolated
// without escaping, because escaping them would double a quote and stop the masking
// pattern from matching, so this pins the masking for ordinary credentials and the
// literal escaping for the location and the region.
func TestCopyCredentialsRegex(t *testing.T) {
	copyStatement := func(secret, region string) string {
		return fmt.Sprintf(
			`COPY %s(%s) FROM %s CSV GZIP ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' SESSION_TOKEN '%s' REGION %s DATEFORMAT 'auto'`,
			warehouseutils.QuoteQualifiedIdentifier(warehouseutils.DoubleQuoteIdentifier, "namespace", "tracks"),
			warehouseutils.JoinQuotedIdentifiers([]string{"id", "received_at"}, warehouseutils.DoubleQuoteIdentifier, ","),
			warehouseutils.SQLStringLiteralBackslash("s3://bucket/rudder/uploads/"),
			"AKIAIOSFODNN7EXAMPLE",
			secret,
			"FQoGZXIvYXdzEPP",
			warehouseutils.SQLStringLiteralBackslash(region),
		)
	}

	t.Run("credentials are masked", func(t *testing.T) {
		masked, err := misc.ReplaceMultiRegex(copyStatement("wJalrXUtnFEMI/K7MDENG", "us-east-1"), copyCredentialsRegex)
		require.NoError(t, err)

		require.NotContains(t, masked, "AKIAIOSFODNN7EXAMPLE")
		require.NotContains(t, masked, "wJalrXUtnFEMI/K7MDENG")
		require.NotContains(t, masked, "FQoGZXIvYXdzEPP")
		require.Contains(t, masked, "ACCESS_KEY_ID '***'")
		require.Contains(t, masked, "SECRET_ACCESS_KEY '***'")
		require.Contains(t, masked, "SESSION_TOKEN '***'")

		// The rest of the statement has to survive, or the log stops being useful.
		require.Contains(t, masked, `COPY "namespace"."tracks"("id","received_at")`)
		require.Contains(t, masked, "'s3://bucket/rudder/uploads/'")
		require.Contains(t, masked, "REGION 'us-east-1'")
	})

	t.Run("region is escaped as a literal", func(t *testing.T) {
		// REGION is a string literal like the location beside it, so a quote in it
		// must not be able to close the literal and let the rest run on as SQL.
		statement := copyStatement("secret", `us-east-1'; DROP TABLE users; --`)
		require.Contains(t, statement, `REGION 'us-east-1''; DROP TABLE users; --'`)
		require.NotContains(t, statement, `REGION 'us-east-1'; DROP`)
	})
}
