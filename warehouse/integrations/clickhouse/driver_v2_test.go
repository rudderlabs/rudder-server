package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// TestS3CredentialsRegexV2 guards the masking the middleware applies before it
// logs a query. It builds the statement through copySQLStatement, so a change
// to the shape of the copy command is a change to what is under test here.
func TestS3CredentialsRegexV2(t *testing.T) {
	const (
		accessKeyID     = "AKIAIOSFODNN7EXAMPLE"
		secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		sessionToken    = "FQoGZXIvYXdzEPP//////////wEaDEXAMPLESESSIONTOKEN"
		loadFolder      = "s3://bucket/rudder/uploads/*.csv.gz"
		columnTypes     = "id String,received_at DateTime"
	)

	statementWith := func(token string) string {
		return copySQLStatement("namespace", "table", "id,received_at",
			s3TableFunctionArgs(loadFolder, accessKeyID, secretAccessKey, token, columnTypes),
		)
	}

	for _, tc := range []struct {
		name  string
		token string
	}{
		{name: "static keys", token: ""},
		{name: "temporary credentials", token: sessionToken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statement := statementWith(tc.token)

			masked, err := misc.ReplaceMultiRegex(statement, s3CredentialsRegex)
			require.NoError(t, err)

			require.NotContains(t, masked, accessKeyID)
			require.NotContains(t, masked, secretAccessKey)
			if tc.token != "" {
				require.Contains(t, statement, sessionToken, "the token has to reach the statement to be worth masking")
				require.NotContains(t, masked, sessionToken)
			}
			require.Contains(t, masked, "'***'")

			// The rest of the statement has to survive, or the log stops being
			// useful.
			require.Contains(t, masked, loadFolder)
			require.Contains(t, masked, columnTypes)
			require.Contains(t, masked, "'CSV'")
			require.Contains(t, masked, "date_time_input_format = 'best_effort'")

			t.Run("masking twice changes nothing", func(t *testing.T) {
				twice, err := misc.ReplaceMultiRegex(masked, s3CredentialsRegex)
				require.NoError(t, err)
				require.Equal(t, masked, twice)
			})
		})
	}

	t.Run("statements without s3 are untouched", func(t *testing.T) {
		insert := `INSERT INTO "namespace"."table" ("id","received_at") VALUES (?,?)`
		out, err := misc.ReplaceMultiRegex(insert, s3CredentialsRegex)
		require.NoError(t, err)
		require.Equal(t, insert, out)
	})
}

// TestS3TableFunctionArgsV2 pins the positions, which is the whole contract:
// the s3 table function reads its arguments by index, so an extra one in the
// wrong place is read as a format or a structure rather than rejected.
func TestS3TableFunctionArgsV2(t *testing.T) {
	const (
		loadFolder  = "s3://bucket/rudder/uploads/*.csv.gz"
		columnTypes = "id String,received_at DateTime"
	)

	t.Run("without a session token", func(t *testing.T) {
		require.Equal(t, []string{
			"'" + loadFolder + "'",
			"'key'",
			"'secret'",
			"'CSV'",
			"'" + columnTypes + "'",
			"'gz'",
		}, s3TableFunctionArgs(loadFolder, "key", "secret", "", columnTypes))
	})

	t.Run("with a session token", func(t *testing.T) {
		require.Equal(t, []string{
			"'" + loadFolder + "'",
			"'key'",
			"'secret'",
			"'token'",
			"'CSV'",
			"'" + columnTypes + "'",
			"'gz'",
		}, s3TableFunctionArgs(loadFolder, "key", "secret", "token", columnTypes))
	})
}
