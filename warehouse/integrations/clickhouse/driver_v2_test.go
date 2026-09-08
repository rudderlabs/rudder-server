package clickhouse

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// TestS3CredentialsRegexV2 guards the masking the middleware applies before it
// logs a query. The statement below mirrors the one loadByCopyCommand builds:
// if that template changes shape, this is what catches the credentials going
// back into the logs.
func TestS3CredentialsRegexV2(t *testing.T) {
	const (
		accessKeyID     = "AKIAIOSFODNN7EXAMPLE"
		secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		loadFolder      = "s3://bucket/rudder/uploads/*.csv.gz"
		columnTypes     = "id String,received_at DateTime"
	)

	statement := fmt.Sprintf(`
		INSERT INTO %[1]q.%[2]q (
			%[3]s
		)
		SELECT
		  *
		FROM
		  s3(
			'%[4]s',
		  	'%[5]s',
		  	'%[6]s',
			'CSV',
			'%[7]s',
			'gz'
		  )
			settings
				date_time_input_format = 'best_effort',
				input_format_csv_arrays_as_nested_csv = 1;
		`,
		"namespace",      // 1
		"table",          // 2
		"id,received_at", // 3
		loadFolder,       // 4
		accessKeyID,      // 5
		secretAccessKey,  // 6
		columnTypes,      // 7
	)

	masked, err := misc.ReplaceMultiRegex(statement, s3CredentialsRegex)
	require.NoError(t, err)

	require.NotContains(t, masked, accessKeyID)
	require.NotContains(t, masked, secretAccessKey)
	require.Contains(t, masked, "'***', '***'")

	// The rest of the statement has to survive, or the log stops being useful.
	require.Contains(t, masked, loadFolder)
	require.Contains(t, masked, columnTypes)
	require.Contains(t, masked, "'CSV'")
	require.Contains(t, masked, "date_time_input_format = 'best_effort'")

	t.Run("masking twice changes nothing", func(t *testing.T) {
		twice, err := misc.ReplaceMultiRegex(masked, s3CredentialsRegex)
		require.NoError(t, err)
		require.Equal(t, masked, twice)
	})

	t.Run("statements without s3 are untouched", func(t *testing.T) {
		insert := `INSERT INTO "namespace"."table" ("id","received_at") VALUES (?,?)`
		out, err := misc.ReplaceMultiRegex(insert, s3CredentialsRegex)
		require.NoError(t, err)
		require.Equal(t, insert, out)
	})
}
