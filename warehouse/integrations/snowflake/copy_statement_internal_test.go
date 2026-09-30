package snowflake

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

const testLoadLocation = "s3://bucket/rudder/uploads/identity-mappings/"

var testSecretAuthClause = awsCredentialsClause(
	"AKIAIOSFODNN7EXAMPLE",
	"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	"FQoGZXIvYXdzEPP//////////wEaDEXAMPLESESSIONTOKEN",
)

func testBuildCopySQL(authClause string) string {
	return fmt.Sprintf(`COPY INTO t FROM '%s' %s PATTERN = '.*\.csv\.gz'`, testLoadLocation, authClause)
}

func TestCopyStatement(t *testing.T) {
	copyStmt := newCopyStatement(testBuildCopySQL, testSecretAuthClause, maskedAWSCredentialsClause)
	loggable := testBuildCopySQL(maskedAWSCredentialsClause)

	t.Run("formatting leaves the credentials out", func(t *testing.T) {
		require.Equal(t, loggable, copyStmt.String())
		require.Equal(t, loggable, fmt.Sprintf("%#v", copyStmt))
	})

	t.Run("the database gets the real auth clause", func(t *testing.T) {
		require.Equal(t, testBuildCopySQL(testSecretAuthClause), copyStmt.secretSQL)
	})
}

// TestCopyCredentialsRegex guards the masking the middleware applies to the
// statements it logs, which reach it with the credentials in place. It has to
// agree with what a copyStatement logs.
func TestCopyCredentialsRegex(t *testing.T) {
	masked, err := misc.ReplaceMultiRegex(testBuildCopySQL(testSecretAuthClause), copyCredentialsRegex)
	require.NoError(t, err)
	require.Equal(t, testBuildCopySQL(maskedAWSCredentialsClause), masked)
}
