package snowflake

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	testSQLTemplate  = `COPY INTO t FROM '%s' %s PATTERN = '.*\.csv\.gz'`
	testLoadLocation = "s3://bucket/rudder/uploads/identity-mappings/"
)

var testSecretAuthClause = awsCredentialsClause(
	"AKIAIOSFODNN7EXAMPLE",
	"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	"FQoGZXIvYXdzEPP//////////wEaDEXAMPLESESSIONTOKEN",
)

func TestCopyStatement(t *testing.T) {
	loggable := fmt.Sprintf(testSQLTemplate, testLoadLocation, secretAuthClausePlaceholder)

	copyStmt, err := newCopyStatement(loggable, testSecretAuthClause)
	require.NoError(t, err)

	t.Run("formatting leaves the credentials out", func(t *testing.T) {
		require.Equal(t, loggable, copyStmt.String())
		require.Equal(t, loggable, fmt.Sprintf("%v", copyStmt))
		require.Equal(t, loggable, fmt.Sprintf("%+v", copyStmt))
		require.Equal(t, loggable, fmt.Sprintf("%#v", copyStmt))
	})

	t.Run("unsafeSQL puts the auth clause where the placeholder was", func(t *testing.T) {
		require.Equal(t, fmt.Sprintf(testSQLTemplate, testLoadLocation, testSecretAuthClause), copyStmt.unsafeSQL())
	})

	t.Run("the placeholder has to appear exactly once", func(t *testing.T) {
		_, err := newCopyStatement(`COPY INTO t FROM 's3://bucket/'`, testSecretAuthClause)
		require.Error(t, err)

		// A location that carries the placeholder must not decide where the
		// credentials land.
		_, err = newCopyStatement(
			fmt.Sprintf(testSQLTemplate, "s3://bucket/"+secretAuthClausePlaceholder, secretAuthClausePlaceholder),
			testSecretAuthClause,
		)
		require.Error(t, err)
	})
}

// TestCopyCredentialsRegex guards the masking the middleware applies to the
// statements it logs, which reach it with the credentials in place.
func TestCopyCredentialsRegex(t *testing.T) {
	masked, err := misc.ReplaceMultiRegex(fmt.Sprintf(testSQLTemplate, testLoadLocation, testSecretAuthClause), copyCredentialsRegex)
	require.NoError(t, err)
	require.Equal(t,
		fmt.Sprintf(testSQLTemplate, testLoadLocation, `CREDENTIALS = (AWS_KEY_ID='***' AWS_SECRET_KEY='***' AWS_TOKEN='***')`),
		masked,
	)
}
