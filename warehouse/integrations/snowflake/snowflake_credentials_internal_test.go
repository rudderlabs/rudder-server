package snowflake

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaskCopyCredentials(t *testing.T) {
	const (
		accessKeyID     = "AKIAIOSFODNN7EXAMPLE"
		secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		sessionToken    = "FQoGZXIvYXdzEPP//////////wEaDEXAMPLESESSIONTOKEN"
		loadLocation    = "s3://bucket/rudder/uploads/identity-mappings/"
	)

	statement := fmt.Sprintf(`COPY INTO t FROM '%s' %s PATTERN = '.*\.csv\.gz'`,
		loadLocation,
		awsCredentialsClause(accessKeyID, secretAccessKey, sessionToken),
	)

	masked := maskCopyCredentials(statement)

	require.NotContains(t, masked, accessKeyID)
	require.NotContains(t, masked, secretAccessKey)
	require.NotContains(t, masked, sessionToken)
	require.Contains(t, masked, `CREDENTIALS = (AWS_KEY_ID='***' AWS_SECRET_KEY='***' AWS_TOKEN='***')`)
	// The rest of the statement has to survive, or the log stops being useful.
	require.Contains(t, masked, loadLocation)
	require.Contains(t, masked, `PATTERN = '.*\.csv\.gz'`)

	t.Run("statements without credentials are untouched", func(t *testing.T) {
		storageIntegration := `COPY INTO t FROM 's3://bucket/' STORAGE_INTEGRATION = my_integration`
		require.Equal(t, storageIntegration, maskCopyCredentials(storageIntegration))
	})
}
