package v2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// The token endpoint answers with the account object, so the same JSON carries the version for the
// cached token and for the workspace config's account.
func TestSecretVersionParsing(t *testing.T) {
	const response = `{"secret":{"access_token":"t","expirationDate":"2099-01-01T00:00:00.000Z"},"secretVersion":8}`

	token, err := getRefreshTokenFromResponse(response, logger.NOP)
	require.Nil(t, err)
	require.Equal(t, 8, token.SecretVersion)
	require.JSONEq(t, `{"access_token":"t","expirationDate":"2099-01-01T00:00:00.000Z"}`, string(token.Secret))
	require.Empty(t, token.ExpirationDate, "getTokenFromAPI copies the nested expiry after parsing")

	var account backendconfig.Account
	require.NoError(t, jsonrs.Unmarshal([]byte(response), &account))
	require.Equal(t, 8, account.SecretVersion)
}
