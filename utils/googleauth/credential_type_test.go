package googleauth

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestCredentialsTypeFromJSON(t *testing.T) {
	t.Run("service account", func(t *testing.T) {
		credType := CredentialsTypeFromJSON([]byte(`{"type":"service_account"}`))
		require.Equal(t, option.ServiceAccount, credType)
	})

	t.Run("authorized user", func(t *testing.T) {
		credType := CredentialsTypeFromJSON([]byte(`{"type":"authorized_user"}`))
		require.Equal(t, option.AuthorizedUser, credType)
	})

	t.Run("impersonated service account", func(t *testing.T) {
		credType := CredentialsTypeFromJSON([]byte(`{"type":"impersonated_service_account"}`))
		require.Equal(t, option.ImpersonatedServiceAccount, credType)
	})

	t.Run("external account", func(t *testing.T) {
		credType := CredentialsTypeFromJSON([]byte(`{"type":"external_account"}`))
		require.Equal(t, option.ExternalAccount, credType)
	})

	t.Run("missing type falls back", func(t *testing.T) {
		credType := CredentialsTypeFromJSON([]byte(`{"project_id":"x"}`))
		require.Equal(t, option.ServiceAccount, credType)
	})

	t.Run("unsupported type falls back", func(t *testing.T) {
		credType := CredentialsTypeFromJSON([]byte(`{"type":"foo"}`))
		require.Equal(t, option.ServiceAccount, credType)
	})

	t.Run("invalid json falls back", func(t *testing.T) {
		credType := CredentialsTypeFromJSON([]byte(`{`))
		require.Equal(t, option.ServiceAccount, credType)
	})
}
