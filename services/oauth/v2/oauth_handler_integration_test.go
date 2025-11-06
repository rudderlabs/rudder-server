package v2_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/rudderauth"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

func TestOauthHandlerIntegration(t *testing.T) {
	envKey := "RSERVER_OAUTH_TEST_CREDENTIALS"
	if v, exists := os.LookupEnv(envKey); !exists || v == "" {
		if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
			t.Fatalf("%s environment variable not set", envKey)
		}
		t.Skipf("Skipping %s as %s is not set", t.Name(), envKey)
	}
	testCredentialsJson := config.GetStringVar("", envKey)
	var testCredentials struct {
		Type                 string            `json:"type"`                 // e.g. reddit
		Secret               json.RawMessage   `json:"secret"`               // initial access and refresh tokens
		Options              json.RawMessage   `json:"options"`              // additional options required for the OAuth flow
		AuthClientSecrets    map[string]string `json:"authClientSecrets"`    // secrets used by the auth server (e.g., client_id and client_secret)
		HasAccountDefinition bool              `json:"hasAccountDefinition"` // whether the destination has an account definition or not (new or old style)
	}
	if err := jsonrs.Unmarshal([]byte(testCredentialsJson), &testCredentials); err != nil {
		t.Fatalf("Failed to unmarshal test credentials: %v", err)
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// The fixture that will be created in rudder auth service for testing
	fixture := rudderauth.AccountFixture{
		ID:                   "account-id",
		Type:                 testCredentials.Type,
		Category:             "destination",
		WorkspaceID:          "workspace-id",
		DestinationID:        "destination-id",
		Options:              testCredentials.Options,
		HasAccountDefinition: testCredentials.HasAccountDefinition,
		Secret:               testCredentials.Secret,
		AuthClientSecrets:    testCredentials.AuthClientSecrets,
	}
	resource, err := rudderauth.Setup(pool, fixture, t)
	require.NoError(t, err, "it should be able to setup the rudder auth resource")

	// create the OAuth handler (client)
	handler := v2.NewOAuthHandler(
		&identityProvider{
			HostedSecret: resource.HostedSecret,
		},
		v2.WithConfigBackendURL(resource.ConfigBackendURL),
	)

	var secret json.RawMessage
	t.Run("FetchToken", func(t *testing.T) {
		secret, err = handler.FetchToken(&v2.OAuthTokenParams{
			AccountID:     fixture.ID,
			WorkspaceID:   fixture.WorkspaceID,
			DestType:      "destType",
			DestinationID: fixture.DestinationID,
		})
		require.NoError(t, err, "it should be able to fetch the token")
		require.NotNil(t, secret, "the fetched token should not be nil")
	})

	t.Run("RefreshToken", func(t *testing.T) {
		newSecret, err := handler.RefreshToken(&v2.OAuthTokenParams{
			AccountID:     fixture.ID,
			WorkspaceID:   fixture.WorkspaceID,
			DestType:      "destType",
			DestinationID: fixture.DestinationID,
		}, secret)
		require.NoError(t, err, "it should be able to refresh the token")
		require.NotNil(t, newSecret, "the refreshed token should not be nil")
		require.NotEqualValues(t, string(secret), string(newSecret), "the refreshed token should be different from the old one")
	})
}

type identityProvider struct {
	HostedSecret string
}

func (m *identityProvider) Identity() identity.Identifier {
	return &identity.Namespace{HostedSecret: m.HostedSecret}
}
