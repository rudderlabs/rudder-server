package backendconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccountAssociations(t *testing.T) {
	t.Run("basic account merge", func(t *testing.T) {
		c := &ConfigT{
			Sources: []SourceT{
				{
					ID: "source-1",
					Destinations: []DestinationT{
						{
							ID: "dest-1",
							Config: map[string]interface{}{
								"rudderAccountId": "acc-1",
							},
						},
					},
				},
			},
			Accounts: map[string]Account{
				"acc-1": {
					AccountDefinitionName: "oauth-def",
					Options:               map[string]interface{}{"key1": "value1"},
					Secret:                map[string]interface{}{"secret1": "secretValue1"},
				},
			},
			AccountDefinitions: map[string]AccountDefinition{
				"oauth-def": {
					Name: "oauth-def",
					Config: map[string]interface{}{
						"OAuth": map[string]interface{}{
							"generateOAuthToken":      true,
							"refreshTokenInDataplane": true,
						},
					},
					AuthenticationType: "OAuth",
				},
			},
		}

		c.processAccountAssociations()

		require.Equal(t, "acc-1", c.Sources[0].Destinations[0].DeliveryAccount.ID)
		require.Equal(t, map[string]interface{}{"key1": "value1"}, c.Sources[0].Destinations[0].DeliveryAccount.Options)
		require.Equal(t, map[string]interface{}{"secret1": "secretValue1"}, c.Sources[0].Destinations[0].DeliveryAccount.Secret)
		require.Equal(t, map[string]interface{}{
			"OAuth": map[string]interface{}{
				"generateOAuthToken":      true,
				"refreshTokenInDataplane": true,
			},
		}, c.Sources[0].Destinations[0].DeliveryAccount.AccountDefinition.Config)
		require.Equal(t, "OAuth", c.Sources[0].Destinations[0].DeliveryAccount.AccountDefinition.AuthenticationType)
	})

	t.Run("multiple destinations with same account", func(t *testing.T) {
		c := &ConfigT{
			Sources: []SourceT{
				{
					ID: "source-1",
					Destinations: []DestinationT{
						{
							ID: "dest-1",
							Config: map[string]interface{}{
								"rudderAccountId": "acc-1",
							},
						},
						{
							ID: "dest-2",
							Config: map[string]interface{}{
								"rudderAccountId": "acc-1",
							},
						},
					},
				},
			},
			Accounts: map[string]Account{
				"acc-1": {
					AccountDefinitionName: "oauth-def",
					Options:               map[string]interface{}{"key1": "value1"},
				},
			},
			AccountDefinitions: map[string]AccountDefinition{
				"oauth-def": {
					Name:               "oauth-def",
					Config:             map[string]interface{}{"oauth": true},
					AuthenticationType: "OAuth",
				},
			},
		}

		c.processAccountAssociations()

		for _, dest := range c.Sources[0].Destinations {
			require.Equal(t, "acc-1", dest.DeliveryAccount.ID)
			require.Equal(t, AccountDefinition{
				Name:               "oauth-def",
				Config:             map[string]interface{}{"oauth": true},
				AuthenticationType: "OAuth",
			}, *dest.DeliveryAccount.AccountDefinition)
		}
	})

	t.Run("destination with delete account", func(t *testing.T) {
		c := &ConfigT{
			Sources: []SourceT{
				{
					ID: "source-1",
					Destinations: []DestinationT{
						{
							ID: "dest-1",
							Config: map[string]interface{}{
								"rudderDeleteAccountId": "acc-1",
							},
						},
					},
				},
			},
			Accounts: map[string]Account{
				"acc-1": {
					AccountDefinitionName: "oauth-def",
					Options:               map[string]interface{}{"key1": "value1"},
				},
			},
			AccountDefinitions: map[string]AccountDefinition{
				"oauth-def": {
					Name:   "oauth-def",
					Config: map[string]interface{}{"oauth": true},
				},
			},
		}

		c.processAccountAssociations()

		require.Equal(t, "acc-1", c.Sources[0].Destinations[0].DeleteAccount.ID)
		require.Equal(t, AccountDefinition{
			Name:   "oauth-def",
			Config: map[string]interface{}{"oauth": true},
		}, *c.Sources[0].Destinations[0].DeleteAccount.AccountDefinition)
	})

	t.Run("destination with no account configuration", func(t *testing.T) {
		c := &ConfigT{
			Sources: []SourceT{
				{
					ID: "source-1",
					Destinations: []DestinationT{
						{
							ID:     "dest-1",
							Config: map[string]interface{}{},
						},
					},
				},
			},
			Accounts: map[string]Account{
				"acc-1": {
					AccountDefinitionName: "oauth-def",
				},
			},
			AccountDefinitions: map[string]AccountDefinition{
				"oauth-def": {
					Name:   "oauth-def",
					Config: map[string]interface{}{},
				},
			},
		}

		c.processAccountAssociations()

		require.Empty(t, c.Sources[0].Destinations[0].DeliveryAccount)
		require.Empty(t, c.Sources[0].Destinations[0].DeleteAccount)
	})

	t.Run("non-existent account id", func(t *testing.T) {
		c := &ConfigT{
			Sources: []SourceT{
				{
					ID: "source-1",
					Destinations: []DestinationT{
						{
							ID: "dest-1",
							Config: map[string]interface{}{
								"rudderAccountId": "non-existent",
							},
						},
					},
				},
			},
			Accounts: map[string]Account{
				"acc-1": {
					AccountDefinitionName: "oauth-def",
				},
			},
			AccountDefinitions: map[string]AccountDefinition{
				"oauth-def": {
					Name:   "oauth-def",
					Config: map[string]interface{}{},
				},
			},
		}

		c.processAccountAssociations()

		require.Empty(t, c.Sources[0].Destinations[0].DeliveryAccount)
	})

	t.Run("non-existent account definition", func(t *testing.T) {
		c := &ConfigT{
			Sources: []SourceT{
				{
					ID: "source-1",
					Destinations: []DestinationT{
						{
							ID: "dest-1",
							Config: map[string]interface{}{
								"rudderAccountId": "acc-1",
							},
						},
					},
				},
			},
			Accounts: map[string]Account{
				"acc-1": {
					AccountDefinitionName: "non-existent-def",
				},
			},
			AccountDefinitions: map[string]AccountDefinition{
				"oauth-def": {
					Name:   "oauth-def",
					Config: map[string]interface{}{},
				},
			},
		}

		c.processAccountAssociations()

		require.Equal(t, "acc-1", c.Sources[0].Destinations[0].DeliveryAccount.ID)
		require.Empty(t, c.Sources[0].Destinations[0].DeliveryAccount.AccountDefinition)
	})
}
