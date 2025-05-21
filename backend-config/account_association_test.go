package backendconfig

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jsonrs"
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
		require.Nil(t, c.Sources[0].Destinations[0].DeliveryAccount.AccountDefinition)
	})

	t.Run("blank account definition name for an account which will result in nil accountDefinition for the account added to the destination", func(t *testing.T) {
		// Set up test configuration
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
					AccountDefinitionName: "", // Blank account definition name
					Options:               map[string]interface{}{"key1": "value1"},
					Secret:                map[string]interface{}{"secret1": "secretValue1"},
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

		// Verify the account is populated but AccountDefinition is nil
		require.Equal(t, "acc-1", c.Sources[0].Destinations[0].DeliveryAccount.ID)
		require.Equal(t, "", c.Sources[0].Destinations[0].DeliveryAccount.AccountDefinitionName)
		require.Equal(t, map[string]interface{}{"key1": "value1"}, c.Sources[0].Destinations[0].DeliveryAccount.Options)
		require.Equal(t, map[string]interface{}{"secret1": "secretValue1"}, c.Sources[0].Destinations[0].DeliveryAccount.Secret)
		require.Nil(t, c.Sources[0].Destinations[0].DeliveryAccount.AccountDefinition)

		// Note: We can't easily test that no warning was logged without mocking the logger,
		// but the behavior is correct - no warning should be logged for blank AccountDefinitionName
	})

	t.Run("This test ensures JSON serialization/deserialization works correctly with the Account so that destination will contain deliveryAccount/deleteAccount", func(t *testing.T) {
		dest := DestinationT{
			ID: "dest-1",
			DeliveryAccount: &Account{
				ID:                    "acc-1",
				AccountDefinitionName: "oauth-def",
				Options:               map[string]interface{}{"key1": "value1"},
				Secret:                map[string]interface{}{"secret1": "secretValue1"},
				AccountDefinition: &AccountDefinition{
					Name:               "oauth-def",
					Config:             map[string]interface{}{"oauth": true},
					AuthenticationType: "OAuth",
				},
			},
			DeleteAccount: &Account{
				ID:                    "acc-2",
				AccountDefinitionName: "oauth-def",
				Options:               map[string]interface{}{"key2": "value2"},
				Secret:                map[string]interface{}{"secret2": "secretValue2"},
				AccountDefinition: &AccountDefinition{
					Name:               "oauth-def",
					Config:             map[string]interface{}{"oauth": true},
					AuthenticationType: "OAuth",
				},
			},
		}

		// Serialize to JSON
		jsonBytes, err := jsonrs.Marshal(dest)
		require.NoError(t, err)

		// Verify JSON field names
		jsonStr := string(jsonBytes)
		require.Contains(t, jsonStr, `"deliveryAccount":`)
		require.Contains(t, jsonStr, `"deleteAccount":`)

		// Deserialize from JSON
		var destFromJSON DestinationT
		err = jsonrs.Unmarshal(jsonBytes, &destFromJSON)
		require.NoError(t, err)

		// Verify fields were correctly deserialized
		require.Equal(t, "dest-1", destFromJSON.ID)
		require.Equal(t, "acc-1", destFromJSON.DeliveryAccount.ID)
		require.Equal(t, "acc-2", destFromJSON.DeleteAccount.ID)
		require.Equal(t, map[string]interface{}{"key1": "value1"}, destFromJSON.DeliveryAccount.Options)
		require.Equal(t, map[string]interface{}{"key2": "value2"}, destFromJSON.DeleteAccount.Options)
		require.Equal(t, "OAuth", destFromJSON.DeliveryAccount.AccountDefinition.AuthenticationType)
		require.Equal(t, "OAuth", destFromJSON.DeleteAccount.AccountDefinition.AuthenticationType)
	})
}
