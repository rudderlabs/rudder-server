package backendconfig

// processAccountAssociations processes account configurations and merges them with their corresponding
// account definitions. It then associates these merged accounts with destinations based on
// their configuration settings.
//
// The process involves three main steps:
// 1. Creating a map of account definitions for quick lookup
// 2. Merging account configurations with their definitions
// 3. Associating the merged accounts with destinations
func (c *ConfigT) processAccountAssociations() {
	// Create a lookup map for account definitions using their names as keys
	accountDefMap := make(map[string]AccountDefinition)
	for _, accDef := range c.AccountDefinitions {
		accountDefMap[accDef.Name] = accDef
	}

	// Create a map of accounts merged with their definitions
	// This combines the account-specific settings with the shared definition settings
	accountWithDefMap := make(map[string]AccountWithDefinition)
	for _, acc := range c.Accounts {
		accountWithDefMap[acc.Id] = AccountWithDefinition{
			Id:                    acc.Id,
			AccountDefinitionName: acc.AccountDefinitionName,
			Options:               acc.Options,
			Secret:                acc.Secret,
			Config:                accountDefMap[acc.AccountDefinitionName].Config,
		}
	}

	// Iterate through all sources and their destinations to set up account associations
	for i := range c.Sources {
		for j := range c.Sources[i].Destinations {
			dest := &c.Sources[i].Destinations[j]
			c.setDestinationAccounts(dest, accountWithDefMap)
		}
	}
}

// setDestinationAccounts assigns accounts to a destination based on its configuration.
// It handles two types of account associations:
// 1. Regular account (rudderAccountId)
// 2. Delete account (rudderDeleteAccountId)
//
// Parameters:
//   - dest: Pointer to the destination being configured
//   - accountMap: Map of available accounts that can be associated with destinations
func (c *ConfigT) setDestinationAccounts(dest *DestinationT, accountMap map[string]AccountWithDefinition) {
	// Check and set the regular account if specified in the destination config
	if accountID, ok := dest.Config["rudderAccountId"].(string); ok {
		if account, exists := accountMap[accountID]; exists {
			dest.Account = account
		}
	}

	// Check and set the delete account if specified in the destination config
	if deleteAccountID, ok := dest.Config["rudderDeleteAccountId"].(string); ok {
		if account, exists := accountMap[deleteAccountID]; exists {
			dest.DeleteAccount = account
		}
	}
}
