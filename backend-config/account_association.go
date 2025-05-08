package backendconfig

// processAccountAssociations processes account configurations and merges them with their corresponding
// account definitions. It then associates these merged accounts with destinations based on
// their configuration settings.
//
// The process involves:
// 1. Iterating through all sources and their destinations
// 2. For each destination, setting up account associations using setDestinationAccounts
func (c *ConfigT) processAccountAssociations() {
	// Iterate through all sources and their destinations to set up account associations
	if len(c.Accounts) > 0 {
		for i := range c.Sources {
			for j := range c.Sources[i].Destinations {
				dest := &c.Sources[i].Destinations[j]
				c.setDestinationAccounts(dest)
			}
		}
	}
}

// setDestinationAccounts assigns accounts to a destination based on its configuration.
// It handles two types of account associations:
// 1. Regular account (rudderAccountId) - Used for normal event delivery flow
// 2. Delete account (rudderDeleteAccountId) - Used for data deletion/regulation flow
//
// For each account type, it:
// 1. Checks if the corresponding account ID exists in the destination config
// 2. Verifies the account exists in the accounts map
// 3. Creates an AccountWithDefinition by combining account details with its definition
// 4. Assigns it to the appropriate field in the destination
//
// Parameters:
//   - dest: Pointer to the destination being configured
func (c *ConfigT) setDestinationAccounts(dest *DestinationT) {
	// Check and set the regular account if specified in the destination config
	if accountID, ok := dest.Config["rudderAccountId"].(string); ok {
		if account, exists := c.Accounts[accountID]; exists {
			dest.DeliveryAccount = &AccountWithDefinition{
				Id:                account.Id,
				Options:           account.Options,
				Secret:            account.Secret,
				AccountDefinition: c.AccountDefinitions[account.AccountDefinitionName],
			}
		}
	}

	// Check and set the delete account if specified in the destination config
	if deleteAccountID, ok := dest.Config["rudderDeleteAccountId"].(string); ok {
		if account, exists := c.Accounts[deleteAccountID]; exists {
			dest.DeleteAccount = &AccountWithDefinition{
				Id:                account.Id,
				Options:           account.Options,
				Secret:            account.Secret,
				AccountDefinition: c.AccountDefinitions[account.AccountDefinitionName],
			}
		}
	}
}
