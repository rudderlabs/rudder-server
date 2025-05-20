package backendconfig

import "github.com/rudderlabs/rudder-go-kit/logger"

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

// getAccountDefinition returns a pointer to the account definition for the given account definition name.
// If the account definition name is empty or the account definition doesn't exist, it returns nil.
//
// Parameters:
//   - accountDefinitionName: The name of the account definition to look up
//   - accountID: The ID of the account (used for logging)
//   - accountField: The field name in the destination config (used for logging)
//   - destinationID: The ID of the destination (used for logging)
//   - flowType: The type of flow (used for logging)
//
// Returns:
//   - *AccountDefinition: A pointer to the account definition or nil if not found
func (c *ConfigT) getAccountDefinition(accountDefinitionName, accountID, accountField, destinationID, flowType string) *AccountDefinition {
	if accountDefinitionName == "" {
		return nil
	}

	if accountDefinition, exists := c.AccountDefinitions[accountDefinitionName]; exists {
		return &accountDefinition
	}

	// Log error if account definition not found
	pkgLogger.Errorn("Account definition not found in configured accountDefinitions for "+flowType+" flow",
		logger.NewStringField(accountField, accountID),
		logger.NewStringField("accountDefinitionName", accountDefinitionName),
		logger.NewStringField("destinationId", destinationID))

	return nil
}

// populateAccountToDestination populates an account to a destination field based on the account ID.
// It creates an Account object by combining account details with its definition.
//
// Parameters:
//   - accountID: The ID of the account to populate
//   - destination: Pointer to the destination being configured
//   - accountField: The field name in the destination config (e.g., "rudderAccountId" or "rudderDeleteAccountId")
//   - flowType: The type of flow (e.g., "delivery" or "regulation")
//
// Returns:
//   - *Account: The populated account object or nil if the account doesn't exist
func (c *ConfigT) populateAccountToDestination(accountID string, destination *DestinationT, accountField, flowType string) *Account {
	if account, exists := c.Accounts[accountID]; exists {
		accountDefinitionPtr := c.getAccountDefinition(
			account.AccountDefinitionName,
			accountID,
			accountField,
			destination.ID,
			flowType,
		)

		return &Account{
			ID:                    accountID,
			AccountDefinitionName: account.AccountDefinitionName,
			Options:               account.Options,
			Secret:                account.Secret,
			AccountDefinition:     accountDefinitionPtr,
		}
	}

	pkgLogger.Errorn("Account not found in configured accounts for "+flowType+" flow",
		logger.NewStringField(accountField, accountID),
		logger.NewStringField("destinationId", destination.ID))

	return nil
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
		dest.DeliveryAccount = c.populateAccountToDestination(accountID, dest, "rudderAccountId", "delivery")
	}

	// Check and set the delete account if specified in the destination config
	if deleteAccountID, ok := dest.Config["rudderDeleteAccountId"].(string); ok {
		dest.DeleteAccount = c.populateAccountToDestination(deleteAccountID, dest, "rudderDeleteAccountId", "regulation")
	}
}
