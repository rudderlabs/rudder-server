package v2

const (
	OAuth           AuthType = "OAuth"
	InvalidAuthType AuthType = "InvalidAuthType"

	RudderFlow_Delivery       RudderFlow = "delivery"
	RudderFlow_Delete         RudderFlow = "delete"
	RudderFlow_Transformation RudderFlow = "transformation"
	DeleteAccountIdKey                   = "rudderDeleteAccountId"
	DeliveryAccountIdKey                 = "rudderAccountId"

	AuthStatusInactive = "inactive"
)
