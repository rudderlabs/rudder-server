package v2

const (
	OAuth                AuthType   = "OAuth"
	InvalidAuthType      AuthType   = "InvalidAuthType"
	DestKey              ContextKey = "destination"
	SecretKey            ContextKey = "secret"
	RudderFlow_Delivery  RudderFlow = "delivery"
	RudderFlow_Delete    RudderFlow = "delete"
	DeleteAccountIdKey              = "rudderDeleteAccountId"
	DeliveryAccountIdKey            = "rudderAccountId"

	AuthStatusInactive = "inactive"
)
