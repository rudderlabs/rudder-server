package api

const (
	ErrUnknownError                        = "ERR_UNKNOWN_ERROR"
	ErrValidationError                     = "ERR_VALIDATION_ERROR"
	ErrAuthenticationFailed                = "ERR_AUTHENTICATION_FAILED"
	ErrRoleDoesNotExistOrNotAuthorized     = "ERR_ROLE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"
	ErrDatabaseDoesNotExistOrNotAuthorized = "ERR_DATABASE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"
	ErrSchemaDoesNotExistOrNotAuthorized   = "ERR_SCHEMA_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"
	ErrTableDoesNotExistOrNotAuthorized    = "ERR_TABLE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"
	ErrSchemaConflict                      = "ERR_SCHEMA_CONFLICT"
)
