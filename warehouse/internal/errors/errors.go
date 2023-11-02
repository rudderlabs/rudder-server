package errors

import "errors"

var (
	ErrInvalidJSONRequestBody      = errors.New("invalid JSON in request body")
	ErrRequestCancelled            = errors.New("request cancelled")
	ErrWorkspaceDegraded           = errors.New("workspace is degraded")
	ErrNoWarehouseFound            = errors.New("no warehouse found")
	ErrWorkspaceFromSourceNotFound = errors.New("workspace from source not found")
	ErrMarshallResponse            = errors.New("can't marshall response")
)
