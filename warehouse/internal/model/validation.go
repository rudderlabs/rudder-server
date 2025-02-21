package model

import backendconfig "github.com/rudderlabs/rudder-server/backend-config"

const (
	VerifyingObjectStorage       = "Verifying Object Storage"
	VerifyingConnections         = "Verifying Connections"
	VerifyingCreateSchema        = "Verifying Create Schema"
	VerifyingCreateAndAlterTable = "Verifying Create and Alter Table"
	VerifyingFetchSchema         = "Verifying Fetch Schema"
	VerifyingLoadTable           = "Verifying Load Table"
)

type ValidationRequest struct {
	Path        string
	Step        string
	Destination *backendconfig.DestinationT
}

type ValidationResponse struct {
	Error string
	Data  string
}

type Step struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

type StepsResponse struct {
	Steps []*Step `json:"steps"`
}
