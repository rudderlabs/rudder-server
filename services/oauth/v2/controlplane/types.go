package controlplane

import (
	"net/http"

	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

type Request struct {
	Body           string
	ContentType    string
	URL            string
	Method         string
	DestName       string
	RequestType    string // This is to add more refined stat tags
	BasicAuthUser  identity.Identifier
	rudderFlowType common.RudderFlow
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}
