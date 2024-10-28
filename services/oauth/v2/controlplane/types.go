package controlplane

//go:generate mockgen -destination=../../../../mocks/services/oauth/v2/http/mock_http_client.go -package=mock_http_client github.com/rudderlabs/rudder-server/services/oauth/v2/controlplane HttpClient

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
