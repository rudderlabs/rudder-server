//go:generate mockgen -destination=../../mocks/utils/sysUtils/mock_httpclient.go -package mock_sysUtils github.com/rudderlabs/rudder-server/utils/sysUtils HTTPClientI
package sysUtils

import (
	"net/http"
)

// HTTPClient interface
type HTTPClientI interface {
	Do(req *http.Request) (*http.Response, error)
}
