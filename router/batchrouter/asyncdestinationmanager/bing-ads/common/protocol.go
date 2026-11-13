package common

import (
	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/config"
)

// APIProtocol returns REST when BatchRouter.<destType>.useRESTAPI is true, SOAP otherwise.
func APIProtocol(conf *config.Config, destType string) bingads.Protocol {
	if conf.GetBoolVar(false, "BatchRouter."+destType+".useRESTAPI") {
		return bingads.ProtocolREST
	}
	return bingads.ProtocolSOAP //nolint:staticcheck // Keep SOAP as the default until the protocol migration is complete.
}
