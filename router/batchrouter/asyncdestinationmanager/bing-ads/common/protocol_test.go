package common_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/common"
)

func TestAPIProtocol(t *testing.T) {
	const (
		audience           = "BINGADS_AUDIENCE"
		offlineConversions = "BINGADS_OFFLINE_CONVERSIONS"
	)
	soap := bingads.ProtocolSOAP //nolint:staticcheck // SOAP remains the default during the migration.
	tests := []struct {
		name        string
		flags       map[string]bool
		destination string
		want        bingads.Protocol
	}{
		{name: "audience defaults to SOAP", destination: audience, want: soap},
		{name: "offline conversions defaults to SOAP", destination: offlineConversions, want: soap},
		{name: "audience REST", flags: map[string]bool{audience: true}, destination: audience, want: bingads.ProtocolREST},
		{name: "offline conversions REST", flags: map[string]bool{offlineConversions: true}, destination: offlineConversions, want: bingads.ProtocolREST},
		{name: "audience flag does not affect offline conversions", flags: map[string]bool{audience: true}, destination: offlineConversions, want: soap},
		{name: "offline conversions flag does not affect audience", flags: map[string]bool{offlineConversions: true}, destination: audience, want: soap},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := config.New()
			for destType, enabled := range tt.flags {
				conf.Set("BatchRouter."+destType+".useRESTAPI", enabled)
			}
			require.Equal(t, tt.want, common.APIProtocol(conf, tt.destination))
		})
	}
}
