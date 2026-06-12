package bqstreamv2

import (
	"github.com/mitchellh/mapstructure"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// Decode decodes the destination configuration from the given map. Also, converts the namespace to the provider case.
func (d *destConfig) Decode(m map[string]any) error {
	if err := mapstructure.Decode(m, d); err != nil {
		return err
	}
	d.Namespace = whutils.ToProviderCase(
		whutils.BQStreamV2,
		whutils.ToSafeNamespace(whutils.BQStreamV2, d.Namespace),
	)
	return nil
}
