package event_schema

import (
	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	eventSchemaManager types.EventSchemasI
)

type InstanceOpts struct {
	DisableInMemoryCache bool
}

// GetInstance returns an instance of EventSchemaManagerT
func GetInstance(opts InstanceOpts) types.EventSchemasI {
	pkgLogger.Info("[[ EventSchemas ]] Setting up EventSchemas FeatureValue")
	if eventSchemaManager == nil {
		schemaManager := &EventSchemaManagerT{disableInMemoryCache: opts.DisableInMemoryCache}
		schemaManager.Setup()
		eventSchemaManager = schemaManager
	}
	return eventSchemaManager
}
