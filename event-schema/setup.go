package event_schema

import (
	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	eventSchemaManager types.EventSchemasI
)

// GetInstance returns an instance of EventSchemaManagerT
func GetInstance() types.EventSchemasI {
	pkgLogger.Info("[[ EventSchemas ]] Setting up EventSchemas FeatureValue")
	if eventSchemaManager == nil {
		schemaManager := &EventSchemaManagerT{}
		schemaManager.Setup()
		eventSchemaManager = schemaManager
	}
	return eventSchemaManager
}
