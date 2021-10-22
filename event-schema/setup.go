package event_schema

import (
	"strings"
	"sync"

	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	eventSchemaManager     types.EventSchemasI
	eventSchemaManagerLock sync.RWMutex
)

// GetInstance returns an instance of EventSchemaManagerT
func GetInstance() types.EventSchemasI {
	pkgLogger.Info("[[ EventSchemas ]] Setting up EventSchemas FeatureValue")
	eventSchemaManagerLock.Lock()
	defer eventSchemaManagerLock.Unlock()
	if eventSchemaManager == nil {
		appTypeStr := strings.ToUpper(config.GetEnv("APP_TYPE", app.EMBEDDED))
		schemaManager := &EventSchemaManagerT{disableInMemoryCache: appTypeStr == app.GATEWAY}
		schemaManager.Setup()
		eventSchemaManager = schemaManager
	}
	return eventSchemaManager
}
