package schematransformer

import (
	"context"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// EventPayload : Generic type for gateway event payload
type EventPayload struct {
	Event map[string]interface{}
}

type SchemaTransformer struct {
	ctx                        context.Context
	g                          *errgroup.Group
	backendConfig              backendconfig.BackendConfig
	sourceWriteKeyMap          map[string]string
	newPIIReportingSettings    map[string]bool
	writeKeyMapLock            sync.RWMutex
	config                     *config.Config
	shouldCaptureNilAsUnknowns bool
	isInitialisedBool          atomic.Bool
}
