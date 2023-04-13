package schematransformer

import (
	"context"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/transientsource"
)

// EventPayload : Generic type for gateway event payload
type EventPayload struct {
	WriteKey string
	Event    map[string]interface{}
}

type SchemaTransformer struct {
	ctx                        context.Context
	g                          *errgroup.Group
	backendConfig              backendconfig.BackendConfig
	transientSources           transientsource.Service
	sourceWriteKeyMap          map[string]string
	newPIIReportingSettings    map[string]bool
	writeKeyMapLock            sync.RWMutex
	writeKeySourceIDMap        map[string]string
	config                     *config.Config
	shouldCaptureNilAsUnknowns bool
	isInitialisedBool          atomic.Bool
}
