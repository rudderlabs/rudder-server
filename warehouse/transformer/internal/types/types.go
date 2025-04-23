package types

import (
	proctypes "github.com/rudderlabs/rudder-server/processor/types"
)

type WarehouseTransformerEvent struct {
	*proctypes.TransformerEvent

	// Currently we don't modify anything on things like Message, Metadata present in processor TransformerEvent
	// If something that needs to be modified in case if it is empty, those fields are present here and set once which can be later used
	MessageID  any
	ReceivedAt string
}
