package backendconfig

import (
	"encoding/json"
	"fmt"
	"time"
)

var _ mapper = &stubMapper{}

// stubMapper stands in until the real v2 to v1 transform lands. It produces the least a ConfigT
// can be identified by, so that the resolver around it - the incremental merge, the memo, the
// post-transform steps - can be exercised on its own.
type stubMapper struct{}

func (*stubMapper) Map(workspaceID string, raw json.RawMessage, _ v2Catalogues) (ConfigT, error) {
	config := ConfigT{WorkspaceID: workspaceID}
	if updatedAt := jsonparser.GetStringOrEmpty(raw, "updatedAt"); updatedAt != "" {
		parsed, err := time.Parse(time.RFC3339, updatedAt)
		if err != nil {
			return ConfigT{}, fmt.Errorf("parsing updatedAt: %w", err)
		}
		config.UpdatedAt = parsed
	}
	return config, nil
}
