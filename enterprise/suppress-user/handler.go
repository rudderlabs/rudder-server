package suppression

import "github.com/rudderlabs/rudder-server/utils/logger"

// handler is a handle to this object
type handler struct {
	log logger.Logger
	r   Repository
}

func (h *handler) IsSuppressedUser(workspaceID, userID, sourceID string) bool {
	h.log.Debugf("IsSuppressedUser called for workspace: %s, user %s, source %s", workspaceID, sourceID, userID)
	suppressed, err := h.r.Suppress(workspaceID, userID, sourceID)
	if err != nil {
		h.log.Errorf("Suppression check failed for workspace: %s, user: %s, source: %s: %w", workspaceID, userID, sourceID, err)
		return false
	}
	return suppressed
}
