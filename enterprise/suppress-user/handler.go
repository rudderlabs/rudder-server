package suppression

import (
	"errors"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
)

// newHandler creates a new handler for the suppression feature
func newHandler(r Repository, log logger.Logger) *handler {
	h := &handler{
		r:   r,
		log: log,
	}
	return h
}

// handler is a handle to this object
type handler struct {
	log logger.Logger
	r   Repository
}

func (h *handler) GetSuppressedUser(workspaceID, userID, sourceID string) *model.Metadata {
	h.log.Debugf("GetSuppressedUser called for workspace: %s, user %s, source %s", workspaceID, userID, sourceID)
	metadata, err := h.r.Suppressed(workspaceID, userID, sourceID)
	if err != nil && !errors.Is(err, model.ErrRestoring) && !errors.Is(err, model.ErrKeyNotFound) {
		h.log.Errorf("Suppression check failed for workspace: %s, user: %s, source: %s: %w", workspaceID, userID, sourceID, err)
	}
	return metadata
}
