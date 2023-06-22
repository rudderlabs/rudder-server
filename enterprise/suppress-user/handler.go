package suppression

import (
	"errors"
	"time"

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

func (h *handler) IsSuppressedUser(workspaceID, userID, sourceID string) bool {
	h.log.Debugf("IsSuppressedUser called for workspace: %s, user %s, source %s", workspaceID, userID, sourceID)
	suppressed, err := h.r.Suppressed(workspaceID, userID, sourceID)
	if err != nil && !errors.Is(err, model.ErrRestoring) {
		h.log.Errorf("Suppression check failed for workspace: %s, user: %s, source: %s: %w", workspaceID, userID, sourceID, err)
	}
	return suppressed
}

func (h *handler) GetCreatedAt(workspaceID, userID, sourceID string) time.Time {
	h.log.Debugf("GetCreatedAt called for workspace: %s, user %s, source %s", workspaceID, userID, sourceID)
	createdAt, err := h.r.GetCreatedAt(workspaceID, userID, sourceID)
	if err != nil && !errors.Is(err, model.ErrRestoring) {
		h.log.Errorf("GetCreatedAt failed for workspace: %s, user: %s, source: %s: %w", workspaceID, userID, sourceID, err)
	}
	return createdAt
}
