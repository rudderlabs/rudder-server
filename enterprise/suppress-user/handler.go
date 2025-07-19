package suppression

import (
	"errors"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
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
	log := h.log.Withn(obskit.WorkspaceID(workspaceID), obskit.SourceID(sourceID), logger.NewStringField("userID", userID))
	log.Debugn("GetSuppressedUser called for workspace")
	metadata, err := h.r.Suppressed(workspaceID, userID, sourceID)
	if err != nil && !errors.Is(err, model.ErrRestoring) && !errors.Is(err, model.ErrKeyNotFound) {
		log.Errorn("Suppression check failed for workspace", obskit.Error(err))
	}
	return metadata
}
