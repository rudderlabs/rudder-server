package suppression

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/samber/lo"
)

// newHandler creates a new handler for the suppression feature
func newHandler(ctx context.Context, r Repository, log logger.Logger) *handler {
	h := &handler{
		r:   r,
		log: log,
	}
	// we don't want to flood logs if, e.g. the suppression repository is restoring,
	// so we are debouncing the logging
	h.errLog.debounceLog, h.errLog.cancel = lo.NewDebounce(1*time.Second, func() {
		h.errLog.errMu.Lock()
		defer h.errLog.errMu.Unlock()
		if h.errLog.err != nil {
			h.log.Warn(h.errLog.err.Error())
		}
	})
	go func() {
		<-ctx.Done()
		h.errLog.cancel()
	}()
	return h
}

// handler is a handle to this object
type handler struct {
	log    logger.Logger
	r      Repository
	errLog struct {
		debounceLog func() // logs suppression failures with a debounce, once every 1 second
		cancel      func() // cancels the debounce timer
		errMu       sync.Mutex
		err         error // the last error
	}
}

func (h *handler) IsSuppressedUser(workspaceID, userID, sourceID string) bool {
	h.log.Debugf("IsSuppressedUser called for workspace: %s, user %s, source %s", workspaceID, userID, sourceID)
	suppressed, err := h.r.Suppress(workspaceID, userID, sourceID)
	if err != nil {
		h.errLog.errMu.Lock()
		h.errLog.err = fmt.Errorf("suppression check failed for workspace: %s, user: %s, source: %s: %w", workspaceID, userID, sourceID, err)
		h.errLog.debounceLog()
		h.errLog.errMu.Unlock()
	}
	return suppressed
}
