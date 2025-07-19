package delete

import (
	"context"
	"strings"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

var pkgLogger = logger.NewLogger().Child("client")

//go:generate mockgen -source=delete.go -destination=mock_delete_test.go -package=delete github.com/rudderlabs/rudder-server/regulation-worker/internal/delete
type deleteManager interface {
	Delete(ctx context.Context, job model.Job, destDetail model.Destination) model.JobStatus
	GetSupportedDestinations() []string
}

type Router struct {
	Managers []deleteManager
	router   map[string]deleteManager
	once     sync.Once
}

func NewRouter(managers ...deleteManager) *Router {
	return &Router{
		Managers: managers,
	}
}

func (r *Router) Delete(ctx context.Context, job model.Job, dest model.Destination) model.JobStatus {
	pkgLogger.Debugn("deleting job from destination",
		logger.NewIntField("jobId", int64(job.ID)),
		logger.NewStringField("destination", dest.DestinationID),
	)
	r.once.Do(func() {
		pkgLogger.Infon("getting all the supported destination")
		r.router = make(map[string]deleteManager, len(r.Managers))

		for _, m := range r.Managers {
			destinations := m.GetSupportedDestinations()
			pkgLogger.Infon("got deletion manager supporting deletion from destinations",
				logger.NewStringField("supportedDestinations", strings.Join(destinations, ",")),
			)
			for _, d := range destinations {
				r.router[d] = m
			}
		}
	})
	if _, ok := r.router[dest.Name]; ok {
		pkgLogger.Debugn("calling deletion manager",
			logger.NewStringField("destinationName", dest.Name),
		)
		return r.router[dest.Name].Delete(ctx, job, dest)
	}

	pkgLogger.Errorn("no deletion manager support deletion from destination",
		logger.NewStringField("destinationName", dest.Name),
	)
	return model.JobStatus{Status: model.JobStatusAborted, Error: model.ErrDestNotSupported}
}
