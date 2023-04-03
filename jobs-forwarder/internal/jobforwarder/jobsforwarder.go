package jobforwarder

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/baseforwarder"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type JobsForwarder struct {
	baseforwarder.BaseForwarder
	pulsarProducer   pulsar.ProducerAdapter
	transientSources transientsource.Service
}

func New(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, transientSources transientsource.Service, log logger.Logger) (*JobsForwarder, error) {
	baseForwarder := baseforwarder.BaseForwarder{}
	baseForwarder.LoadMetaData(ctx, g, schemaDB, log)
	forwarder := JobsForwarder{
		transientSources: transientSources,
		BaseForwarder:    baseForwarder,
	}
	client, err := pulsar.New()
	if err != nil {
		return nil, err
	}
	forwarder.pulsarProducer = client

	return &forwarder, nil
}

func (jf *JobsForwarder) Start(ctx context.Context) {
	jf.ErrGroup.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				_, limitReached, err := jf.GetJobs(ctx)
				if err != nil {
					return err
				}
				time.Sleep(jf.GetSleepTime(limitReached))
			}
		}
	}))
}

func (jf *JobsForwarder) Stop() {
	jf.pulsarProducer.Close()
}
