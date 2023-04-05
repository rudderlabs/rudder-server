package jobforwarder

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/internal/pulsar"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/forwarder/baseforwarder"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/schematransformer"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type JobsForwarder struct {
	baseforwarder.BaseForwarder
	pulsarProducer   pulsar.ProducerAdapter
	transientSources transientsource.Service
	backendConfig    backendconfig.BackendConfig
	transformer      schematransformer.Transformer
}

func New(ctx context.Context, g *errgroup.Group, schemaDB jobsdb.JobsDB, transientSources transientsource.Service, backendConfig backendconfig.BackendConfig, log logger.Logger) (*JobsForwarder, error) {
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
	forwarder.backendConfig = backendConfig
	forwarder.transformer = schematransformer.New(ctx, g, backendConfig)
	return &forwarder, nil
}

func (jf *JobsForwarder) Start(ctx context.Context) {
	jf.ErrGroup.Go(misc.WithBugsnag(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				jobs, limitReached, err := jf.GetJobs(ctx)
				if err != nil {
					return err
				}
				for _, job := range jobs {
					transformedBytes, err := jf.transformer.Transform(job)
					if err != nil {
						return err
					}
					jf.pulsarProducer.SendMessageAsync(ctx, transformedBytes)
					err = jf.pulsarProducer.Flush()
					if err != nil {
						return err
					}
				}
				time.Sleep(jf.GetSleepTime(limitReached))
			}
		}
	}))
}

func (jf *JobsForwarder) Stop() {
	jf.pulsarProducer.Close()
}
