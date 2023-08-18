package warehouse

import (
	"context"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type slaveNotifier interface {
	Subscribe(ctx context.Context, workerId string, jobsBufferSize int) chan pgnotifier.Claim
	RunMaintenanceWorker(ctx context.Context) error
	UpdateClaimedEvent(claim *pgnotifier.Claim, response *pgnotifier.ClaimResponse)
}

type slave struct {
	conf               *config.Config
	log                logger.Logger
	stats              stats.Stats
	notifier           slaveNotifier
	bcManager          *backendConfigManager
	constraintsManager *constraintsManager
	encodingFactory    *encoding.Factory

	config struct {
		noOfSlaveWorkerRoutines int
	}
}

func newSlave(
	conf *config.Config,
	logger logger.Logger,
	stats stats.Stats,
	notifier slaveNotifier,
	bcManager *backendConfigManager,
	constraintsManager *constraintsManager,
	encodingFactory *encoding.Factory,
) *slave {
	s := &slave{}

	s.conf = conf
	s.log = logger
	s.stats = stats
	s.notifier = notifier
	s.bcManager = bcManager
	s.constraintsManager = constraintsManager
	s.encodingFactory = encodingFactory

	conf.RegisterIntConfigVariable(4, &s.config.noOfSlaveWorkerRoutines, true, 1, "Warehouse.noOfSlaveWorkerRoutines")

	return s
}

func (s *slave) setupSlave(ctx context.Context) error {
	slaveID := misc.FastUUID().String()

	jobNotificationChannel := s.notifier.Subscribe(ctx, slaveID, s.config.noOfSlaveWorkerRoutines)

	g, gCtx := errgroup.WithContext(ctx)

	for workerIdx := 0; workerIdx <= s.config.noOfSlaveWorkerRoutines-1; workerIdx++ {
		idx := workerIdx

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			slaveWorker := newSlaveWorker(s.conf, s.log, s.stats, s.notifier, s.bcManager, s.constraintsManager, s.encodingFactory, idx)
			slaveWorker.start(gCtx, jobNotificationChannel, slaveID)
			return nil
		}))
	}

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		return s.notifier.RunMaintenanceWorker(gCtx)
	}))

	return g.Wait()
}
