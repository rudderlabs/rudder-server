package slave

import (
	"context"

	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	"github.com/rudderlabs/rudder-server/warehouse/constraints"

	"github.com/rudderlabs/rudder-server/warehouse/encoding"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type slaveNotifier interface {
	Subscribe(ctx context.Context, workerId string, jobsBufferSize int) <-chan *notifier.ClaimJob
	RunMaintenance(ctx context.Context) error
	UpdateClaim(ctx context.Context, job *notifier.ClaimJob, response *notifier.ClaimJobResponse)
}

type Slave struct {
	conf               *config.Config
	log                logger.Logger
	stats              stats.Stats
	notifier           slaveNotifier
	bcManager          *bcm.BackendConfigManager
	constraintsManager *constraints.Manager
	encodingFactory    *encoding.Factory

	config struct {
		noOfSlaveWorkerRoutines config.ValueLoader[int]
	}
}

func New(
	conf *config.Config,
	logger logger.Logger,
	stats stats.Stats,
	notifier slaveNotifier,
	bcManager *bcm.BackendConfigManager,
	constraintsManager *constraints.Manager,
	encodingFactory *encoding.Factory,
) *Slave {
	s := &Slave{}

	s.conf = conf
	s.log = logger
	s.stats = stats
	s.notifier = notifier
	s.bcManager = bcManager
	s.constraintsManager = constraintsManager
	s.encodingFactory = encodingFactory
	s.config.noOfSlaveWorkerRoutines = conf.GetReloadableIntVar(4, 1, "Warehouse.noOfSlaveWorkerRoutines")

	return s
}

func (s *Slave) SetupSlave(ctx context.Context) error {
	slaveID := misc.FastUUID().String()

	jobNotificationChannel := s.notifier.Subscribe(ctx, slaveID, s.config.noOfSlaveWorkerRoutines.Load())

	g, gCtx := errgroup.WithContext(ctx)

	for workerIdx := 0; workerIdx <= s.config.noOfSlaveWorkerRoutines.Load()-1; workerIdx++ {
		idx := workerIdx

		g.Go(crash.NotifyWarehouse(func() error {
			slaveWorker := newWorker(s.conf, s.log, s.stats, s.notifier, s.bcManager, s.constraintsManager, s.encodingFactory, idx)
			slaveWorker.start(gCtx, jobNotificationChannel, slaveID)
			return nil
		}))
	}

	g.Go(crash.NotifyWarehouse(func() error {
		return s.notifier.RunMaintenance(gCtx)
	}))

	return g.Wait()
}
