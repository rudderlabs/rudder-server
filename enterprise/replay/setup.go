package replay

import (
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	pkgLogger     logger.LoggerI
	Namespace     string
	InstanceID    string
	replayEnabled bool
)

func loadConfig() {
	Namespace = config.GetKubeNamespace()
	InstanceID = config.GetEnv("INSTANCE_ID", "1")
	replayEnabled = config.GetBool("Replay.enabled", types.DEFAULT_REPLAY_ENABLED)
	config.RegisterIntConfigVariable(200, &userTransformBatchSize, true, 1, "Processor.userTransformBatchSize")
}

func setup(replayDB, gwDB, routerDB, batchRouterDB *jobsdb.HandleT) {
	tablePrefix := config.GetEnv("TO_REPLAY", "gw")
	replayToDB := config.GetEnv("REPLAY_TO_DB", "gw")
	pkgLogger.Infof("brt-debug: REPLAY_TO_DB=%s", replayToDB)
	var dumpsLoader DumpsLoaderHandleT
	dumpsLoader.SetUp(replayDB, tablePrefix)

	var replayer HandleT
	var toDB *jobsdb.HandleT
	switch replayToDB {
	case "gw":
		toDB = gwDB
	case "rt":
		toDB = routerDB
	case "brt":
		toDB = batchRouterDB
	default:
		toDB = routerDB
	}
	toDB.Start()
	replayer.Setup(&dumpsLoader, replayDB, toDB, tablePrefix)
}

type Factory struct {
	EnterpriseToken string
}

// Setup initializes Replay feature
func (m *Factory) Setup(replayDB, gwDB, routerDB, batchRouterDB *jobsdb.HandleT) {
	if m.EnterpriseToken == "" {
		return
	}

	loadConfig()
	pkgLogger = logger.NewLogger().Child("enterprise").Child("replay")

	if replayEnabled {
		pkgLogger.Info("[[ Reporting ]] Setting up reporting handler")
		setup(replayDB, gwDB, routerDB, batchRouterDB)
	}
}
