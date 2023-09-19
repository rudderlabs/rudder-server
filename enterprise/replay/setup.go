package replay

import (
	"context"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var replayEnabled bool

func loadConfig() {
	replayEnabled = config.GetBool("Replay.enabled", types.DefaultReplayEnabled)
	userTransformBatchSize = config.GetReloadableIntVar(200, 1, "Processor.userTransformBatchSize")
}

func initFileManager(log logger.Logger) (filemanager.FileManager, string, error) {
	bucket := strings.TrimSpace(config.GetString("JOBS_REPLAY_BACKUP_BUCKET", ""))
	if bucket == "" {
		log.Error("[[ Replay ]] JOBS_REPLAY_BACKUP_BUCKET is not set")
		panic("Bucket is not configured.")
	}

	provider := config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
	uploader, err := filemanager.New(&filemanager.Settings{
		Provider: provider,
		Config:   filemanager.GetProviderConfigFromEnv(filemanagerutil.ProviderConfigOpts(context.TODO(), provider, config.Default)),
		Conf:     config.Default,
	})
	if err != nil {
		log.Errorf("[[ Replay ]] Error creating file manager: %s", err.Error())
		return nil, "", err
	}

	return uploader, bucket, nil
}

func setup(ctx context.Context, replayDB, gwDB, routerDB, batchRouterDB *jobsdb.Handle, log logger.Logger) error {
	tablePrefix := config.GetString("TO_REPLAY", "gw")
	replayToDB := config.GetString("REPLAY_TO_DB", "gw")
	log.Infof("TO_REPLAY=%s and REPLAY_TO_DB=%s", tablePrefix, replayToDB)
	var dumpsLoader dumpsLoaderHandleT
	uploader, bucket, err := initFileManager(log)
	if err != nil {
		return err
	}

	dumpsLoader.Setup(ctx, replayDB, tablePrefix, uploader, bucket, log)

	var replayer Handler
	var toDB *jobsdb.Handle
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
	_ = toDB.Start()
	replayer.Setup(ctx, &dumpsLoader, replayDB, toDB, tablePrefix, uploader, bucket, log)
	return nil
}

type Factory struct {
	EnterpriseToken string
	Log             logger.Logger
}

// Setup initializes Replay feature
func (m *Factory) Setup(ctx context.Context, replayDB, gwDB, routerDB, batchRouterDB *jobsdb.Handle) {
	if m.Log == nil {
		m.Log = logger.NewLogger().Child("enterprise").Child("replay")
	}
	if m.EnterpriseToken == "" {
		return
	}

	loadConfig()
	if replayEnabled {
		m.Log.Info("[[ Replay ]] Setting up Replay")
		err := setup(ctx, replayDB, gwDB, routerDB, batchRouterDB, m.Log)
		if err != nil {
			panic(err)
		}
	}
}
