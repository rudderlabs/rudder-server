package replay

import (
	"context"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	pkgLogger     logger.LoggerI
	replayEnabled bool
)

func loadConfig() {
	replayEnabled = config.GetBool("Replay.enabled", types.DEFAULT_REPLAY_ENABLED)
	config.RegisterIntConfigVariable(200, &userTransformBatchSize, true, 1, "Processor.userTransformBatchSize")
}

func initFileManager() (filemanager.FileManager, string, error) {
	bucket := strings.TrimSpace(config.GetEnv("JOBS_BACKUP_BUCKET", ""))
	if bucket == "" {
		pkgLogger.Error("[[ Replay ]] JOBS_BACKUP_BUCKET is not set")
		panic("Bucket is not configured.")
	}

	provider := config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
	fileManagerFactory := filemanager.DefaultFileManagerFactory

	startTimeStr := strings.TrimSpace(config.GetEnv("START_TIME", "2000-10-02T15:04:05.000Z"))
	startTime, err := time.Parse(misc.RFC3339Milli, startTimeStr)
	if err != nil {
		pkgLogger.Errorf("[[ Replay ]] Error parsing START_TIME: %s", err.Error())
		return nil, "", err
	}

	configFromEnv := filemanager.GetProviderConfigFromEnv()
	configFromEnv["startAfter"] = startTime.UnixNano() / int64(time.Millisecond)
	uploader, err := fileManagerFactory.New(&filemanager.SettingsT{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           configFromEnv,
			UseRudderStorage: false,
		}),
	})
	if err != nil {
		pkgLogger.Errorf("[[ Replay ]] Error creating file manager: %s", err.Error())
		return nil, "", err
	}

	return uploader, bucket, nil
}

func setup(ctx context.Context, replayDB, gwDB, routerDB, batchRouterDB *jobsdb.HandleT) error {
	tablePrefix := config.GetEnv("TO_REPLAY", "gw")
	replayToDB := config.GetEnv("REPLAY_TO_DB", "gw")
	pkgLogger.Infof("TO_REPLAY=%s and REPLAY_TO_DB=%s", tablePrefix, replayToDB)
	var dumpsLoader dumpsLoaderHandleT
	uploader, bucket, err := initFileManager()
	if err != nil {
		return err
	}

	dumpsLoader.Setup(ctx, replayDB, tablePrefix, uploader, bucket)

	var replayer Handler
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
	_ = toDB.Start()
	replayer.Setup(ctx, &dumpsLoader, replayDB, toDB, tablePrefix, uploader, bucket)
	return nil
}

type Factory struct {
	EnterpriseToken string
}

// Setup initializes Replay feature
func (m *Factory) Setup(ctx context.Context, replayDB, gwDB, routerDB, batchRouterDB *jobsdb.HandleT) {
	if m.EnterpriseToken == "" {
		return
	}

	loadConfig()
	pkgLogger = logger.NewLogger().Child("enterprise").Child("replay")
	if replayEnabled {
		pkgLogger.Info("[[ Replay ]] Setting up Replay")
		err := setup(ctx, replayDB, gwDB, routerDB, batchRouterDB)
		if err != nil {
			panic(err)
		}
	}
}
