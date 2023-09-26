package replay

import (
	"context"
	"errors"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/enterprise/replay/dumpsloader"
	"github.com/rudderlabs/rudder-server/enterprise/replay/replayer"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func initFileManager(ctx context.Context, config *config.Config, log logger.Logger) (filemanager.FileManager, string, error) {
	bucket := strings.TrimSpace(config.GetString("JOBS_REPLAY_BACKUP_BUCKET", ""))
	if bucket == "" {
		log.Error("[[ Replay ]] JOBS_REPLAY_BACKUP_BUCKET is not set")
		return nil, "", errors.New("JOBS_REPLAY_BACKUP_BUCKET is not set")
	}

	provider := config.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
	uploader, err := filemanager.New(&filemanager.Settings{
		Provider: provider,
		Config:   filemanager.GetProviderConfigFromEnv(filemanagerutil.ProviderConfigOpts(ctx, provider, config)),
		Conf:     config,
	})
	if err != nil {
		log.Errorf("[[ Replay ]] Error creating file manager: %s", err.Error())
		return nil, "", err
	}

	return uploader, bucket, nil
}

type Factory struct {
	EnterpriseToken string
	Log             logger.Logger
}
type replay struct {
	toDB        *jobsdb.Handle
	dumpsLoader dumpsloader.DumpsLoader
	replayer    replayer.Replay
}
type Replay interface {
	Start() error
	Stop() error
}

// Setup initializes Replay feature
func (m *Factory) Setup(ctx context.Context, config *config.Config, replayDB, gwDB, routerDB, batchRouterDB *jobsdb.Handle) (Replay, error) {
	if m.Log == nil {
		m.Log = logger.NewLogger().Child("enterprise").Child("replay")
	}
	if m.EnterpriseToken == "" {
		return nil, errors.New("enterprise token is not set")
	}
	if !config.GetBool("Replay.enabled", types.DefaultReplayEnabled) {
		return nil, errors.New("replay is not enabled")
	}
	m.Log.Info("[[ Replay ]] Setting up Replay")
	tablePrefix := config.GetString("TO_REPLAY", "gw")
	replayToDB := config.GetString("REPLAY_TO_DB", "gw")
	m.Log.Infof("TO_REPLAY=%s and REPLAY_TO_DB=%s", tablePrefix, replayToDB)
	uploader, bucket, err := initFileManager(ctx, config, m.Log)
	if err != nil {
		return nil, err
	}

	dumpsLoader, err := dumpsloader.Setup(ctx, config, replayDB, tablePrefix, uploader, bucket, m.Log)
	if err != nil {
		return nil, err
	}
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
	setup, err := replayer.Setup(ctx, config, dumpsLoader, replayDB, toDB, tablePrefix, uploader, bucket, m.Log)
	if err != nil {
		return nil, err
	}
	return &replay{
		toDB:        toDB,
		dumpsLoader: dumpsLoader,
		replayer:    setup,
	}, nil
}

func (r *replay) Start() error {
	err := r.toDB.Start()
	if err != nil {
		return err
	}
	r.dumpsLoader.Start()
	r.replayer.Start()
	return nil
}

func (r *replay) Stop() error {
	err := r.replayer.Stop()
	if err != nil {
		return err
	}
	err = r.dumpsLoader.Stop()
	if err != nil {
		return err
	}
	r.toDB.Stop()
	return nil
}
