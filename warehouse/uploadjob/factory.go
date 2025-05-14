package uploadjob

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/alerta"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type Factory struct {
	reporting            types.Reporting
	db                   *sqlquerywrapper.DB
	destinationValidator validations.DestinationValidator
	loadFile             loadfiles.Generator
	conf                 *config.Config
	logger               logger.Logger
	statsFactory         stats.Stats
	encodingFactory      *encoding.Factory
}

func NewFactory(
	reporting types.Reporting,
	db *sqlquerywrapper.DB,
	destinationValidator validations.DestinationValidator,
	loadFile loadfiles.Generator,
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	encodingFactory *encoding.Factory,
) *Factory {
	return &Factory{
		reporting:            reporting,
		db:                   db,
		destinationValidator: destinationValidator,
		loadFile:             loadFile,
		conf:                 conf,
		logger:               logger,
		statsFactory:         statsFactory,
		encodingFactory:      encodingFactory,
	}
}

func (f *Factory) NewUploadJob(ctx context.Context, dto *model.UploadJob, whManager manager.Manager) *UploadJob {
	ujCtx := whutils.CtxWithUploadID(ctx, dto.Upload.ID)

	log := f.logger.With(
		logfield.UploadJobID, dto.Upload.ID,
		logfield.Namespace, dto.Warehouse.Namespace,
		logfield.SourceID, dto.Warehouse.Source.ID,
		logfield.SourceType, dto.Warehouse.Source.SourceDefinition.Name,
		logfield.DestinationID, dto.Warehouse.Destination.ID,
		logfield.DestinationType, dto.Warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, dto.Upload.WorkspaceID,
		logfield.UseRudderStorage, dto.Upload.UseRudderStorage,
	)

	uj := &UploadJob{
		ctx:                  ujCtx,
		reporting:            f.reporting,
		db:                   f.db,
		loadfile:             f.loadFile,
		whManager:            whManager,
		destinationValidator: f.destinationValidator,
		conf:                 f.conf,
		logger:               log,
		statsFactory:         f.statsFactory,
		tableUploadsRepo:     repo.NewTableUploads(f.db, f.conf),
		uploadsRepo:          repo.NewUploads(f.db),
		stagingFileRepo:      repo.NewStagingFiles(f.db),
		loadFilesRepo:        repo.NewLoadFiles(f.db, f.conf),
		whSchemaRepo:         repo.NewWHSchemas(f.db),
		upload:               dto.Upload,
		warehouse:            dto.Warehouse,
		stagingFiles:         dto.StagingFiles,
		stagingFileIDs:       repo.StagingFileIDs(dto.StagingFiles),

		pendingTableUploadsRepo: repo.NewUploads(f.db),
		pendingTableUploads:     []model.PendingTableUpload{},

		alertSender: alerta.NewClient(
			f.conf.GetString("ALERTA_URL", "https://alerta.rudderstack.com/api/"),
		),
		now: timeutil.Now,

		errorHandler:    ErrorHandler{Mapper: whManager},
		encodingFactory: f.encodingFactory,
	}

	uj.config.refreshPartitionBatchSize = f.conf.GetInt("Warehouse.refreshPartitionBatchSize", 100)
	uj.config.minRetryAttempts = f.conf.GetInt("Warehouse.minRetryAttempts", 3)
	uj.config.disableAlter = f.conf.GetBool("Warehouse.disableAlter", false)
	uj.config.alwaysRegenerateAllLoadFiles = f.conf.GetBool("Warehouse.alwaysRegenerateAllLoadFiles", true)
	uj.config.reportingEnabled = f.conf.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
	uj.config.columnsBatchSize = f.conf.GetInt(fmt.Sprintf("Warehouse.%s.columnsBatchSize", whutils.WHDestNameMap[uj.upload.DestinationType]), 100)
	uj.config.maxParallelLoadsWorkspaceIDs = f.conf.GetStringMap(fmt.Sprintf("Warehouse.%s.maxParallelLoadsWorkspaceIDs", whutils.WHDestNameMap[uj.upload.DestinationType]), nil)
	uj.config.longRunningUploadStatThresholdInMin = f.conf.GetDurationVar(120, time.Minute, "Warehouse.longRunningUploadStatThreshold", "Warehouse.longRunningUploadStatThresholdInMin")
	uj.config.minUploadBackoff = f.conf.GetDurationVar(60, time.Second, "Warehouse.minUploadBackoff", "Warehouse.minUploadBackoffInS")
	uj.config.maxUploadBackoff = f.conf.GetDurationVar(1800, time.Second, "Warehouse.maxUploadBackoff", "Warehouse.maxUploadBackoffInS")
	uj.config.retryTimeWindow = f.conf.GetDurationVar(180, time.Minute, "Warehouse.retryTimeWindow", "Warehouse.retryTimeWindowInMins")
	uj.config.skipPreviouslyFailedTables = f.conf.GetBool("Warehouse.skipPreviouslyFailedTables", false)

	uj.stats.uploadTime = uj.timerStat("upload_time")
	uj.stats.userTablesLoadTime = uj.timerStat("user_tables_load_time")
	uj.stats.identityTablesLoadTime = uj.timerStat("identity_tables_load_time")
	uj.stats.otherTablesLoadTime = uj.timerStat("other_tables_load_time")
	uj.stats.loadFileGenerationTime = uj.timerStat("load_file_generation_time")
	uj.stats.tablesAdded = uj.counterStat("tables_added")
	uj.stats.columnsAdded = uj.counterStat("columns_added")
	uj.stats.uploadFailed = uj.counterStat("warehouse_failed_uploads")
	uj.stats.totalRowsSynced = uj.counterStat("total_rows_synced")
	uj.stats.numStagedEvents = uj.counterStat("num_staged_events")
	uj.stats.uploadSuccess = uj.counterStat("upload_success")
	uj.stats.stagingLoadFileEventsCountMismatch = uj.gaugeStat(
		"warehouse_staging_load_file_events_count_mismatched",
		whutils.Tag{Name: "sourceCategory", Value: uj.warehouse.Source.SourceDefinition.Category},
	)

	syncFrequency := "1440" // 24h
	if frequency := uj.warehouse.GetStringDestinationConfig(uj.conf, model.SyncFrequencySetting); frequency != "" {
		syncFrequency = frequency
	}
	uj.stats.eventDeliveryTime = uj.timerStat("event_delivery_time",
		whutils.Tag{Name: "syncFrequency", Value: syncFrequency},
		whutils.Tag{Name: "sourceCategory", Value: uj.warehouse.Source.SourceDefinition.Category},
	)

	return uj
}
