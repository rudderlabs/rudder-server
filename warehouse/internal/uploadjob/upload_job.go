package uploadjob

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/alerta"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/utils/stats"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/internal/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/schema"
	"github.com/rudderlabs/rudder-server/warehouse/internal/validations"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

// UploadJob represents a warehouse upload job
type UploadJob struct {
	ctx                  context.Context
	db                   *sqlquerywrapper.DB
	reporting            types.Reporting
	destinationValidator validations.DestinationValidator
	loadfile             *loadfiles.LoadFileGenerator
	tableUploadsRepo     *repo.TableUploads
	uploadsRepo          *repo.Uploads
	stagingFileRepo      *repo.StagingFiles
	loadFilesRepo        *repo.LoadFiles
	whSchemaRepo         *repo.WHSchema
	whManager            manager.Manager
	schemaHandle         schema.Handler
	conf                 *config.Config
	logger               logger.Logger
	statsFactory         stats.Stats

	upload         model.Upload
	warehouse      model.Warehouse
	stagingFiles   []*model.StagingFile
	stagingFileIDs []int64
	alertSender    alerta.AlertSender
	now            func() time.Time

	pendingTableUploads      []model.PendingTableUpload
	pendingTableUploadsRepo  pendingTableUploadsRepo
	pendingTableUploadsOnce  sync.Once
	pendingTableUploadsError error

	config struct {
		refreshPartitionBatchSize           int
		retryTimeWindow                     time.Duration
		minRetryAttempts                    int
		disableAlter                        bool
		minUploadBackoff                    time.Duration
		maxUploadBackoff                    time.Duration
		alwaysRegenerateAllLoadFiles        bool
		reportingEnabled                    bool
		maxParallelLoadsWorkspaceIDs        map[string]interface{}
		columnsBatchSize                    int
		longRunningUploadStatThresholdInMin time.Duration
		skipPreviouslyFailedTables          bool
	}

	errorHandler    ErrorHandler
	encodingFactory *encoding.Factory
}

// UploadJobFactory creates new upload jobs
type UploadJobFactory struct {
	Reporting            types.Reporting
	DB                   *sqlquerywrapper.DB
	DestinationValidator validations.DestinationValidator
	LoadFile             *loadfiles.LoadFileGenerator
	Conf                 *config.Config
	Logger               logger.Logger
	StatsFactory         stats.Stats
	EncodingFactory      *encoding.Factory
}

// NewUploadJobFactory creates a new upload job factory
func NewUploadJobFactory(
	reporting types.Reporting,
	db *sqlquerywrapper.DB,
	destinationValidator validations.DestinationValidator,
	loadFile *loadfiles.LoadFileGenerator,
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	encodingFactory *encoding.Factory,
) *UploadJobFactory {
	return &UploadJobFactory{
		Reporting:            reporting,
		DB:                   db,
		DestinationValidator: destinationValidator,
		LoadFile:             loadFile,
		Conf:                 conf,
		Logger:               logger,
		StatsFactory:         statsFactory,
		EncodingFactory:      encodingFactory,
	}
}

// NewUploadJob creates a new upload job
func (f *UploadJobFactory) NewUploadJob(ctx context.Context, dto *model.UploadJob, whManager manager.Manager) *UploadJob {
	ujCtx := utils.CtxWithUploadID(ctx, dto.Upload.ID)

	log := f.Logger.With(
		"upload_job_id", dto.Upload.ID,
		"namespace", dto.Warehouse.Namespace,
		"source_id", dto.Warehouse.Source.ID,
		"source_type", dto.Warehouse.Source.SourceDefinition.Name,
		"destination_id", dto.Warehouse.Destination.ID,
		"destination_type", dto.Warehouse.Destination.DestinationDefinition.Name,
		"workspace_id", dto.Upload.WorkspaceID,
		"use_rudder_storage", dto.Upload.UseRudderStorage,
	)

	uj := &UploadJob{
		ctx:                  ujCtx,
		reporting:            f.Reporting,
		db:                   f.DB,
		loadfile:             f.LoadFile,
		whManager:            whManager,
		destinationValidator: f.DestinationValidator,
		conf:                 f.Conf,
		logger:               log,
		statsFactory:         f.StatsFactory,
		tableUploadsRepo:     repo.NewTableUploads(f.DB, f.Conf),
		uploadsRepo:          repo.NewUploads(f.DB),
		stagingFileRepo:      repo.NewStagingFiles(f.DB),
		loadFilesRepo:        repo.NewLoadFiles(f.DB, f.Conf),
		whSchemaRepo:         repo.NewWHSchemas(f.DB),
		upload:               dto.Upload,
		warehouse:            dto.Warehouse,
		stagingFiles:         dto.StagingFiles,
		stagingFileIDs:       repo.StagingFileIDs(dto.StagingFiles),

		pendingTableUploadsRepo: repo.NewUploads(f.DB),
		pendingTableUploads:     []model.PendingTableUpload{},

		alertSender: alerta.NewClient(
			f.Conf.GetString("ALERTA_URL", "https://alerta.rudderstack.com/api/"),
		),
		now: timeutil.Now,

		errorHandler:    ErrorHandler{Mapper: whManager},
		encodingFactory: f.EncodingFactory,
	}

	uj.config.refreshPartitionBatchSize = f.Conf.GetInt("Warehouse.refreshPartitionBatchSize", 100)
	uj.config.minRetryAttempts = f.Conf.GetInt("Warehouse.minRetryAttempts", 3)
	uj.config.retryTimeWindow = f.Conf.GetDurationVar(24, time.Hour, "Warehouse.retryTimeWindow", "Warehouse.retryTimeWindowInHr")
	uj.config.disableAlter = f.Conf.GetBool("Warehouse.disableAlter", false)
	uj.config.minUploadBackoff = f.Conf.GetDurationVar(1, time.Minute, "Warehouse.minUploadBackoff", "Warehouse.minUploadBackoffInMin")
	uj.config.maxUploadBackoff = f.Conf.GetDurationVar(180, time.Minute, "Warehouse.maxUploadBackoff", "Warehouse.maxUploadBackoffInMin")
	uj.config.alwaysRegenerateAllLoadFiles = f.Conf.GetBool("Warehouse.alwaysRegenerateAllLoadFiles", false)
	uj.config.reportingEnabled = f.Conf.GetBool("Warehouse.reportingEnabled", true)
	uj.config.maxParallelLoadsWorkspaceIDs = f.Conf.GetStringMap("Warehouse.maxParallelLoadsWorkspaceIDs", map[string]interface{}{})
	uj.config.columnsBatchSize = f.Conf.GetInt("Warehouse.columnsBatchSize", 100)
	uj.config.longRunningUploadStatThresholdInMin = f.Conf.GetDurationVar(60, time.Minute, "Warehouse.longRunningUploadStatThresholdInMin", "Warehouse.longRunningUploadStatThresholdInMin")
	uj.config.skipPreviouslyFailedTables = f.Conf.GetBool("Warehouse.skipPreviouslyFailedTables", false)

	return uj
}

// Run executes the upload job
func (job *UploadJob) Run() (err error) {
	start := job.now()
	ch := job.trackLongRunningUpload()
	defer func() {
		_ = job.uploadsRepo.Update(
			job.ctx,
			job.upload.ID,
			[]repo.UpdateKeyValue{
				repo.UploadFieldInProgress(false),
			},
		)

		job.stats.uploadTime.Since(start)
		ch <- struct{}{}
	}()

	job.logger.Infon("Starting upload job")
	_ = job.uploadsRepo.Update(
		job.ctx,
		job.upload.ID,
		[]repo.UpdateKeyValue{
			repo.UploadFieldLastExecAt(job.now()),
			repo.UploadFieldInProgress(true),
		},
	)
	job.logger.Infon("Upload job is in progress")

	if len(job.stagingFiles) == 0 {
		err := fmt.Errorf("no staging files found")
		_, _ = job.setUploadError(err, model.InternalProcessingFailed)
		return err
	}

	whManager := job.whManager
	whManager.SetConnectionTimeout(utils.GetConnectionTimeout(
		job.warehouse.Type, job.warehouse.Destination.ID,
	))

	job.logger.Infon("Setting up warehouse manager")
	err = whManager.Setup(job.ctx, job.warehouse, job)
	if err != nil {
		_, _ = job.setUploadError(err, model.InternalProcessingFailed)
		return err
	}
	defer whManager.Cleanup(job.ctx)

	job.schemaHandle, err = schema.New(
		job.ctx,
		job.warehouse,
		job.conf,
		job.logger.Child("warehouse"),
		job.statsFactory,
		whManager,
		repo.NewWHSchemas(job.db),
		repo.NewStagingFiles(job.db),
	)
	if err != nil {
		_, _ = job.setUploadError(err, model.InternalProcessingFailed)
		return err
	}

	var (
		newStatus       string
		nextUploadState *state
	)
	nextUploadState = nextState(job.upload.Status)
	if nextUploadState == nil {
		nextUploadState = stateTransitions[model.GeneratedUploadSchema]
	}

	for nextUploadState != nil {
		job.logger.Infon("Processing upload state: %s", nextUploadState.name)
		newStatus, err = nextUploadState.handler(job)
		if err != nil {
			_, _ = job.setUploadError(err, newStatus)
			return err
		}

		nextUploadState = nextState(newStatus)
	}

	return nil
}

// DTO returns the upload job data transfer object
func (job *UploadJob) DTO() *model.UploadJob {
	return &model.UploadJob{
		Warehouse:    job.warehouse,
		Upload:       job.upload,
		StagingFiles: job.stagingFiles,
	}
}

// GetLoadFilesMetadata returns the load files metadata
func (job *UploadJob) GetLoadFilesMetadata(ctx context.Context, options utils.GetLoadFilesOptions) (loadFiles []utils.LoadFile, err error) {
	return job.loadFilesRepo.Get(ctx, job.upload.ID, options)
}

// IsWarehouseSchemaEmpty returns true if the warehouse schema is empty
func (job *UploadJob) IsWarehouseSchemaEmpty() bool {
	return job.schemaHandle.IsSchemaEmpty(job.ctx)
}

// GetTableSchemaInWarehouse returns the table schema in the warehouse
func (job *UploadJob) GetTableSchemaInWarehouse(tableName string) model.TableSchema {
	return job.schemaHandle.GetTableSchema(job.ctx, tableName)
}

// GetTableSchemaInUpload returns the table schema in the upload
func (job *UploadJob) GetTableSchemaInUpload(tableName string) model.TableSchema {
	return job.upload.UploadSchema[tableName]
}

// GetSingleLoadFile returns a single load file
func (job *UploadJob) GetSingleLoadFile(ctx context.Context, tableName string) (utils.LoadFile, error) {
	var (
		tableUpload model.TableUpload
		err         error
	)

	if tableUpload, err = job.tableUploadsRepo.GetByUploadIDAndTableName(ctx, job.upload.ID, tableName); err != nil {
		return utils.LoadFile{}, fmt.Errorf("get single load file: %w", err)
	}

	return utils.LoadFile{Location: tableUpload.Location}, err
}

// ShouldOnDedupUseNewRecord returns true if new record should be used on dedup
func (job *UploadJob) ShouldOnDedupUseNewRecord() bool {
	category := job.warehouse.Source.SourceDefinition.Category
	return category == singerProtocolSourceCategory || category == cloudSourceCategory
}

// UseRudderStorage returns true if rudder storage should be used
func (job *UploadJob) UseRudderStorage() bool {
	return job.upload.UseRudderStorage
}

// GetLoadFileType returns the load file type
func (job *UploadJob) GetLoadFileType() string {
	return job.upload.LoadFileType
}

// GetLocalSchema returns the local schema
func (job *UploadJob) GetLocalSchema(ctx context.Context) (model.Schema, error) {
	whSchema, err := job.whSchemaRepo.GetForNamespace(
		ctx,
		job.warehouse.Source.ID,
		job.warehouse.Destination.ID,
		job.warehouse.Namespace,
	)
	if err != nil {
		return nil, fmt.Errorf("getting schema for namespace: %w", err)
	}
	if whSchema.Schema == nil {
		return model.Schema{}, nil
	}
	return whSchema.Schema, nil
}

// UpdateLocalSchema updates the local schema
func (job *UploadJob) UpdateLocalSchema(ctx context.Context, schema model.Schema) error {
	return job.schemaHandle.UpdateSchema(ctx, schema)
}
