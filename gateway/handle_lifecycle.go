package gateway

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"

	"github.com/rudderlabs/rudder-server/gateway/validator"

	"github.com/rudderlabs/rudder-server/gateway/webhook/auth"

	"github.com/rudderlabs/rudder-schemas/go/stream"

	"golang.org/x/sync/errgroup"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/cors"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/chiware"
	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/throttler"
	"github.com/rudderlabs/rudder-server/gateway/webhook"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/middleware"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	rsources_http "github.com/rudderlabs/rudder-server/services/rsources/http"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
)

type msgToUpload struct {
	payload []byte
	fields  []logger.Field
}

/*
Setup initializes this module:
- Monitors backend config for changes.
- Starts web request batching goroutine, that batches incoming messages.
- Starts web request batch db writer goroutine, that writes incoming batches to JobsDB.
- Starts debugging goroutine that prints gateway stats.

This function will block until backend config is initially received.
*/
func (gw *Handle) Setup(
	ctx context.Context,
	config *config.Config, logger logger.Logger, stat stats.Stats,
	application app.App, backendConfig backendconfig.BackendConfig, jobsDB, errDB jobsdb.JobsDB,
	rateLimiter throttler.Throttler, versionHandler func(w http.ResponseWriter, r *http.Request),
	rsourcesService rsources.JobService, transformerFeaturesService transformer.FeaturesService,
	sourcehandle sourcedebugger.SourceDebugger, streamMsgValidator func(message *stream.Message) error,
	opts ...OptFunc,
) error {
	gw.config = config
	gw.logger = logger
	gw.stats = stat
	gw.tracer = stat.NewTracer("gateway")
	gw.application = application
	gw.backendConfig = backendConfig
	gw.jobsDB = jobsDB
	gw.errDB = errDB
	gw.rateLimiter = rateLimiter
	gw.versionHandler = versionHandler
	gw.rsourcesService = rsourcesService
	gw.sourcehandle = sourcehandle
	gw.inFlightRequests = new(sync.WaitGroup)

	// Port where GW is running
	gw.conf.webPort = config.GetIntVar(8080, 1, "Gateway.webPort")
	// Number of incoming requests that are batched before handing off to write workers
	gw.conf.maxUserWebRequestBatchSize = config.GetIntVar(128, 1, "Gateway.maxUserRequestBatchSize")
	// Number of userWorkerBatchRequest that are batched before initiating write
	gw.conf.maxDBBatchSize = config.GetIntVar(128, 1, "Gateway.maxDBBatchSize")
	// Multiple workers are used to batch user web requests
	gw.conf.maxUserWebRequestWorkerProcess = config.GetIntVar(64, 1, "Gateway.maxUserWebRequestWorkerProcess")
	// Multiple DB writers are used to write data to DB
	gw.conf.maxDBWriterProcess = config.GetIntVar(256, 1, "Gateway.maxDBWriterProcess")
	// Timeout after which batch is formed anyway with whatever requests are available
	gw.conf.userWebRequestBatchTimeout = config.GetReloadableDurationVar(15, time.Millisecond, "Gateway.userWebRequestBatchTimeout", "Gateway.userWebRequestBatchTimeoutInMS")
	gw.conf.dbBatchWriteTimeout = config.GetReloadableDurationVar(5, time.Millisecond, "Gateway.dbBatchWriteTimeout", "Gateway.dbBatchWriteTimeoutInMS")
	// Enables accepting requests without user id and anonymous id. This is added to prevent client 4xx retries.
	gw.conf.allowReqsWithoutUserIDAndAnonymousID = config.GetReloadableBoolVar(false, "Gateway.allowReqsWithoutUserIDAndAnonymousID")
	gw.conf.gwAllowPartialWriteWithErrors = config.GetReloadableBoolVar(true, "Gateway.allowPartialWriteWithErrors")
	// Maximum request size to gateway
	gw.conf.maxReqSize = config.GetReloadableIntVar(4000, 1024, "Gateway.maxReqSizeInKB")
	// Enable rate limit on incoming events. false by default
	gw.conf.enableRateLimit = config.GetReloadableBoolVar(false, "Gateway.enableRateLimit")
	// Enable suppress user feature. false by default
	gw.conf.enableSuppressUserFeature = config.GetBoolVar(true, "Gateway.enableSuppressUserFeature")
	// Time period for diagnosis ticker
	gw.conf.diagnosisTickerTime = config.GetDurationVar(60, time.Second, "Diagnostics.gatewayTimePeriod", "Diagnostics.gatewayTimePeriodInS")
	gw.conf.ReadTimeout = config.GetDurationVar(0, time.Second, "ReadTimeout", "ReadTimeOutInSec")
	gw.conf.ReadHeaderTimeout = config.GetDurationVar(0, time.Second, "ReadHeaderTimeout", "ReadHeaderTimeoutInSec")
	gw.conf.WriteTimeout = config.GetDurationVar(10, time.Second, "WriteTimeout", "WriteTimeOutInSec")
	gw.conf.IdleTimeout = config.GetDurationVar(720, time.Second, "IdleTimeout", "IdleTimeoutInSec")
	gw.conf.maxHeaderBytes = config.GetIntVar(524288, 1, "MaxHeaderBytes")
	// if set to '0', it means disabled.
	gw.conf.maxConcurrentRequests = config.GetIntVar(50000, 1, "Gateway.maxConcurrentRequests")
	// enable internal batch validator
	// this is used to validate the batch request before sending it to the db writer
	gw.conf.enableInternalBatchValidator = config.GetReloadableBoolVar(true, "gateway.enableMsgValidator")
	// enable internal batch enrichment
	// this is used to enrich the event before sending it to the db writer
	gw.conf.enableInternalBatchEnrichment = config.GetReloadableBoolVar(true, "gateway.enableBatchEnrichment")
	// enable webhook v2 handler. disabled by default
	gw.conf.webhookV2HandlerEnabled = config.GetBoolVar(false, "Gateway.webhookV2HandlerEnabled")
	// enable event blocking. false by default
	gw.conf.enableEventBlocking = config.GetReloadableBoolVar(false, "enableEventBlocking")
	// Registering stats
	gw.batchSizeStat = gw.stats.NewStat("gateway.batch_size", stats.HistogramType)
	gw.requestSizeStat = gw.stats.NewStat("gateway.request_size", stats.HistogramType)
	gw.dbWritesStat = gw.stats.NewStat("gateway.db_writes", stats.CountType)
	gw.dbWorkersBufferFullStat = gw.stats.NewStat("gateway.db_workers_buffer_full", stats.CountType)
	gw.dbWorkersTimeOutStat = gw.stats.NewStat("gateway.db_workers_time_out", stats.CountType)
	gw.bodyReadTimeStat = gw.stats.NewStat("gateway.http_body_read_time", stats.TimerType)
	gw.addToWebRequestQWaitTime = gw.stats.NewStat("gateway.web_request_queue_wait_time", stats.TimerType)
	gw.addToBatchRequestQWaitTime = gw.stats.NewStat("gateway.batch_request_queue_wait_time", stats.TimerType)
	gw.processRequestTime = gw.stats.NewStat("gateway.process_request_time", stats.TimerType)
	gw.emptyAnonIdHeaderStat = gw.stats.NewStat("gateway.empty_anonymous_id_header", stats.CountType)

	gw.now = timeutil.Now
	gw.diagnosisTicker = time.NewTicker(gw.conf.diagnosisTickerTime)
	gw.userWorkerBatchRequestQ = make(chan *userWorkerBatchRequestT, gw.conf.maxDBBatchSize)
	gw.batchUserWorkerBatchRequestQ = make(chan *batchUserWorkerBatchRequestT, gw.conf.maxDBWriterProcess)
	gw.irh = &ImportRequestHandler{Handle: gw}
	gw.rrh = &RegularRequestHandler{Handle: gw}
	gw.webhook = webhook.Setup(gw, transformerFeaturesService, gw.stats, gw.config, newSourceStatReporter)
	whURL, err := url.ParseRequestURI(misc.GetWarehouseURL())
	if err != nil {
		return fmt.Errorf("invalid warehouse URL %s: %w", whURL, err)
	}
	gw.whProxy = httputil.NewSingleHostReverseProxy(whURL)
	if gw.conf.enableSuppressUserFeature && gw.application.Features().SuppressUser != nil {
		gw.suppressUserHandler, err = application.Features().SuppressUser.Setup(ctx, gw.backendConfig)
		if err != nil {
			return fmt.Errorf("could not setup suppress user feature: %w", err)
		}
	}

	for _, opt := range opts {
		opt(gw)
	}

	if streamMsgValidator == nil {
		streamMsgValidator = stream.NewMessageValidator()
	}
	gw.streamMsgValidator = streamMsgValidator

	gw.msgValidator = validator.NewValidateMediator(gw.logger, stream.NewMessagePropertiesValidator())

	gw.webhookAuthMiddleware = auth.NewWebhookAuth(
		func(w http.ResponseWriter, r *http.Request, errorMessage string, authCtx *gwtypes.AuthRequestContext) {
			gw.handleHttpError(w, r, errorMessage)
			gw.handleFailureStats(errorMessage, "webhook", authCtx)
		},
		func(writeKey string) (*gwtypes.AuthRequestContext, error) {
			authCtx := gw.authRequestContextForWriteKey(writeKey)
			if authCtx == nil {
				return nil, auth.ErrSourceNotFound
			}
			return authCtx, nil
		})

	// new bg ctx for leaky logger
	// we don't want to cancel the main context.
	leakyCtx, leakyCancel := context.WithCancel(context.Background())
	leakyUploaderEnabled := gw.config.GetBool("Gateway.leakyUploader.enabled", false)
	var leakyUploaderDone chan struct{}
	var leakyUploaderBuffer chan msgToUpload

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := kitsync.ErrGroupWithContext(ctx)
	gw.backgroundCancel = cancel
	gw.backgroundWait = func() error {
		err := g.Wait()
		if err != nil {
			return err
		}
		if leakyUploaderEnabled {
			leakyCancel()
			<-leakyUploaderDone
			close(leakyUploaderBuffer)
		}
		return nil
	}
	gw.initUserWebRequestWorkers()
	gw.backendConfigInitialisedChan = make(chan struct{})
	gw.transformerFeaturesInitialised = transformerFeaturesService.Wait()

	g.Go(crash.Wrapper(func() error {
		gw.backendConfigSubscriber(ctx)
		return nil
	}))
	g.Go(crash.Wrapper(func() error {
		gw.runUserWebRequestWorkers(ctx)
		return nil
	}))
	g.Go(crash.Wrapper(func() error {
		gw.userWorkerRequestBatcher()
		return nil
	}))
	g.Go(crash.Wrapper(func() error {
		gw.initDBWriterWorkers(ctx)
		return nil
	}))
	g.Go(crash.Wrapper(func() error {
		gw.collectMetrics(ctx)
		return nil
	}))

	if leakyUploaderEnabled {
		leakyUploaderDone = make(chan struct{})
		leakyUploaderBuffer = make(chan msgToUpload, config.GetInt("Gateway.leakyUploader.bufferSize", 1))
		fm, err := getLeakyUploaderFileManager(gw.config, gw.logger)
		if err == nil {
			gw.leakyUploader = func(upload msgToUpload) {
				select {
				case leakyUploaderBuffer <- msgToUpload{payload: upload.payload, fields: upload.fields}:
				default:
					gw.logger.Warnn("leakyUploader buffer full", upload.fields...)
				}
			}
			go leakyUploader(leakyCtx, gw.config, gw.logger.Child("leaky-uploader"), leakyUploaderDone, leakyUploaderBuffer, fm)
		} else {
			gw.logger.Errorn("failed to create leaky uploader in gateway", obskit.Error(err))
		}
	}
	return nil
}

func getLeakyUploaderFileManager(conf *config.Config, log logger.Logger) (filemanager.FileManager, error) {
	var (
		regionHint       = conf.GetStringVar("us-east-1", "Gateway.leakyUploader.Storage.RegionHint", "AWS_S3_REGION_HINT")
		endpoint         = conf.GetString("Gateway.leakyUploader.Storage.Endpoint", "")
		accessKeyID      = conf.GetStringVar("", "Gateway.leakyUploader.Storage.AccessKeyId", "AWS_ACCESS_KEY_ID")
		accessKey        = conf.GetStringVar("", "Gateway.leakyUploader.Storage.AccessKey", "AWS_SECRET_ACCESS_KEY")
		s3ForcePathStyle = conf.GetBool("Gateway.leakyUploader.Storage.S3ForcePathStyle", false)
		disableSSL       = conf.GetBool("Gateway.leakyUploader.Storage.DisableSsl", false)
		enableSSE        = conf.GetBoolVar(false, "Gateway.leakyUploader.Storage.EnableSse", "AWS_ENABLE_SSE")
		useGlue          = conf.GetBool("Gateway.leakyUploader.Storage.UseGlue", false)
		region           = conf.GetStringVar("us-east-1", "Gateway.leakyUploader.Storage.Region", "AWS_DEFAULT_REGION")
		bucket           = conf.GetStringVar("rudder-customer-sample-payloads-us", "Gateway.leakyUploader.Storage.Bucket")
	)

	s3Config := map[string]any{
		"bucketName":       bucket,
		"endpoint":         endpoint,
		"accessKeyID":      accessKeyID,
		"accessKey":        accessKey,
		"s3ForcePathStyle": s3ForcePathStyle,
		"disableSSL":       disableSSL,
		"enableSSE":        enableSSE,
		"regionHint":       regionHint,
		"useGlue":          useGlue,
		"region":           region,
	}
	return filemanager.NewS3Manager(
		conf,
		s3Config,
		log.Withn(logger.NewStringField("component", "leaky-uploader")),
		func() time.Duration {
			return conf.GetDuration("Gateway.leakyUploader.Timeout", 120, time.Second)
		},
	)
}

func leakyUploader(ctx context.Context, conf *config.Config, log logger.Logger, done chan struct{}, uploads <-chan msgToUpload, fm filemanager.FileManager) {
	backoff := conf.GetDuration("Gateway.leakyUploader.backoff", 1, time.Second)
	instanceName := conf.GetString("INSTANCE_ID", "unknown-instance")
	log.Infon("starting leaky payload uploader")
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			return
		case upload := <-uploads:
			fileName := path.Join("gw-failed-events", instanceName, time.Now().Format("2006-01-02"), uuid.New().String())
			uploadedFile, err := fm.UploadReader(ctx, fileName, bytes.NewReader(upload.payload))
			if err != nil {
				log.Errorn("cannot upload payload dump", obskit.Error(err))
				continue
			}
			upload.fields = append(upload.fields, logger.NewStringField("payload", uploadedFile.Location))

			log.Infon("payload dump uploaded", upload.fields...)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
	}
}

type OptFunc func(*Handle)

func WithInternalHttpHandlers(handlers map[string]http.Handler) OptFunc {
	return func(gw *Handle) {
		gw.internalHttpHandlers = handlers
	}
}

func WithNow(now func() time.Time) OptFunc {
	return func(gw *Handle) {
		gw.now = now
	}
}

// initUserWebRequestWorkers initiates `maxUserWebRequestWorkerProcess` number of `webRequestWorkers` that listen on their `webRequestQ` for new WebRequests.
func (gw *Handle) initUserWebRequestWorkers() {
	gw.userWebRequestWorkers = make([]*userWebRequestWorkerT, gw.conf.maxUserWebRequestWorkerProcess)
	for i := 0; i < gw.conf.maxUserWebRequestWorkerProcess; i++ {
		gw.logger.Debug("User Web Request Worker Started", i)
		userWebRequestWorker := &userWebRequestWorkerT{
			webRequestQ:    make(chan *webRequestT, gw.conf.maxUserWebRequestBatchSize),
			batchRequestQ:  make(chan *batchWebRequestT),
			reponseQ:       make(chan map[uuid.UUID]string),
			batchTimeStat:  gw.stats.NewStat("gateway.batch_time", stats.TimerType),
			bufferFullStat: gw.stats.NewStat("gateway.user_request_worker_buffer_full", stats.CountType),
			timeOutStat:    gw.stats.NewStat("gateway.user_request_worker_time_out", stats.CountType),
		}
		gw.userWebRequestWorkers[i] = userWebRequestWorker
	}
}

// processBackendConfig processes backend config data and updates internal maps
func (gw *Handle) processBackendConfig(configData map[string]backendconfig.ConfigT) {
	var (
		writeKeysSourceMap                = map[string]backendconfig.SourceT{}
		sourceIDSourceMap                 = map[string]backendconfig.SourceT{}
		nonEventStreamSources             = map[string]bool{}
		blockedEventsWorkspaceTypeNameMap = map[string]map[string]map[string]bool{}
	)

	for workspaceID, wsConfig := range configData {
		for _, source := range wsConfig.Sources {
			writeKeysSourceMap[source.WriteKey] = source
			sourceIDSourceMap[source.ID] = source
			if !gw.conf.webhookV2HandlerEnabled {
				if source.Enabled && source.SourceDefinition.Category == "webhook" {
					gw.webhook.Register(source.SourceDefinition.Name)
				}
			}
			if source.SourceDefinition.Category != "" && !strings.EqualFold(source.SourceDefinition.Category, webhookSourceCategory) {
				nonEventStreamSources[source.ID] = true
			}
		}

		if len(wsConfig.Settings.EventBlocking.Events) > 0 {
			blockedEventsWorkspaceTypeNameMap[workspaceID] = make(map[string]map[string]bool, len(wsConfig.Settings.EventBlocking.Events))

			for eventType, events := range wsConfig.Settings.EventBlocking.Events {
				blockedEventsWorkspaceTypeNameMap[workspaceID][eventType] = make(map[string]bool, len(events))

				for _, event := range events {
					blockedEventsWorkspaceTypeNameMap[workspaceID][eventType][event] = true
				}
			}
		}
	}

	gw.configSubscriberLock.Lock()
	gw.writeKeysSourceMap = writeKeysSourceMap
	gw.sourceIDSourceMap = sourceIDSourceMap
	gw.nonEventStreamSources = nonEventStreamSources
	gw.blockedEventsWorkspaceTypeNameMap = blockedEventsWorkspaceTypeNameMap
	gw.configSubscriberLock.Unlock()
}

// backendConfigSubscriber gets the config from config backend and extracts source information from it.
func (gw *Handle) backendConfigSubscriber(ctx context.Context) {
	closeConfigChan := func(sources int) {
		if !gw.backendConfigInitialised {
			gw.logger.Infow("BackendConfig initialised", "sources", sources)
			gw.backendConfigInitialised = true
			close(gw.backendConfigInitialisedChan)
		}
	}
	defer closeConfigChan(0)
	ch := gw.backendConfig.Subscribe(ctx, backendconfig.TopicProcessConfig)
	for data := range ch {
		configData := data.Data.(map[string]backendconfig.ConfigT)
		gw.processBackendConfig(configData)
		closeConfigChan(len(gw.writeKeysSourceMap))
	}
}

// runUserWebRequestWorkers starts two goroutines for each worker:
//  1. `userWebRequestBatcher` batches the webRequests that a worker gets
//  2. `userWebRequestWorkerProcess` processes the requests in the batches and sends them as part of a `jobsList` to `dbWriterWorker`s.
func (gw *Handle) runUserWebRequestWorkers(ctx context.Context) {
	g, _ := errgroup.WithContext(ctx)

	for _, y := range gw.userWebRequestWorkers {
		userWebRequestWorker := y
		g.Go(func() error {
			gw.userWebRequestWorkerProcess(userWebRequestWorker)
			return nil
		})

		g.Go(func() error {
			gw.userWebRequestBatcher(userWebRequestWorker)
			return nil
		})
	}
	_ = g.Wait()

	close(gw.userWorkerBatchRequestQ)
}

// initDBWriterWorkers initiates `maxDBWriterProcess` number of dbWriterWorkers
func (gw *Handle) initDBWriterWorkers(ctx context.Context) {
	g, _ := errgroup.WithContext(ctx)
	for i := 0; i < gw.conf.maxDBWriterProcess; i++ {
		gw.logger.Debug("DB Writer Worker Started", i)
		g.Go(crash.Wrapper(func() error {
			gw.dbWriterWorkerProcess()
			return nil
		}))
	}
	_ = g.Wait()
}

//	userWorkerRequestBatcher batches together jobLists received on the `userWorkerBatchRequestQ` channel of the gateway
//	and queues the batch at the `batchUserWorkerBatchRequestQ` channel of the gateway.
//
// Initiated during the gateway Setup and keeps batching jobLists received from webRequestWorkers
func (gw *Handle) userWorkerRequestBatcher() {
	userWorkerBatchRequestBuffer := make([]*userWorkerBatchRequestT, 0)

	timeout := time.After(gw.conf.dbBatchWriteTimeout.Load())
	for {
		select {
		case userWorkerBatchRequest, hasMore := <-gw.userWorkerBatchRequestQ:
			if !hasMore {
				if len(userWorkerBatchRequestBuffer) > 0 {
					gw.batchUserWorkerBatchRequestQ <- &batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				}
				close(gw.batchUserWorkerBatchRequestQ)
				return
			}
			// Append to request buffer
			userWorkerBatchRequestBuffer = append(userWorkerBatchRequestBuffer, userWorkerBatchRequest)
			if len(userWorkerBatchRequestBuffer) == gw.conf.maxDBBatchSize {
				breq := batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				gw.dbWorkersBufferFullStat.Count(1)
				gw.batchUserWorkerBatchRequestQ <- &breq
				userWorkerBatchRequestBuffer = make([]*userWorkerBatchRequestT, 0)
			}
		case <-timeout:
			timeout = time.After(gw.conf.dbBatchWriteTimeout.Load())
			if len(userWorkerBatchRequestBuffer) > 0 {
				breq := batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				gw.dbWorkersTimeOutStat.Count(1)
				gw.batchUserWorkerBatchRequestQ <- &breq
				userWorkerBatchRequestBuffer = make([]*userWorkerBatchRequestT, 0)
			}
		}
	}
}

// dbWriterWorkerProcess goes over the batches of jobs-list, and stores each job in every jobList into gw_db
// sends a map of errors if any(errors mapped to the job.uuid) over the responseQ channel of the webRequestWorker.
// userWebRequestWorkerProcess method of the webRequestWorker is waiting for this errorMessageMap.
// This in turn sends the error over the done channel of each respective webRequest.
func (gw *Handle) dbWriterWorkerProcess() {
	for breq := range gw.batchUserWorkerBatchRequestQ {
		var (
			jobBatches       = make([][]*jobsdb.JobT, 0)
			errorMessagesMap map[uuid.UUID]string
		)

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			jobBatches = append(jobBatches, userWorkerBatchRequest.jobBatches...)
		}

		ctx, cancel := context.WithTimeout(context.Background(), gw.conf.WriteTimeout)
		err := gw.jobsDB.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
			if gw.conf.gwAllowPartialWriteWithErrors.Load() {
				var err error
				errorMessagesMap, err = gw.jobsDB.StoreEachBatchRetryInTx(ctx, tx, jobBatches)
				if err != nil {
					return err
				}
			} else {
				err := gw.jobsDB.StoreInTx(ctx, tx, lo.Flatten(jobBatches))
				if err != nil {
					gw.logger.Errorf("Store into gateway db failed with error: %v", err)
					gw.logger.Errorf("JobList: %+v", jobBatches)
					return err
				}
			}

			// rsources stats
			rsourcesStats := rsources.NewStatsCollector(gw.rsourcesService, "gw", gw.stats, rsources.IgnoreDestinationID())
			rsourcesStats.JobsStoredWithErrors(lo.Flatten(jobBatches), errorMessagesMap)
			return rsourcesStats.Publish(ctx, tx.SqlTx())
		})
		if err != nil {
			errorMessage := err.Error()
			if ctx.Err() != nil {
				errorMessage = ctx.Err().Error()
			}
			if errorMessagesMap == nil {
				errorMessagesMap = make(map[uuid.UUID]string, len(jobBatches))
			}
			for _, batch := range jobBatches {
				errorMessagesMap[batch[0].UUID] = errorMessage
			}
		}

		cancel()
		gw.dbWritesStat.Count(1)

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			userWorkerBatchRequest.respChannel <- errorMessagesMap
		}
	}
}

/*
StartWebHandler starts all gateway web handlers, listening on gateway port.
Supports CORS from all origins. This function will block.
*/
func (gw *Handle) StartWebHandler(ctx context.Context) error {
	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		gw.logger.Infof("WebHandler waiting for BackendConfig before starting on %d", gw.conf.webPort)
		<-gw.backendConfigInitialisedChan
		gw.logger.Infof("backendConfig initialised")
		return nil
	})
	g.Go(func() error {
		gw.logger.Infof("WebHandler waiting for transformer feature before starting on %d", gw.conf.webPort)
		<-gw.transformerFeaturesInitialised
		gw.logger.Infof("transformer feature initialised")
		return nil
	})
	err := g.Wait()
	if err != nil {
		return err
	}
	gw.logger.Infof("WebHandler Starting on %d", gw.conf.webPort)
	component := "gateway"
	srvMux := chi.NewRouter()
	// rudder-sources new APIs
	rsourcesHandlerV1 := rsources_http.NewV1Handler(
		gw.rsourcesService,
		gw.logger.Child("rsources"),
	)
	rsourcesHandlerV2 := rsources_http.NewV2Handler(
		gw.rsourcesService,
		gw.logger.Child("rsources_failed_keys"),
	)
	srvMux.Use(
		func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gw.inFlightRequests.Add(1)
				h.ServeHTTP(w, r)
				gw.inFlightRequests.Done()
			})
		},
		chiware.StatMiddleware(ctx, stats.Default, component),
		middleware.LimitConcurrentRequests(gw.conf.maxConcurrentRequests),
		middleware.UncompressMiddleware,
	)
	srvMux.Route("/internal", func(r chi.Router) {
		r.Post("/v1/extract", gw.webExtractHandler())
		r.Post("/v1/retl", gw.webRetlHandler())
		r.Get("/v1/warehouse/fetch-tables", gw.whProxy.ServeHTTP)
		r.Post("/v1/audiencelist", gw.webAudienceListHandler())
		r.Post("/v1/replay", gw.webReplayHandler())
		r.Post("/v1/batch", gw.internalBatchHandler())

		// TODO: delete this handler once we are ready to remove support for the v1 api
		r.Mount("/v1/job-status", withContentType("application/json; charset=utf-8", rsourcesHandlerV1.ServeHTTP))

		r.Mount("/v2/job-status", withContentType("application/json; charset=utf-8", rsourcesHandlerV2.ServeHTTP))
		for path, handler := range gw.internalHttpHandlers {
			r.Mount(path, withContentType("application/json; charset=utf-8", handler.ServeHTTP))
		}
	})

	// TODO: delete this handler once we are ready to remove support for the v1 api
	srvMux.Mount("/v1/job-status", withContentType("application/json; charset=utf-8", rsourcesHandlerV1.ServeHTTP))

	srvMux.Route("/v1", func(r chi.Router) {
		r.Post("/alias", gw.webAliasHandler())
		r.Post("/audiencelist", gw.webAudienceListHandler())
		r.Post("/batch", gw.webBatchHandler())
		r.Post("/group", gw.webGroupHandler())
		r.Post("/identify", gw.webIdentifyHandler())
		r.Post("/merge", gw.webMergeHandler())
		r.Post("/page", gw.webPageHandler())
		r.Post("/screen", gw.webScreenHandler())
		r.Post("/track", gw.webTrackHandler())

		r.Post("/import", gw.webImportHandler())
		r.Post("/webhook", gw.webhookHandler())

		r.Get("/webhook", gw.webhookHandler())

		r.Route("/warehouse", func(r chi.Router) {
			r.Post("/pending-events", gw.whProxy.ServeHTTP)
			r.Post("/trigger-upload", gw.whProxy.ServeHTTP)
			r.Post("/jobs", gw.whProxy.ServeHTTP)
			// TODO: Remove this endpoint once sources change is released
			r.Get("/fetch-tables", gw.whProxy.ServeHTTP)

			r.Get("/jobs/status", gw.whProxy.ServeHTTP)
		})
	})

	srvMux.Get("/health", withContentType("application/json; charset=utf-8", app.LivenessHandler(gw.jobsDB)))
	srvMux.Get("/", withContentType("application/json; charset=utf-8", app.LivenessHandler(gw.jobsDB)))
	srvMux.Get("/docs",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(openApiSpec)
		},
	)

	srvMux.Route("/pixel/v1", func(r chi.Router) {
		r.Get("/track", gw.pixelTrackHandler())
		r.Get("/page", gw.pixelPageHandler())
	})
	srvMux.Post("/beacon/v1/batch", gw.beaconBatchHandler())
	srvMux.Get("/version", withContentType("application/json; charset=utf-8", gw.versionHandler))
	srvMux.Get("/robots.txt", gw.robotsHandler)

	c := cors.New(cors.Options{
		AllowOriginFunc:  func(_ string) bool { return true },
		AllowCredentials: true,
		AllowedHeaders:   []string{"*"},
		MaxAge:           900, // 15 mins
	})
	if diagnostics.EnableServerStartedMetric {
		diagnostics.Diagnostics.Track(diagnostics.ServerStarted, map[string]interface{}{
			diagnostics.ServerStarted: time.Now(),
		})
	}
	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(gw.conf.webPort),
		Handler:           c.Handler(crash.Handler(srvMux)),
		ReadTimeout:       gw.conf.ReadTimeout,
		ReadHeaderTimeout: gw.conf.ReadHeaderTimeout,
		WriteTimeout:      gw.conf.WriteTimeout,
		IdleTimeout:       gw.conf.IdleTimeout,
		MaxHeaderBytes:    gw.conf.maxHeaderBytes,
	}

	return kithttputil.ListenAndServe(ctx, srv)
}

// Shutdown the gateway
func (gw *Handle) Shutdown() error {
	gw.backgroundCancel()
	if err := gw.webhook.Shutdown(); err != nil {
		return err
	}

	gw.inFlightRequests.Wait()

	// UserWebRequestWorkers
	for _, worker := range gw.userWebRequestWorkers {
		close(worker.webRequestQ)
	}

	return gw.backgroundWait()
}
