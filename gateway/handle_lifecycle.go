package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/cors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/chiware"
	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	event_schema "github.com/rudderlabs/rudder-server/event-schema"
	"github.com/rudderlabs/rudder-server/gateway/throttler"
	"github.com/rudderlabs/rudder-server/gateway/webhook"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/middleware"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	rsources_http "github.com/rudderlabs/rudder-server/services/rsources/http"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

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
	rsourcesService rsources.JobService, sourcehandle sourcedebugger.SourceDebugger,
) error {
	gw.config = config
	gw.logger = logger
	gw.stats = stat
	gw.application = application
	gw.backendConfig = backendConfig
	gw.jobsDB = jobsDB
	gw.errDB = errDB
	gw.rateLimiter = rateLimiter
	gw.versionHandler = versionHandler
	gw.rsourcesService = rsourcesService
	gw.sourcehandle = sourcehandle

	config.RegisterDurationConfigVariable(30, &gw.conf.httpTimeout, false, time.Second, "Gateway.httpTimeout")
	// Port where GW is running
	config.RegisterIntConfigVariable(8080, &gw.conf.webPort, false, 1, "Gateway.webPort")
	// Number of incoming requests that are batched before handing off to write workers
	config.RegisterIntConfigVariable(128, &gw.conf.maxUserWebRequestBatchSize, false, 1, "Gateway.maxUserRequestBatchSize")
	// Number of userWorkerBatchRequest that are batched before initiating write
	config.RegisterIntConfigVariable(128, &gw.conf.maxDBBatchSize, false, 1, "Gateway.maxDBBatchSize")
	// Timeout after which batch is formed anyway with whatever requests
	// are available
	config.RegisterDurationConfigVariable(15, &gw.conf.userWebRequestBatchTimeout, true, time.Millisecond, []string{"Gateway.userWebRequestBatchTimeout", "Gateway.userWebRequestBatchTimeoutInMS"}...)
	config.RegisterDurationConfigVariable(5, &gw.conf.dbBatchWriteTimeout, true, time.Millisecond, []string{"Gateway.dbBatchWriteTimeout", "Gateway.dbBatchWriteTimeoutInMS"}...)
	// Multiple workers are used to batch user web requests
	config.RegisterIntConfigVariable(64, &gw.conf.maxUserWebRequestWorkerProcess, false, 1, "Gateway.maxUserWebRequestWorkerProcess")
	// Multiple DB writers are used to write data to DB
	config.RegisterIntConfigVariable(256, &gw.conf.maxDBWriterProcess, false, 1, "Gateway.maxDBWriterProcess")
	// Maximum request size to gateway
	config.RegisterIntConfigVariable(4000, &gw.conf.maxReqSize, true, 1024, "Gateway.maxReqSizeInKB")
	// Enable rate limit on incoming events. false by default
	config.RegisterBoolConfigVariable(false, &gw.conf.enableRateLimit, true, "Gateway.enableRateLimit")
	// Enable suppress user feature. false by default
	config.RegisterBoolConfigVariable(true, &gw.conf.enableSuppressUserFeature, false, "Gateway.enableSuppressUserFeature")
	// EventSchemas feature. false by default
	config.RegisterBoolConfigVariable(false, &gw.conf.enableEventSchemasFeature, false, "EventSchemas.enableEventSchemasFeature")
	// Time period for diagnosis ticker
	config.RegisterDurationConfigVariable(60, &gw.conf.diagnosisTickerTime, false, time.Second, []string{"Diagnostics.gatewayTimePeriod", "Diagnostics.gatewayTimePeriodInS"}...)
	// Enables accepting requests without user id and anonymous id. This is added to prevent client 4xx retries.
	config.RegisterBoolConfigVariable(false, &gw.conf.allowReqsWithoutUserIDAndAnonymousID, true, "Gateway.allowReqsWithoutUserIDAndAnonymousID")
	config.RegisterBoolConfigVariable(true, &gw.conf.gwAllowPartialWriteWithErrors, true, "Gateway.allowPartialWriteWithErrors")
	config.RegisterBoolConfigVariable(true, &gw.conf.allowBatchSplitting, true, "Gateway.allowBatchSplitting")
	config.RegisterDurationConfigVariable(0, &gw.conf.ReadTimeout, false, time.Second, []string{"ReadTimeout", "ReadTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(0, &gw.conf.ReadHeaderTimeout, false, time.Second, []string{"ReadHeaderTimeout", "ReadHeaderTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(10, &gw.conf.WriteTimeout, false, time.Second, []string{"WriteTimeout", "WriteTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(720, &gw.conf.IdleTimeout, false, time.Second, []string{"IdleTimeout", "IdleTimeoutInSec"}...)
	config.RegisterIntConfigVariable(524288, &gw.conf.maxHeaderBytes, false, 1, "MaxHeaderBytes")
	// if set to '0', it means disabled.
	config.RegisterIntConfigVariable(50000, &gw.conf.maxConcurrentRequests, false, 1, "Gateway.maxConcurrentRequests")

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

	gw.diagnosisTicker = time.NewTicker(gw.conf.diagnosisTickerTime)
	gw.netHandle = &http.Client{Transport: &http.Transport{}, Timeout: gw.conf.httpTimeout}
	gw.userWorkerBatchRequestQ = make(chan *userWorkerBatchRequestT, gw.conf.maxDBBatchSize)
	gw.batchUserWorkerBatchRequestQ = make(chan *batchUserWorkerBatchRequestT, gw.conf.maxDBWriterProcess)
	gw.irh = &ImportRequestHandler{Handle: gw}
	gw.rrh = &RegularRequestHandler{Handle: gw}
	gw.webhook = webhook.Setup(gw, gw.stats)
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
	if gw.conf.enableEventSchemasFeature {
		gw.eventSchemaHandler = event_schema.GetInstance()
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	gw.backgroundCancel = cancel
	gw.backgroundWait = g.Wait
	gw.initUserWebRequestWorkers()
	gw.backendConfigInitialisedChan = make(chan struct{})

	g.Go(misc.WithBugsnag(func() error {
		gw.backendConfigSubscriber(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gw.runUserWebRequestWorkers(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gw.userWorkerRequestBatcher()
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gw.initDBWriterWorkers(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gw.printStats(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gw.collectMetrics(ctx)
		return nil
	}))
	return nil
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
		var (
			writeKeysSourceMap = map[string]backendconfig.SourceT{}
			sourceIDSourceMap  = map[string]backendconfig.SourceT{}
		)
		configData := data.Data.(map[string]backendconfig.ConfigT)
		for _, wsConfig := range configData {
			for _, source := range wsConfig.Sources {
				writeKeysSourceMap[source.WriteKey] = source
				sourceIDSourceMap[source.ID] = source
				if source.Enabled && source.SourceDefinition.Category == "webhook" {
					gw.webhook.Register(source.SourceDefinition.Name)
				}
			}
		}
		gw.configSubscriberLock.Lock()
		gw.writeKeysSourceMap = writeKeysSourceMap
		gw.sourceIDSourceMap = sourceIDSourceMap
		gw.configSubscriberLock.Unlock()
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
		g.Go(misc.WithBugsnag(func() error {
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

	timeout := time.After(gw.conf.dbBatchWriteTimeout)
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
			timeout = time.After(gw.conf.dbBatchWriteTimeout)
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
			if gw.conf.gwAllowPartialWriteWithErrors {
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
			rsourcesStats := rsources.NewStatsCollector(gw.rsourcesService)
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
	gw.logger.Infof("WebHandler waiting for BackendConfig before starting on %d", gw.conf.webPort)
	<-gw.backendConfigInitialisedChan
	gw.logger.Infof("WebHandler Starting on %d", gw.conf.webPort)
	component := "gateway"
	srvMux := chi.NewRouter()
	srvMux.Use(
		chiware.StatMiddleware(ctx, srvMux, stats.Default, component),
		middleware.LimitConcurrentRequests(gw.conf.maxConcurrentRequests),
		middleware.UncompressMiddleware,
	)
	srvMux.Route("/internal", func(r chi.Router) {
		r.Post("/v1/extract", gw.webExtractHandler())
		r.Get("/v1/warehouse/fetch-tables", gw.whProxy.ServeHTTP)
	})

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
	srvMux.Route("/pixel/v1", func(r chi.Router) {
		r.Get("/track", gw.pixelTrackHandler())
		r.Get("/page", gw.pixelPageHandler())
	})
	srvMux.Post("/beacon/v1/batch", gw.beaconBatchHandler())
	srvMux.Get("/version", withContentType("application/json; charset=utf-8", gw.versionHandler))
	srvMux.Get("/robots.txt", gw.robotsHandler)

	if gw.conf.enableEventSchemasFeature {
		srvMux.Route("/schemas", func(r chi.Router) {
			r.Get("/event-models", withContentType("application/json; charset=utf-8", gw.eventSchemaController(gw.eventSchemaHandler.GetEventModels)))
			r.Get("/event-versions", withContentType("application/json; charset=utf-8", gw.eventSchemaController(gw.eventSchemaHandler.GetEventVersions)))
			r.Get("/event-model/{EventID}/key-counts", withContentType("application/json; charset=utf-8", gw.eventSchemaController(gw.eventSchemaHandler.GetKeyCounts)))
			r.Get("/event-model/{EventID}/metadata", withContentType("application/json; charset=utf-8", gw.eventSchemaController(gw.eventSchemaHandler.GetEventModelMetadata)))
			r.Get("/event-version/{VersionID}/metadata", withContentType("application/json; charset=utf-8", gw.eventSchemaController(gw.eventSchemaHandler.GetSchemaVersionMetadata)))
			r.Get("/event-version/{VersionID}/missing-keys", withContentType("application/json; charset=utf-8", gw.eventSchemaController(gw.eventSchemaHandler.GetSchemaVersionMissingKeys)))
			r.Get("/event-models/json-schemas", withContentType("application/json; charset=utf-8", gw.eventSchemaController(gw.eventSchemaHandler.GetJsonSchemas)))
		})
	}

	// rudder-sources new APIs
	rsourcesHandler := rsources_http.NewHandler(
		gw.rsourcesService,
		gw.logger.Child("rsources"))
	srvMux.Mount("/v1/job-status", withContentType("application/json; charset=utf-8", rsourcesHandler.ServeHTTP))

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
		Handler:           c.Handler(bugsnag.Handler(srvMux)),
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

	// UserWebRequestWorkers
	for _, worker := range gw.userWebRequestWorkers {
		close(worker.webRequestQ)
	}

	return gw.backgroundWait()
}
