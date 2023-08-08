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
func (gateway *Handle) Setup(
	ctx context.Context,
	config *config.Config, logger logger.Logger, stat stats.Stats,
	application app.App, backendConfig backendconfig.BackendConfig, jobsDB, errDB jobsdb.JobsDB,
	rateLimiter throttler.Throttler, versionHandler func(w http.ResponseWriter, r *http.Request),
	rsourcesService rsources.JobService, sourcehandle sourcedebugger.SourceDebugger,
) error {
	gateway.config = config
	gateway.logger = logger
	gateway.stats = stat
	gateway.application = application
	gateway.backendConfig = backendConfig
	gateway.jobsDB = jobsDB
	gateway.errDB = errDB
	gateway.rateLimiter = rateLimiter
	gateway.versionHandler = versionHandler
	gateway.rsourcesService = rsourcesService
	gateway.sourcehandle = sourcehandle

	config.RegisterDurationConfigVariable(30, &gateway.conf.httpTimeout, false, time.Second, "Gateway.httpTimeout")
	// Port where GW is running
	config.RegisterIntConfigVariable(8080, &gateway.conf.webPort, false, 1, "Gateway.webPort")
	// Port where AdminHandler is running
	config.RegisterIntConfigVariable(8089, &gateway.conf.adminWebPort, false, 1, "Gateway.adminWebPort")
	// Number of incoming requests that are batched before handing off to write workers
	config.RegisterIntConfigVariable(128, &gateway.conf.maxUserWebRequestBatchSize, false, 1, "Gateway.maxUserRequestBatchSize")
	// Number of userWorkerBatchRequest that are batched before initiating write
	config.RegisterIntConfigVariable(128, &gateway.conf.maxDBBatchSize, false, 1, "Gateway.maxDBBatchSize")
	// Timeout after which batch is formed anyway with whatever requests
	// are available
	config.RegisterDurationConfigVariable(15, &gateway.conf.userWebRequestBatchTimeout, true, time.Millisecond, []string{"Gateway.userWebRequestBatchTimeout", "Gateway.userWebRequestBatchTimeoutInMS"}...)
	config.RegisterDurationConfigVariable(5, &gateway.conf.dbBatchWriteTimeout, true, time.Millisecond, []string{"Gateway.dbBatchWriteTimeout", "Gateway.dbBatchWriteTimeoutInMS"}...)
	// Multiple workers are used to batch user web requests
	config.RegisterIntConfigVariable(64, &gateway.conf.maxUserWebRequestWorkerProcess, false, 1, "Gateway.maxUserWebRequestWorkerProcess")
	// Multiple DB writers are used to write data to DB
	config.RegisterIntConfigVariable(256, &gateway.conf.maxDBWriterProcess, false, 1, "Gateway.maxDBWriterProcess")
	// Maximum request size to gateway
	config.RegisterIntConfigVariable(4000, &gateway.conf.maxReqSize, true, 1024, "Gateway.maxReqSizeInKB")
	// Enable rate limit on incoming events. false by default
	config.RegisterBoolConfigVariable(false, &gateway.conf.enableRateLimit, true, "Gateway.enableRateLimit")
	// Enable suppress user feature. false by default
	config.RegisterBoolConfigVariable(true, &gateway.conf.enableSuppressUserFeature, false, "Gateway.enableSuppressUserFeature")
	// EventSchemas feature. false by default
	config.RegisterBoolConfigVariable(false, &gateway.conf.enableEventSchemasFeature, false, "EventSchemas.enableEventSchemasFeature")
	// Time period for diagnosis ticker
	config.RegisterDurationConfigVariable(60, &gateway.conf.diagnosisTickerTime, false, time.Second, []string{"Diagnostics.gatewayTimePeriod", "Diagnostics.gatewayTimePeriodInS"}...)
	// Enables accepting requests without user id and anonymous id. This is added to prevent client 4xx retries.
	config.RegisterBoolConfigVariable(false, &gateway.conf.allowReqsWithoutUserIDAndAnonymousID, true, "Gateway.allowReqsWithoutUserIDAndAnonymousID")
	config.RegisterBoolConfigVariable(true, &gateway.conf.gwAllowPartialWriteWithErrors, true, "Gateway.allowPartialWriteWithErrors")
	config.RegisterBoolConfigVariable(true, &gateway.conf.allowBatchSplitting, true, "Gateway.allowBatchSplitting")
	config.RegisterDurationConfigVariable(0, &gateway.conf.ReadTimeout, false, time.Second, []string{"ReadTimeout", "ReadTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(0, &gateway.conf.ReadHeaderTimeout, false, time.Second, []string{"ReadHeaderTimeout", "ReadHeaderTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(10, &gateway.conf.WriteTimeout, false, time.Second, []string{"WriteTimeout", "WriteTimeOutInSec"}...)
	config.RegisterDurationConfigVariable(720, &gateway.conf.IdleTimeout, false, time.Second, []string{"IdleTimeout", "IdleTimeoutInSec"}...)
	config.RegisterIntConfigVariable(524288, &gateway.conf.maxHeaderBytes, false, 1, "MaxHeaderBytes")
	// if set to '0', it means disabled.
	config.RegisterIntConfigVariable(50000, &gateway.conf.maxConcurrentRequests, false, 1, "Gateway.maxConcurrentRequests")

	// Registering stats
	gateway.batchSizeStat = gateway.stats.NewStat("gateway.batch_size", stats.HistogramType)
	gateway.requestSizeStat = gateway.stats.NewStat("gateway.request_size", stats.HistogramType)
	gateway.dbWritesStat = gateway.stats.NewStat("gateway.db_writes", stats.CountType)
	gateway.dbWorkersBufferFullStat = gateway.stats.NewStat("gateway.db_workers_buffer_full", stats.CountType)
	gateway.dbWorkersTimeOutStat = gateway.stats.NewStat("gateway.db_workers_time_out", stats.CountType)
	gateway.bodyReadTimeStat = gateway.stats.NewStat("gateway.http_body_read_time", stats.TimerType)
	gateway.addToWebRequestQWaitTime = gateway.stats.NewStat("gateway.web_request_queue_wait_time", stats.TimerType)
	gateway.addToBatchRequestQWaitTime = gateway.stats.NewStat("gateway.batch_request_queue_wait_time", stats.TimerType)
	gateway.processRequestTime = gateway.stats.NewStat("gateway.process_request_time", stats.TimerType)
	gateway.emptyAnonIdHeaderStat = gateway.stats.NewStat("gateway.empty_anonymous_id_header", stats.CountType)

	gateway.diagnosisTicker = time.NewTicker(gateway.conf.diagnosisTickerTime)
	gateway.netHandle = &http.Client{Transport: &http.Transport{}, Timeout: gateway.conf.httpTimeout}
	gateway.userWorkerBatchRequestQ = make(chan *userWorkerBatchRequestT, gateway.conf.maxDBBatchSize)
	gateway.batchUserWorkerBatchRequestQ = make(chan *batchUserWorkerBatchRequestT, gateway.conf.maxDBWriterProcess)
	gateway.irh = &ImportRequestHandler{Handle: gateway}
	gateway.rrh = &RegularRequestHandler{Handle: gateway}
	gateway.webhookHandler = webhook.Setup(gateway, gateway.stats)
	whURL, err := url.ParseRequestURI(misc.GetWarehouseURL())
	if err != nil {
		return fmt.Errorf("invalid warehouse URL %s: %w", whURL, err)
	}
	gateway.whProxy = httputil.NewSingleHostReverseProxy(whURL)
	if gateway.conf.enableSuppressUserFeature && gateway.application.Features().SuppressUser != nil {
		gateway.suppressUserHandler, err = application.Features().SuppressUser.Setup(ctx, gateway.backendConfig)
		if err != nil {
			return fmt.Errorf("could not setup suppress user feature: %w", err)
		}
	}
	if gateway.conf.enableEventSchemasFeature {
		gateway.eventSchemaHandler = event_schema.GetInstance()
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	gateway.backgroundCancel = cancel
	gateway.backgroundWait = g.Wait
	gateway.initUserWebRequestWorkers()
	gateway.backendConfigInitialisedChan = make(chan struct{})

	g.Go(misc.WithBugsnag(func() error {
		gateway.backendConfigSubscriber(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gateway.runUserWebRequestWorkers(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gateway.userWorkerRequestBatcher()
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gateway.initDBWriterWorkers(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gateway.printStats(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		gateway.collectMetrics(ctx)
		return nil
	}))
	return nil
}

// Part of the gateway module Setup call.
//
//	Initiates `maxUserWebRequestWorkerProcess` number of `webRequestWorkers` that listen on their `webRequestQ` for new WebRequests.
func (gateway *Handle) initUserWebRequestWorkers() {
	gateway.userWebRequestWorkers = make([]*userWebRequestWorkerT, gateway.conf.maxUserWebRequestWorkerProcess)
	for i := 0; i < gateway.conf.maxUserWebRequestWorkerProcess; i++ {
		gateway.logger.Debug("User Web Request Worker Started", i)
		userWebRequestWorker := &userWebRequestWorkerT{
			webRequestQ:    make(chan *webRequestT, gateway.conf.maxUserWebRequestBatchSize),
			batchRequestQ:  make(chan *batchWebRequestT),
			reponseQ:       make(chan map[uuid.UUID]string),
			batchTimeStat:  gateway.stats.NewStat("gateway.batch_time", stats.TimerType),
			bufferFullStat: gateway.stats.NewStat("gateway.user_request_worker_buffer_full", stats.CountType),
			timeOutStat:    gateway.stats.NewStat("gateway.user_request_worker_time_out", stats.CountType),
		}
		gateway.userWebRequestWorkers[i] = userWebRequestWorker
	}
}

// Gets the config from config backend and extracts enabled writekeys
func (gateway *Handle) backendConfigSubscriber(ctx context.Context) {
	closeConfigChan := func(sources int) {
		if !gateway.backendConfigInitialised {
			gateway.logger.Infow("BackendConfig initialised", "sources", sources)
			gateway.backendConfigInitialised = true
			close(gateway.backendConfigInitialisedChan)
		}
	}
	defer closeConfigChan(0)
	ch := gateway.backendConfig.Subscribe(ctx, backendconfig.TopicProcessConfig)
	for data := range ch {
		var (
			newWriteKeysSourceMap          = map[string]backendconfig.SourceT{}
			newEnabledWriteKeyWebhookMap   = map[string]string{}
			newEnabledWriteKeyWorkspaceMap = map[string]string{}
			newSourceIDToNameMap           = map[string]string{}
		)
		configData := data.Data.(map[string]backendconfig.ConfigT)
		for workspaceID, wsConfig := range configData {
			for _, source := range wsConfig.Sources {
				newSourceIDToNameMap[source.ID] = source.Name
				newWriteKeysSourceMap[source.WriteKey] = source

				if source.Enabled {
					newEnabledWriteKeyWorkspaceMap[source.WriteKey] = workspaceID
					if source.SourceDefinition.Category == "webhook" {
						newEnabledWriteKeyWebhookMap[source.WriteKey] = source.SourceDefinition.Name
						gateway.webhookHandler.Register(source.SourceDefinition.Name)
					}
				}
			}
		}
		gateway.conf.configSubscriberLock.Lock()
		gateway.conf.writeKeysSourceMap = newWriteKeysSourceMap
		gateway.conf.enabledWriteKeyWebhookMap = newEnabledWriteKeyWebhookMap
		gateway.conf.enabledWriteKeyWorkspaceMap = newEnabledWriteKeyWorkspaceMap
		gateway.conf.configSubscriberLock.Unlock()
		closeConfigChan(len(gateway.conf.writeKeysSourceMap))
	}
}

// runUserWebRequestWorkers starts two goroutines for each worker:
//  1. `userWebRequestBatcher` batches the webRequests that a worker gets
//  2. `userWebRequestWorkerProcess` processes the requests in the batches and sends them as part of a `jobsList` to `dbWriterWorker`s.
func (gateway *Handle) runUserWebRequestWorkers(ctx context.Context) {
	g, _ := errgroup.WithContext(ctx)

	for _, y := range gateway.userWebRequestWorkers {
		userWebRequestWorker := y
		g.Go(func() error {
			gateway.userWebRequestWorkerProcess(userWebRequestWorker)
			return nil
		})

		g.Go(func() error {
			gateway.userWebRequestBatcher(userWebRequestWorker)
			return nil
		})
	}
	_ = g.Wait()

	close(gateway.userWorkerBatchRequestQ)
}

// Initiates `maxDBWriterProcess` number of dbWriterWorkers
func (gateway *Handle) initDBWriterWorkers(ctx context.Context) {
	g, _ := errgroup.WithContext(ctx)
	for i := 0; i < gateway.conf.maxDBWriterProcess; i++ {
		gateway.logger.Debug("DB Writer Worker Started", i)
		g.Go(misc.WithBugsnag(func() error {
			gateway.dbWriterWorkerProcess()
			return nil
		}))
	}
	_ = g.Wait()
}

//	Batches together jobLists received on the `userWorkerBatchRequestQ` channel of the gateway
//	and queues the batch at the `batchUserWorkerBatchRequestQ` channel of the gateway.
//
// Initiated during the gateway Setup and keeps batching jobLists received from webRequestWorkers
func (gateway *Handle) userWorkerRequestBatcher() {
	userWorkerBatchRequestBuffer := make([]*userWorkerBatchRequestT, 0)

	timeout := time.After(gateway.conf.dbBatchWriteTimeout)
	for {
		select {
		case userWorkerBatchRequest, hasMore := <-gateway.userWorkerBatchRequestQ:
			if !hasMore {
				if len(userWorkerBatchRequestBuffer) > 0 {
					gateway.batchUserWorkerBatchRequestQ <- &batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				}
				close(gateway.batchUserWorkerBatchRequestQ)
				return
			}
			// Append to request buffer
			userWorkerBatchRequestBuffer = append(userWorkerBatchRequestBuffer, userWorkerBatchRequest)
			if len(userWorkerBatchRequestBuffer) == gateway.conf.maxDBBatchSize {
				breq := batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				gateway.dbWorkersBufferFullStat.Count(1)
				gateway.batchUserWorkerBatchRequestQ <- &breq
				userWorkerBatchRequestBuffer = make([]*userWorkerBatchRequestT, 0)
			}
		case <-timeout:
			timeout = time.After(gateway.conf.dbBatchWriteTimeout)
			if len(userWorkerBatchRequestBuffer) > 0 {
				breq := batchUserWorkerBatchRequestT{batchUserWorkerBatchRequest: userWorkerBatchRequestBuffer}
				gateway.dbWorkersTimeOutStat.Count(1)
				gateway.batchUserWorkerBatchRequestQ <- &breq
				userWorkerBatchRequestBuffer = make([]*userWorkerBatchRequestT, 0)
			}
		}
	}
}

// goes over the batches of jobs-list, and stores each job in every jobList into gw_db
// sends a map of errors if any(errors mapped to the job.uuid) over the responseQ channel of the webRequestWorker.
// userWebRequestWorkerProcess method of the webRequestWorker is waiting for this errorMessageMap.
// This in turn sends the error over the done channel of each respective webRequest.
func (gateway *Handle) dbWriterWorkerProcess() {
	for breq := range gateway.batchUserWorkerBatchRequestQ {
		var (
			jobBatches       = make([][]*jobsdb.JobT, 0)
			errorMessagesMap map[uuid.UUID]string
		)

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			jobBatches = append(jobBatches, userWorkerBatchRequest.jobBatches...)
		}

		ctx, cancel := context.WithTimeout(context.Background(), gateway.conf.WriteTimeout)
		err := gateway.jobsDB.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
			if gateway.conf.gwAllowPartialWriteWithErrors {
				var err error
				errorMessagesMap, err = gateway.jobsDB.StoreEachBatchRetryInTx(ctx, tx, jobBatches)
				if err != nil {
					return err
				}
			} else {
				err := gateway.jobsDB.StoreInTx(ctx, tx, lo.Flatten(jobBatches))
				if err != nil {
					gateway.logger.Errorf("Store into gateway db failed with error: %v", err)
					gateway.logger.Errorf("JobList: %+v", jobBatches)
					return err
				}
			}

			// rsources stats
			rsourcesStats := rsources.NewStatsCollector(gateway.rsourcesService)
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
		gateway.dbWritesStat.Count(1)

		for _, userWorkerBatchRequest := range breq.batchUserWorkerBatchRequest {
			userWorkerBatchRequest.respChannel <- errorMessagesMap
		}
	}
}

/*
StartWebHandler starts all gateway web handlers, listening on gateway port.
Supports CORS from all origins.
This function will block.
*/
func (gateway *Handle) StartWebHandler(ctx context.Context) error {
	gateway.logger.Infof("WebHandler waiting for BackendConfig before starting on %d", gateway.conf.webPort)
	<-gateway.backendConfigInitialisedChan
	gateway.logger.Infof("WebHandler Starting on %d", gateway.conf.webPort)
	component := "gateway"
	srvMux := chi.NewRouter()
	srvMux.Use(
		chiware.StatMiddleware(ctx, srvMux, stats.Default, component),
		middleware.LimitConcurrentRequests(gateway.conf.maxConcurrentRequests),
		middleware.UncompressMiddleware,
	)
	srvMux.Route("/internal", func(r chi.Router) {
		r.Post("/v1/extract", gateway.webExtractHandler)
		r.Get("/v1/warehouse/fetch-tables", gateway.whProxy.ServeHTTP)
	})

	srvMux.Route("/v1", func(r chi.Router) {
		r.Post("/alias", gateway.webAliasHandler)
		r.Post("/audiencelist", gateway.webAudienceListHandler)
		r.Post("/batch", gateway.webBatchHandler)
		r.Post("/group", gateway.webGroupHandler)
		r.Post("/identify", gateway.webIdentifyHandler)
		r.Post("/import", gateway.webImportHandler)
		r.Post("/merge", gateway.webMergeHandler)
		r.Post("/page", gateway.webPageHandler)
		r.Post("/screen", gateway.webScreenHandler)
		r.Post("/track", gateway.webTrackHandler)
		r.Post("/webhook", gateway.webhookHandler.RequestHandler)

		r.Get("/webhook", gateway.webhookHandler.RequestHandler)

		r.Route("/warehouse", func(r chi.Router) {
			r.Post("/pending-events", gateway.whProxy.ServeHTTP)
			r.Post("/trigger-upload", gateway.whProxy.ServeHTTP)
			r.Post("/jobs", gateway.whProxy.ServeHTTP)
			// TODO: Remove this endpoint once sources change is released
			r.Get("/fetch-tables", gateway.whProxy.ServeHTTP)

			r.Get("/jobs/status", gateway.whProxy.ServeHTTP)
		})
	})

	srvMux.Get("/health", WithContentType("application/json; charset=utf-8", app.LivenessHandler(gateway.jobsDB)))

	srvMux.Get("/", WithContentType("application/json; charset=utf-8", app.LivenessHandler(gateway.jobsDB)))
	srvMux.Route("/pixel/v1", func(r chi.Router) {
		r.Get("/track", gateway.pixelTrackHandler)
		r.Get("/page", gateway.pixelPageHandler)
	})
	srvMux.Post("/beacon/v1/batch", gateway.beaconBatchHandler)
	srvMux.Get("/version", WithContentType("application/json; charset=utf-8", gateway.versionHandler))
	srvMux.Get("/robots.txt", gateway.robotsHandler)

	if gateway.conf.enableEventSchemasFeature {
		srvMux.Route("/schemas", func(r chi.Router) {
			r.Get("/event-models", WithContentType("application/json; charset=utf-8", gateway.eventSchemaController(gateway.eventSchemaHandler.GetEventModels)))
			r.Get("/event-versions", WithContentType("application/json; charset=utf-8", gateway.eventSchemaController(gateway.eventSchemaHandler.GetEventVersions)))
			r.Get("/event-model/{EventID}/key-counts", WithContentType("application/json; charset=utf-8", gateway.eventSchemaController(gateway.eventSchemaHandler.GetKeyCounts)))
			r.Get("/event-model/{EventID}/metadata", WithContentType("application/json; charset=utf-8", gateway.eventSchemaController(gateway.eventSchemaHandler.GetEventModelMetadata)))
			r.Get("/event-version/{VersionID}/metadata", WithContentType("application/json; charset=utf-8", gateway.eventSchemaController(gateway.eventSchemaHandler.GetSchemaVersionMetadata)))
			r.Get("/event-version/{VersionID}/missing-keys", WithContentType("application/json; charset=utf-8", gateway.eventSchemaController(gateway.eventSchemaHandler.GetSchemaVersionMissingKeys)))
			r.Get("/event-models/json-schemas", WithContentType("application/json; charset=utf-8", gateway.eventSchemaController(gateway.eventSchemaHandler.GetJsonSchemas)))
		})
	}

	// rudder-sources new APIs
	rsourcesHandler := rsources_http.NewHandler(
		gateway.rsourcesService,
		gateway.logger.Child("rsources"))
	srvMux.Mount("/v1/job-status", WithContentType("application/json; charset=utf-8", rsourcesHandler.ServeHTTP))

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
		Addr:              ":" + strconv.Itoa(gateway.conf.webPort),
		Handler:           c.Handler(bugsnag.Handler(srvMux)),
		ReadTimeout:       gateway.conf.ReadTimeout,
		ReadHeaderTimeout: gateway.conf.ReadHeaderTimeout,
		WriteTimeout:      gateway.conf.WriteTimeout,
		IdleTimeout:       gateway.conf.IdleTimeout,
		MaxHeaderBytes:    gateway.conf.maxHeaderBytes,
	}

	return kithttputil.ListenAndServe(ctx, srv)
}

// StartAdminHandler for Admin Operations
func (gateway *Handle) StartAdminHandler(ctx context.Context) error {
	gateway.logger.Infof("AdminHandler waiting for BackendConfig before starting on %d", gateway.conf.adminWebPort)
	<-gateway.backendConfigInitialisedChan
	gateway.logger.Infof("AdminHandler starting on %d", gateway.conf.adminWebPort)
	component := "gateway"
	srvMux := chi.NewRouter()
	srvMux.Use(
		chiware.StatMiddleware(ctx, srvMux, stats.Default, component),
		middleware.LimitConcurrentRequests(gateway.conf.maxConcurrentRequests),
	)
	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(gateway.conf.adminWebPort),
		Handler:           bugsnag.Handler(srvMux),
		ReadHeaderTimeout: gateway.conf.ReadHeaderTimeout,
	}

	return kithttputil.ListenAndServe(ctx, srv)
}

func (gateway *Handle) Shutdown() error {
	gateway.backgroundCancel()
	if err := gateway.webhookHandler.Shutdown(); err != nil {
		return err
	}

	// UserWebRequestWorkers
	for _, worker := range gateway.userWebRequestWorkers {
		close(worker.webRequestQ)
	}

	return gateway.backgroundWait()
}
