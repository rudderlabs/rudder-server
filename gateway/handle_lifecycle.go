package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"time"

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
	gw.webhook = webhook.Setup(gw, transformerFeaturesService, gw.stats)
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

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	gw.backgroundCancel = cancel
	gw.backgroundWait = g.Wait
	gw.initUserWebRequestWorkers()
	gw.backendConfigInitialisedChan = make(chan struct{})

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
	return nil
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
			rsourcesStats := rsources.NewStatsCollector(gw.rsourcesService, rsources.IgnoreDestinationID())
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
