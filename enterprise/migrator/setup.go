package migrator

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/enterprise/pathfinder"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type Migrator struct {
	migrationMode string
}

var migratorPort int

// Run Migrator feature
func (m *Migrator) Run(ctx context.Context, gatewayDB, routerDB, batchRouterDB *jobsdb.HandleT,
	startProcessor, startRouter func(),
) {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("enterprise").Child("migrator")

	pkgLogger.Info("[[ MultiNode ]] Setting up migration")

	forExport := strings.Contains(m.migrationMode, db.EXPORT)

	var pf pathfinder.ClusterStateT

	migratorPort = config.GetEnvAsInt("MIGRATOR_PORT", 8084)
	if forExport {
		nextClusterBackendCount := config.GetRequiredEnvAsInt("MIGRATING_TO_BACKEND_COUNT")
		nextClusterVersion := config.GetRequiredEnvAsInt("MIGRATING_TO_CLUSTER_VERSION")
		dnsPattern := config.GetEnv("URL_PATTERN", "http://backend-<CLUSTER_VERSION><NODENUM>")
		instanceIDPattern := config.GetEnv("INSTANCE_ID_PATTERN", "hosted-v<CLUSTER_VERSION>-rudderstack-<NODENUM>")
		pf = pathfinder.New(nextClusterBackendCount, nextClusterVersion, migratorPort, dnsPattern, instanceIDPattern)
	}

	pkgLogger.Info("Setting up migrators")
	var (
		gatewayMigrator     MigratorT
		routerMigrator      MigratorT
		batchRouterMigrator MigratorT
		failedKeysMigrator  FailedKeysMigratorT
	)

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		gatewayMigrator = New(m.migrationMode, gatewayDB, pf)
		return nil
	})
	g.Go(func() error {
		routerMigrator = New(m.migrationMode, routerDB, pf)
		return nil
	})
	g.Go(func() error {
		batchRouterMigrator = New(m.migrationMode, batchRouterDB, pf)
		return nil
	})
	g.Go(func() error {
		failedKeysMigrator = NewFailedKeysMigrator(m.migrationMode, router.GetFailedEventsManager(), pf)
		return nil
	})

	_ = g.Wait()

	m.StartWebHandler(ctx, &gatewayMigrator, &routerMigrator, &batchRouterMigrator, &failedKeysMigrator, startProcessor, startRouter)
}

// Setup initializes Migrator feature
func (m *Migrator) PrepareJobsdbsForImport(gatewayDB, routerDB, batchRouterDB *jobsdb.HandleT) {
	switch m.migrationMode {
	case db.IMPORT:
		seqNoForNewDS := int64(GetMigratingToVersion())*int64(math.Pow10(13)) + 1
		if gatewayDB != nil {
			gatewayDB.UpdateSequenceNumberOfLatestDS(seqNoForNewDS)
		}
		if routerDB != nil {
			routerDB.UpdateSequenceNumberOfLatestDS(seqNoForNewDS)
		}
		if batchRouterDB != nil {
			batchRouterDB.UpdateSequenceNumberOfLatestDS(seqNoForNewDS)
		}
	}
}

// StatusResponseT structure for status thats called by operator
type StatusResponseT struct {
	Completed  bool   `json:"completed"`
	Gw         bool   `json:"gw"`
	Rt         bool   `json:"rt"`
	BatchRt    bool   `json:"batch_rt"`
	FailedKeys bool   `json:"failed_keys"`
	Mode       string `json:"mode,omitempty"`
}

// StartWebHandler starts the webhandler for internal communication and status
func (m *Migrator) StartWebHandler(ctx context.Context, gatewayMigrator, routerMigrator, batchRouterMigrator *MigratorT, failedKeysMigrator *FailedKeysMigratorT, startProcessor, startRouter func()) {
	pkgLogger.Infof("Migrator: Starting migrationWebHandler on port %d", migratorPort)
	srvMux := http.NewServeMux()
	srvMux.HandleFunc("/gw"+notificationURI, m.importRequestHandler((*gatewayMigrator).importer.importHandler))
	srvMux.HandleFunc("/rt"+notificationURI, m.importRequestHandler((*routerMigrator).importer.importHandler))
	srvMux.HandleFunc("/batch_rt"+notificationURI, m.importRequestHandler((*batchRouterMigrator).importer.importHandler))
	srvMux.HandleFunc("/failed_keys"+notificationURI, m.importRequestHandler((*failedKeysMigrator).importer.importHandler))

	srvMux.HandleFunc("/postImport", func(w http.ResponseWriter, r *http.Request) {
		startProcessor()
		startRouter()
	})

	srvMux.HandleFunc("/export/status", func(w http.ResponseWriter, r *http.Request) {
		gwCompleted := (*gatewayMigrator).exporter.exportStatusHandler()
		rtCompleted := (*routerMigrator).exporter.exportStatusHandler()
		brtCompleted := (*batchRouterMigrator).exporter.exportStatusHandler()
		fkCompleted := (*failedKeysMigrator).exporter.exportStatusHandler()
		completed := gwCompleted && rtCompleted && brtCompleted && fkCompleted
		mode := m.migrationMode

		response := StatusResponseT{
			Completed:  completed,
			Gw:         gwCompleted,
			Rt:         rtCompleted,
			BatchRt:    brtCompleted,
			FailedKeys: fkCompleted,
			Mode:       mode,
		}

		responseJSON, err := json.Marshal(response)
		if err != nil {
			panic("Invalid JSON in export status")
		}
		w.Write(responseJSON)
	})

	srvMux.HandleFunc("/import/status", func(w http.ResponseWriter, r *http.Request) {
		gwCompleted := (*gatewayMigrator).importer.importStatusHandler()
		rtCompleted := (*routerMigrator).importer.importStatusHandler()
		brtCompleted := (*batchRouterMigrator).importer.importStatusHandler()
		fkCompleted := (*failedKeysMigrator).importer.importStatusHandler()
		completed := gwCompleted && rtCompleted && brtCompleted && fkCompleted
		mode := m.migrationMode

		response := StatusResponseT{
			Completed:  completed,
			Gw:         gwCompleted,
			Rt:         rtCompleted,
			BatchRt:    brtCompleted,
			FailedKeys: fkCompleted,
			Mode:       mode,
		}

		responseJSON, err := json.Marshal(response)
		if err != nil {
			panic("Invalid JSON in export status")
		}
		w.Write(responseJSON)
	})

	srv := &http.Server{
		Addr:    ":" + strconv.Itoa(migratorPort),
		Handler: bugsnag.Handler(srvMux),
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		<-ctx.Done()
		return srv.Shutdown(context.Background())
	})
	g.Go(func() error {
		return srv.ListenAndServe()
	})

	err := g.Wait()
	if err != nil {
		pkgLogger.Fatal(err)
	}
}

func (m *Migrator) importRequestHandler(importHandler func(jobsdb.MigrationCheckpointT) error) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)
		r.Body.Close()
		migrationCheckpoint := jobsdb.MigrationCheckpointT{}
		err := json.Unmarshal(body, &migrationCheckpoint)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = importHandler(migrationCheckpoint)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}
