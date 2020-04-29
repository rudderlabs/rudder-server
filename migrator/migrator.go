package migrator

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/bugsnag/bugsnag-go"
	"github.com/rs/cors"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/pathfinder"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

//Migrator is a handle to this object used in main.go
type Migrator struct {
	jobsDB             *jobsdb.HandleT
	fileManager        filemanager.FileManager
	fromClusterVersion int
	toClusterVersion   int
}

//Transporter interface for export and import implementations
type Transporter interface {
	Setup(jobsDB *jobsdb.HandleT, pf pathfinder.Pathfinder)
	ImportHandler(w http.ResponseWriter, r *http.Request)
	ImportStatusHandler() bool
	ExportStatusHandler() bool
}

//New gives a transporter of type export, import or export-import
func New(migrationMode string, jobsDB *jobsdb.HandleT, pf pathfinder.Pathfinder) Transporter {
	switch migrationMode {
	case "export":
		exporter := &Exporter{}
		exporter.Setup(jobsDB, pf)
		return exporter
	case "import":
		importer := &Importer{}
		importer.Setup(jobsDB, pf)
		return importer
	case "import-export":
		exportImporter := &ExportImporter{}
		exportImporter.Setup(jobsDB, pf)
		return exportImporter
	}
	panic(fmt.Sprintf("Unknown Migration Mode : %s", migrationMode))
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes the module
func (migrator *Migrator) Setup(jobsDB *jobsdb.HandleT) {
	logger.Infof("Migrator: Setting up migrator for %s jobsdb", jobsDB.GetTablePrefix())

	migrator.jobsDB = jobsDB
	migrator.fileManager = migrator.setupFileManager()

	migrator.fromClusterVersion = config.GetRequiredEnvAsInt("MIGRATING_FROM_CLUSTER_VERSION")
	migrator.toClusterVersion = config.GetRequiredEnvAsInt("MIGRATING_TO_CLUSTER_VERSION")

	migrator.jobsDB.SetupCheckpointTable()
}

func reflectOrigin(origin string) bool {
	return true
}

func (migrator *Migrator) getURI(uri string) string {
	return fmt.Sprintf("/%s%s", migrator.jobsDB.GetTablePrefix(), uri)
}

//StatusResponseT structure for status thats called by operator
type StatusResponseT struct {
	Completed bool   `json:"completed"`
	Gw        bool   `json:"gw"`
	Rt        bool   `json:"rt"`
	BatchRt   bool   `json:"batch_rt"`
	Mode      string `json:"mode,omitempty"`
}

//StartWebHandler starts the webhandler for internal communication and status
func StartWebHandler(migratorPort int, gatewayMigrator *Transporter, routerMigrator *Transporter, batchRouterMigrator *Transporter) {
	logger.Infof("Migrator: Starting migrationWebHandler on port %d", migratorPort)

	http.HandleFunc("/gw/fileToImport", (*gatewayMigrator).ImportHandler)
	http.HandleFunc("/rt/fileToImport", (*routerMigrator).ImportHandler)
	http.HandleFunc("/batch_rt/fileToImport", (*batchRouterMigrator).ImportHandler)

	http.HandleFunc("/export/status", func(w http.ResponseWriter, r *http.Request) {
		gwCompleted := (*gatewayMigrator).ExportStatusHandler()
		rtCompleted := (*routerMigrator).ExportStatusHandler()
		brtCompleted := (*batchRouterMigrator).ExportStatusHandler()
		completed := gwCompleted && rtCompleted && brtCompleted
		// completed := gwCompleted
		mode := "export"

		response := StatusResponseT{
			Completed: completed,
			Gw:        gwCompleted,
			Rt:        rtCompleted,
			BatchRt:   brtCompleted,
			Mode:      mode,
		}

		responseJSON, err := json.Marshal(response)
		if err != nil {
			panic("Invalid JSON in export status")
		}
		w.Write(responseJSON)
	})

	http.HandleFunc("/import/status", func(w http.ResponseWriter, r *http.Request) {
		gwCompleted := (*gatewayMigrator).ImportStatusHandler()
		rtCompleted := (*routerMigrator).ImportStatusHandler()
		brtCompleted := (*batchRouterMigrator).ImportStatusHandler()
		completed := gwCompleted && rtCompleted && brtCompleted
		// completed := gwCompleted
		mode := "import"

		response := StatusResponseT{
			Completed: completed,
			Gw:        gwCompleted,
			Rt:        rtCompleted,
			BatchRt:   brtCompleted,
			Mode:      mode,
		}

		responseJSON, err := json.Marshal(response)
		if err != nil {
			panic("Invalid JSON in export status")
		}
		w.Write(responseJSON)
	})

	c := cors.New(cors.Options{
		AllowOriginFunc:  reflectOrigin,
		AllowCredentials: true,
		AllowedHeaders:   []string{"*"},
	})

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(migratorPort), c.Handler(bugsnag.Handler(nil))))
}

func (migrator *Migrator) setupFileManager() filemanager.FileManager {
	conf := map[string]interface{}{}
	conf["bucketName"] = config.GetRequiredEnv("MIGRATOR_BUCKET")

	bucketPrefix := config.GetEnv("MIGRATOR_BUCKET_PREFIX", "")
	versionPrefix := fmt.Sprintf("%d-%d", migrator.fromClusterVersion, migrator.toClusterVersion)

	if bucketPrefix != "" {
		bucketPrefix = fmt.Sprintf("%s/%s", bucketPrefix, versionPrefix)
	} else {
		bucketPrefix = versionPrefix
	}
	conf["prefix"] = bucketPrefix

	conf["accessKeyID"] = config.GetEnv("MIGRATOR_ACCESS_KEY_ID", "")
	conf["accessKey"] = config.GetEnv("MIGRATOR_SECRET_ACCESS_KEY", "")
	settings := filemanager.SettingsT{Provider: "S3", Config: conf}
	fm, err := filemanager.New(&settings)
	if err == nil {
		return fm
	}
	panic("Unable to get filemanager")
}
