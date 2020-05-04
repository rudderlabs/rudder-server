package migrator

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//Migrator is a handle to this object used in main.go
type Migrator struct {
	jobsDB             *jobsdb.HandleT
	fileManager        filemanager.FileManager
	fromClusterVersion int
	toClusterVersion   int
	importer           *Importer
	exporter           *Exporter
}

//New gives a transporter of type export, import or export-import
func New(migrationMode string, jobsDB *jobsdb.HandleT, pf pathfinder.Pathfinder) Migrator {
	migrator := Migrator{}
	switch migrationMode {
	case "export":
		migrator.exporter = &Exporter{}
		migrator.exporter.Setup(jobsDB, pf)
		return migrator
	case "import":
		migrator.importer = &Importer{}
		migrator.importer.Setup(jobsDB)
		return migrator
	case "import-export":
		migrator.exporter = &Exporter{}
		migrator.exporter.Setup(jobsDB, pf)
		migrator.importer = &Importer{}
		migrator.importer.Setup(jobsDB)
		return migrator
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
	migrator.fromClusterVersion = misc.GetMigratingFromVersion()
	migrator.toClusterVersion = misc.GetMigratingToVersion()

	migrator.fileManager = migrator.setupFileManager()

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
func StartWebHandler(migratorPort int, gatewayMigrator *Migrator, routerMigrator *Migrator, batchRouterMigrator *Migrator,
	StartProcessor func(enableProcessor bool, gatewayDB, routerDB, batchRouterDB *jobsdb.HandleT),
	StartRouter func(enableRouter bool, routerDB, batchRouterDB *jobsdb.HandleT)) {
	logger.Infof("Migrator: Starting migrationWebHandler on port %d", migratorPort)

	http.HandleFunc("/gw/fileToImport", importRequestHandler((*gatewayMigrator).importer.importHandler))
	http.HandleFunc("/rt/fileToImport", importRequestHandler((*routerMigrator).importer.importHandler))
	http.HandleFunc("/batch_rt/fileToImport", importRequestHandler((*batchRouterMigrator).importer.importHandler))

	http.HandleFunc("/postImport", func(w http.ResponseWriter, r *http.Request) {
		StartProcessor(config.GetBool("enableProcessor", true), gatewayMigrator.jobsDB, routerMigrator.jobsDB, batchRouterMigrator.jobsDB)
		StartRouter(config.GetBool("enableRouter", true), routerMigrator.jobsDB, batchRouterMigrator.jobsDB)
	})

	http.HandleFunc("/export/status", func(w http.ResponseWriter, r *http.Request) {
		gwCompleted := (*gatewayMigrator).exporter.exportStatusHandler()
		rtCompleted := (*routerMigrator).exporter.exportStatusHandler()
		brtCompleted := (*batchRouterMigrator).exporter.exportStatusHandler()
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
		gwCompleted := (*gatewayMigrator).importer.importStatusHandler()
		rtCompleted := (*routerMigrator).importer.importStatusHandler()
		brtCompleted := (*batchRouterMigrator).importer.importStatusHandler()
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

func importRequestHandler(importHandler func(jobsdb.MigrationEvent)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)
		r.Body.Close()
		migrationEvent := jobsdb.MigrationEvent{}
		err := json.Unmarshal(body, &migrationEvent)
		if err != nil {
			panic(err)
		}
		importHandler(migrationEvent)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
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
