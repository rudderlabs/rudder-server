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
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

//Migrator is a handle to this object used in main.go
type Migrator struct {
	jobsDB      *jobsdb.HandleT
	fileManager filemanager.FileManager
	port        int
	exporter	*Exporter
	importer	*Importer
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes the module
func (migrator *Migrator) Setup(jobsDB *jobsdb.HandleT, migratorPort int) {
	logger.Infof("Migrator: Setting up migrator for %s jobsdb", jobsDB.GetTablePrefix())

	migrator.jobsDB = jobsDB
	migrator.fileManager = migrator.setupFileManager()
	migrator.port = migratorPort

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
func StartWebHandler(migratorPort int, gatewayExporter *Exporter, gatewayImporter *Importer, routerExporter *Exporter, routerImporter *Importer, batchRouterExporter *Exporter, batchRouterImporter *Importer) {
	logger.Infof("Migrator: Starting migrationWebHandler on port %d", migratorPort)

	http.HandleFunc("/gw/fileToImport", gatewayImporter.importHandler)
	http.HandleFunc("/rt/fileToImport", routerImporter.importHandler)
	http.HandleFunc("/batch_rt/fileToImport", batchRouterImporter.importHandler)

	http.HandleFunc("/export/status", func(w http.ResponseWriter, r *http.Request) {
		gwCompleted := gatewayExporter.exportStatusHandler()
		rtCompleted := routerExporter.exportStatusHandler()
		brtCompleted := batchRouterExporter.exportStatusHandler()
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
		gwCompleted := gatewayImporter.importStatusHandler()
		rtCompleted := routerImporter.importStatusHandler()
		brtCompleted := batchRouterImporter.importStatusHandler()
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

	//TODO fix importing prefix bug
	/*bucketPrefix := config.GetEnv("MIGRATOR_BUCKET_PREFIX", "")
	versionPrefix := fmt.Sprintf("%d-%d", migrator.version, migrator.nextVersion)

	if bucketPrefix != "" {
		bucketPrefix = fmt.Sprintf("%s/%s", bucketPrefix, versionPrefix)
	} else {
		bucketPrefix = versionPrefix
	}
	conf["prefix"] = bucketPrefix*/

	conf["accessKeyID"] = config.GetEnv("MIGRATOR_ACCESS_KEY_ID", "")
	conf["accessKey"] = config.GetEnv("MIGRATOR_SECRET_ACCESS_KEY", "")
	settings := filemanager.SettingsT{"S3", conf}
	fm, err := filemanager.New(&settings)
	// _ = err
	// return fm
	if err == nil {
		return fm
	}
	panic("Unable to get filemanager")
}
