package migrator

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"

	"github.com/bugsnag/bugsnag-go"
	"github.com/rs/cors"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/pathfinder"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

//Migrator is a handle to this object used in main.go
type Migrator struct {
	jobsDB       *jobsdb.HandleT
	pf           pathfinder.Pathfinder
	fileManager  filemanager.FileManager
	doneLock     sync.RWMutex
	done         bool
	port         int
	dumpQueues   map[string]chan []*jobsdb.JobT
	notifyQueues map[string]chan *jobsdb.MigrationEvent
	importQueues map[string]chan *jobsdb.MigrationEvent
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes the module
func (migrator *Migrator) Setup(jobsDB *jobsdb.HandleT, pf pathfinder.Pathfinder, forExport bool, forImport bool, migratorPort int, wg *sync.WaitGroup) {
	defer wg.Done()
	logger.Infof("Migrator: Setting up migrator for %s jobsdb", jobsDB.GetTablePrefix())
	migrator.jobsDB = jobsDB
	migrator.pf = pf
	migrator.fileManager = migrator.setupFileManager()
	migrator.port = migratorPort

	migrator.jobsDB.SetupCheckpointTable()

	//take these as arguments
	migrator.jobsDB.SetupForMigrations(forExport, forImport)

	if forExport {
		rruntime.Go(func() {
			migrator.export()
		})
	}

	if forImport {
		rruntime.Go(func() {
			migrator.readFromCheckPointAndTriggerImport()
		})
	}

}

func (migrator *Migrator) exportStatusHandler() bool {
	migrator.doneLock.RLock()
	defer migrator.doneLock.RUnlock()
	return migrator.done
}

func (migrator *Migrator) importStatusHandler() bool {
	return true
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
func StartWebHandler(migratorPort int, gwMigrator *Migrator, rtMigrator *Migrator, brtMigrator *Migrator) {
	logger.Infof("Migrator: Starting migrationWebHandler on port %d", migratorPort)

	http.HandleFunc("/gw/fileToImport", gwMigrator.importHandler)
	http.HandleFunc("/rt/fileToImport", rtMigrator.importHandler)
	http.HandleFunc("/batch_rt/fileToImport", brtMigrator.importHandler)

	http.HandleFunc("/export/status", func(w http.ResponseWriter, r *http.Request) {
		gwCompleted := gwMigrator.exportStatusHandler()
		rtCompleted := rtMigrator.exportStatusHandler()
		brtCompleted := brtMigrator.exportStatusHandler()
		completed := gwCompleted && rtCompleted && brtCompleted
		// completed := gwCompleted
		mode := "export"

		response := StatusResponseT{
			Completed: completed,
			Gw:        gwCompleted,
			Rt:        completed, //TODO change
			BatchRt:   completed, //TODO change
			Mode:      mode,
		}

		responseJSON, err := json.Marshal(response)
		if err != nil {
			panic("Invalid JSON in export status")
		}
		w.Write(responseJSON)
	})

	http.HandleFunc("/import/status", func(w http.ResponseWriter, r *http.Request) {
		gwCompleted := gwMigrator.importStatusHandler()
		rtCompleted := rtMigrator.importStatusHandler()
		brtCompleted := brtMigrator.importStatusHandler()
		completed := gwCompleted && rtCompleted && brtCompleted
		// completed := gwCompleted
		mode := "import"

		response := StatusResponseT{
			Completed: completed,
			Gw:        gwCompleted,
			Rt:        rtCompleted,  //TODO change
			BatchRt:   brtCompleted, //TODO change
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
