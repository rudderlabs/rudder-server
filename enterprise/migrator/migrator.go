package migrator

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/enterprise/pathfinder"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// MigratorT is a handle to this object used in main.go
type MigratorT struct {
	jobsDB             *jobsdb.HandleT
	fileManager        filemanager.FileManager
	fromClusterVersion int
	toClusterVersion   int
	importer           *importerT
	exporter           *exporterT
	migrationMode      string
}

var (
	workLoopSleepDuration time.Duration
	pkgLogger             logger.LoggerI
)

const (
	notificationURI = "/notify"
)

// New gives a migrator of type export, import or export-import
// NOTE: IMPORT_EXPORT is deprecated. This is not supported going forward.
func New(mode string, jobsDB *jobsdb.HandleT, pf pathfinder.ClusterStateT) MigratorT {
	migrator := MigratorT{}
	migrator.Setup(jobsDB, mode)

	switch mode {
	case db.EXPORT:
		migrator.exporter = &exporterT{}
		migrator.exporter.Setup(&migrator, pf)
		return migrator
	case db.IMPORT:
		migrator.importer = &importerT{}
		migrator.importer.Setup(&migrator)
		return migrator
	case db.IMPORT_EXPORT:
		migrator.exporter = &exporterT{}
		migrator.importer = &importerT{}
		// Order of this setup is important. Exporter should be first setup before importer because export needs to capture last ds for export before accepting new events
		migrator.exporter.Setup(&migrator, pf)
		migrator.importer.Setup(&migrator)
		return migrator
	}
	jobsDB.RecoverFromMigrationJournal()

	panic(fmt.Sprintf("Unknown Migration Mode : %s", mode))
}

func loadConfig() {
	dbReadBatchSize = config.GetInt("Migrator.dbReadBatchSize", 10000)
	config.RegisterDurationConfigVariable(20, &exportDoneCheckSleepDuration, false, time.Second, []string{"Migrator.exportDoneCheckSleepDuration", "Migrator.exportDoneCheckSleepDurationIns"}...)
	config.RegisterDurationConfigVariable(1, &workLoopSleepDuration, false, time.Second, []string{"Migrator.workLoopSleepDuration,Migrator.workLoopSleepDurationIns"}...)
}

// Setup initializes the module
func (migrator *MigratorT) Setup(jobsDB *jobsdb.HandleT, migrationMode string) {
	pkgLogger.Infof("Migrator: Setting up migrator for %s jobsdb", jobsDB.GetTablePrefix())

	migrator.jobsDB = jobsDB
	migrator.migrationMode = migrationMode
	migrator.fromClusterVersion = GetMigratingFromVersion()
	migrator.toClusterVersion = GetMigratingToVersion()

	migrator.fileManager = migrator.setupFileManager()

	migrator.jobsDB.SetupForMigration(migrator.fromClusterVersion, migrator.toClusterVersion)
}

func (migrator *MigratorT) getURI(uri string) string {
	return fmt.Sprintf("/%s%s", migrator.jobsDB.GetTablePrefix(), uri)
}

func (migrator *MigratorT) setupFileManager() filemanager.FileManager {
	versionPrefix := fmt.Sprintf("%d-%d", migrator.fromClusterVersion, migrator.toClusterVersion)
	provider := config.GetEnv("MIGRATOR_STORAGE_PROVIDER", "S3")
	conf := map[string]interface{}{}
	conf["bucketName"] = config.GetRequiredEnv("MIGRATOR_BUCKET")
	bucketPrefix := config.GetEnv("MIGRATOR_BUCKET_PREFIX", "")

	if bucketPrefix != "" {
		bucketPrefix = fmt.Sprintf("%s/%s", bucketPrefix, versionPrefix)
	} else {
		bucketPrefix = versionPrefix
	}
	conf["prefix"] = bucketPrefix
	switch provider {
	case "S3":
		conf["accessKeyID"] = config.GetEnv("MIGRATOR_ACCESS_KEY_ID", "")
		conf["accessKey"] = config.GetEnv("MIGRATOR_SECRET_ACCESS_KEY", "")
	case "GCS":
		credentialsFilePath := config.GetRequiredEnv("GOOGLE_APPLICATION_CREDENTIALS")
		credentials, error := ioutil.ReadFile(credentialsFilePath)
		if error != nil {
			panic(fmt.Sprintf("Error when reading GCS credentials file: %s", credentialsFilePath))
		}
		conf["credentials"] = string(credentials)
	default:
		panic(fmt.Sprintf("Provider missing or Configured with unknown provider: %s", provider))
	}

	settings := filemanager.SettingsT{Provider: provider, Config: conf}
	fm, err := filemanager.DefaultFileManagerFactory.New(&settings)
	if err == nil {
		return fm
	}
	panic("Unable to get filemanager")
}

// GetMigratingFromVersion gives the from version during migration
func GetMigratingFromVersion() int {
	return config.GetRequiredEnvAsInt("MIGRATING_FROM_CLUSTER_VERSION")
}

// GetMigratingToVersion gives the from version during migration
func GetMigratingToVersion() int {
	return config.GetRequiredEnvAsInt("MIGRATING_TO_CLUSTER_VERSION")
}
