package main

import (
	"net/http"
	"os"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// 1. Create a badgerbd.Repository
// 2. Restore by reading from the backup file retrieved from the HTTP server
func main() {
	log := logger.NewLogger().Child("main")
	repo, err := suppression.NewBadgerRepository("/tmp/badgerdb", logger.NOP)
	if err != nil {
		log.Fatal("failed to start badger repository", err)
		os.Exit(1)
	}
	start := time.Now()
	resp, err := http.Get(config.GetString("BACKUP_URL", "http://localhost:8080/backup.badger"))
	if err != nil {
		log.Fatal("failed to fetch the backup file", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	err = repo.Restore(resp.Body)
	if err != nil {
		log.Fatal("failed to restore from backup file", err)
		os.Exit(1)
	}
	log.Infof("restore completed in %f seconds", time.Since(start).Seconds())
}
