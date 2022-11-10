package main

import (
	"io"
	"net/http"
	"os"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// 1. Create a badgerbd.Repository
// 2. Restore by reading from the backup file retrieved from the HTTP server
func main() {
	log := logger.NewLogger().Child("main")
	start := time.Now()
	serverURL := config.GetString("BACKUP_URL", "http://localhost:8080/backup.badger")
	log.Info(`fetching backup file from `, serverURL)
	resp, err := http.Get(serverURL)
	if err != nil {
		log.Fatal("failed to fetch the backup file", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	log.Infof(`fetched file in %f seconds`, time.Since(start).Seconds())
	backupFile, err := os.Create("/tmp/backup.badger")
	if err != nil {
		log.Fatal("failed to create backup file", err)
		os.Exit(1)
	}
	defer backupFile.Close()
	_, err = io.Copy(backupFile, resp.Body)
	if err != nil {
		log.Fatal("failed to copy backup file", err)
		os.Exit(1)
	}
	fileInfo, err := backupFile.Stat()
	if err != nil {
		log.Fatal(`failed to get backup file info`, err)
		os.Exit(1)
	}
	log.Infof(`copied %d MB to backup file`, fileInfo.Size()/1024/1024)
	log.Infof("fetch and copy completed in %f seconds", time.Since(start).Seconds())
}
