package main

import (
	"compress/gzip"
	"fmt"
	"net/http"
	"os"
	"time"

	gorilla "github.com/gorilla/handlers"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// 1. Prepare a badgerdb with 40_000_000 keys.
// 2. Backup the repo in a file
// 3. Start an HTTP server to serve the backup file (using gzip faster compression - github.com/gorilla/handlers)
func main() {
	log := logger.NewLogger().Child("main")

	repo, err := suppression.NewBadgerRepository("/tmp/badgerdb/repo", logger.NOP)
	if err != nil {
		log.Fatal("failed to start badger repository", err)
		os.Exit(1)
	}

	log.Info("adding suppressions")
	totalSuppressions := 40_000_000
	batchSize := 5000
	populationTimer := time.Now()
	for i := 0; i < totalSuppressions/batchSize; i++ {
		suppressions := generateSuppressions(i*batchSize, batchSize)
		token := []byte(fmt.Sprintf("token%d", i))
		err := repo.Add(suppressions, token)
		if err != nil {
			log.Fatal(`failed to add suppressions`, err)
			os.Exit(1)
		}
	}
	log.Infof(`populated badgerdb with %d suppressions in %f seconds`,
		totalSuppressions,
		time.Since(populationTimer).Seconds())

	log.Info(`starting backup`)
	backupTimer := time.Now()
	f, err := os.Create(`/tmp/badgerdb/backup.badger`)
	if err != nil {
		log.Fatal(`failed to create backup file`, err)
		os.Exit(1)
	}
	err = repo.Backup(f)
	if err != nil {
		log.Fatal(`failed to backup`, err)
		os.Exit(1)
	}
	fileInfo, err := f.Stat()
	if err != nil {
		log.Fatal(`failed to get backup file info`, err)
		os.Exit(1)
	}
	log.Infof(`backup completed in %f seconds`, time.Since(backupTimer).Seconds())
	log.Infof(`backup file size: %d MB`, fileInfo.Size()/1024/1024)

	fs := http.FileServer(http.Dir("/tmp/badgerdb"))
	log.Info(`starting HTTP server`)
	log.Fatal(http.ListenAndServe(
		":8080",
		gorilla.CompressHandlerLevel(fs, gzip.BestSpeed),
	))
}

func generateSuppressions(startFrom, batchSize int) []model.Suppression {
	var res []model.Suppression

	for i := startFrom; i < startFrom+batchSize; i++ {
		res = append(res, model.Suppression{
			Canceled:    false,
			WorkspaceID: "1yaBlqltp5Y4V2NK8qePowlyaaaa",
			UserID:      fmt.Sprintf("client-%d", i),
			SourceIDs:   []string{},
		})
	}
	return res
}
