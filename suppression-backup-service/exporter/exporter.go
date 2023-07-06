package exporter

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func Export(repo suppression.Repository, file model.File) error {
	// export initially to a temp file
	tmpDir, err := misc.CreateTMPDIR()
	if err != nil {
		return fmt.Errorf("could not create tmp dir: %w", err)
	}

	tempFile, err := os.CreateTemp(tmpDir, "tmp-export")
	if err != nil {
		return fmt.Errorf("error creating temp file: %w", err)
	}
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
	}()

	// export to temp file
	err = repo.Backup(tempFile)
	if err != nil {
		return fmt.Errorf("error exporting suppression list: %w", err)
	}

	// move temp file to final destination
	file.Mu.Lock()
	defer file.Mu.Unlock()
	fmt.Printf("renaming file: %s to %s\n", tempFile.Name(), file.Path)
	err = os.Rename(tempFile.Name(), file.Path)
	if err != nil {
		return fmt.Errorf("error moving temp file to final destination: %w", err)
	}

	return nil
}

func newBadgerDBInstance(baseDir string, pkgLogger logger.Logger) (suppression.Repository, error) {
	repository, err := suppression.NewBadgerRepository(
		baseDir,
		pkgLogger)
	if err != nil {
		return nil, fmt.Errorf("could not create badger repository: %w", err)
	}
	return repository, nil
}

type Exporter struct {
	Id   identity.Identifier
	File model.File
	Log  logger.Logger
}

func (e *Exporter) FullExporterLoop(ctx context.Context) error {
	pollInterval := config.GetDuration("SuppressionExporter.fullExportInterval", 24, time.Hour)
	tmpDir, err := misc.CreateTMPDIR()
	if err != nil {
		return fmt.Errorf("fullExporterLoop: %w", err)
	}
	repo, err := newBadgerDBInstance(path.Join(tmpDir, "fullsuppressionV2"), e.Log)
	if err != nil {
		return fmt.Errorf("fullExporterLoop: %w", err)
	}
	defer func() {
		_ = repo.Stop()
	}()

	syncer, err := suppression.NewSyncer(
		config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderstack.com"),
		e.Id,
		repo,
		suppression.WithLogger(e.Log),
		suppression.WithHttpClient(&http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}),
		suppression.WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
	)
	if err != nil {
		return fmt.Errorf("fullExporterLoop: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			syncStart := time.Now()
			if err := syncer.Sync(ctx); err != nil {
				return fmt.Errorf("fullExporterLoop: %w", err)
			}
			stats.Default.NewStat("suppression_backup_service_full_sync_time", stats.TimerType).Since(syncStart)
			exportStart := time.Now()
			if err = Export(repo, e.File); err != nil {
				return fmt.Errorf("fullExporterLoop: %w", err)
			}
			stats.Default.NewStat("suppression_backup_service_full_export_time", stats.TimerType).Since(exportStart)
			if err = misc.SleepCtx(ctx, pollInterval); err != nil {
				return fmt.Errorf("fullExporterLoop: %w", err)
			}
		}
	}
}

func (e *Exporter) LatestExporterLoop(ctx context.Context) error {
	pollInterval := config.GetDuration("SuppressionExporter.latestExportInterval", 24, time.Hour)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			latestExport := func() error {
				tmpDir, err := misc.CreateTMPDIR()
				if err != nil {
					return fmt.Errorf("latestExporterLoop: %w", err)
				}
				baseDir := path.Join(tmpDir, "latestsuppression")
				err = os.RemoveAll(baseDir)
				if err != nil {
					return fmt.Errorf("could not remove tmp dir: %w", err)
				}
				repo, err := newBadgerDBInstance(baseDir, e.Log)
				if err != nil {
					return fmt.Errorf("latestExporterLoop: %w", err)
				}
				defer func() {
					_ = repo.Stop()
				}()

				path.Join(tmpDir, "latestsuppression")
				syncer, err := suppression.NewSyncer(
					config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderstack.com"),
					e.Id,
					repo,
					suppression.WithLogger(e.Log),
					suppression.WithHttpClient(&http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}),
					suppression.WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
				)
				if err != nil {
					return fmt.Errorf("latestExporterLoop: %w", err)
				}
				// manually add token to repo to make sure that we get suppression regulation corresponding to last one month and not older than that.
				token, err := latestToken()
				if err != nil {
					return fmt.Errorf("latestExporterLoop: %w", err)
				}
				if err := repo.Add(nil, []byte(token)); err != nil {
					return fmt.Errorf("latestExporterLoop: %w", err)
				}
				syncStart := time.Now()
				if err := syncer.Sync(ctx); err != nil {
					return fmt.Errorf("latestExporterLoop: %w", err)
				}
				stats.Default.NewStat("suppression_backup_service_latest_sync_time", stats.TimerType).Since(syncStart)
				exportStart := time.Now()
				if err := Export(repo, e.File); err != nil {
					return fmt.Errorf("latestExporterLoop: %w", err)
				}
				stats.Default.NewStat("suppression_backup_service_latest_export_time", stats.TimerType).Since(exportStart)

				return nil
			}
			if err := latestExport(); err != nil {
				return err
			}
			if err := misc.SleepCtx(ctx, pollInterval); err != nil {
				return err
			}
		}
	}
}

func latestToken() (string, error) {
	marshalledToken, err := json.Marshal(Token{SyncStartTime: time.Now().Add(-config.GetDuration("SuppressionExporter.latestExportDuration", 30*24, time.Hour)), SyncSeqId: -1})
	if err != nil {
		return "", fmt.Errorf("latestToken: %w", err)
	}
	token := base64.StdEncoding.EncodeToString(marshalledToken)
	return token, nil
}

type Token struct {
	SyncStartTime time.Time
	SyncSeqId     int
}
