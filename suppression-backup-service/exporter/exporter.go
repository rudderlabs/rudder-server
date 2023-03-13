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

	"github.com/rudderlabs/rudder-server/config"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/suppression-backup-service/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// ExporterOpt represents a configuration option for the exporter
type ExporterOpt func(*Exporter)

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
	err := os.MkdirAll(baseDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("could not create tmp dir: %w", err)
	}
	repository, err := suppression.NewBadgerRepository(
		baseDir,
		pkgLogger)
	if err != nil {
		return nil, fmt.Errorf("could not create badger repository: %w", err)
	}
	return repository, nil
}

type Exporter struct {
	id             identity.Identifier
	file           model.File
	log            logger.Logger
	pollIntervalFn func() time.Duration
}

// WithPollIntervalFn sets the interval at which the syncer will poll the backend
func WithPollIntervalFn(pollIntervalFn func() time.Duration) ExporterOpt {
	return func(c *Exporter) {
		c.pollIntervalFn = pollIntervalFn
	}
}

// NewExporter creates a new syncer
func NewExporter(id identity.Identifier, file model.File, log logger.Logger, opts ...ExporterOpt) (*Exporter, error) {
	e := &Exporter{
		id:   id,
		file: file,
		log:  log,
		pollIntervalFn: func() time.Duration {
			return time.Hour
		},
	}
	for _, opt := range opts {
		opt(e)
	}
	return e, nil
}

func (e *Exporter) FullExporterLoop(ctx context.Context) error {
	tmpDir, err := misc.CreateTMPDIR()
	if err != nil {
		return err
	}
	repo, err := newBadgerDBInstance(path.Join(tmpDir, "fullsuppression"), e.log)
	if err != nil {
		return err
	}
	defer func() {
		_ = repo.Stop()
	}()

	syncer, err := suppression.NewSyncer(
		config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderstack.com"),
		e.id,
		repo,
		suppression.WithLogger(e.log),
		suppression.WithHttpClient(&http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}),
		suppression.WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			syncStart := time.Now()
			if err := syncer.Sync(ctx); err != nil {
				return err
			}
			stats.Default.NewStat("suppression_backup_service_full_sync_time", stats.TimerType).Since(syncStart)
			exportStart := time.Now()
			if err = Export(repo, e.file); err != nil {
				return err
			}
			stats.Default.NewStat("suppression_backup_service_full_export_time", stats.TimerType).Since(exportStart)
			if err = misc.SleepCtx(ctx, e.pollIntervalFn()); err != nil {
				return err
			}
		}
	}
}

func (e *Exporter) LatestExporterLoop(ctx context.Context) error {
	var interval time.Duration
	config.RegisterDurationConfigVariable(600, &interval, true, time.Second, "SuppressionExporter.latestExportInterval")

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			latestExport := func() error {
				tmpDir, err := misc.CreateTMPDIR()
				if err != nil {
					return err
				}
				baseDir := path.Join(tmpDir, "latestsuppression")
				err = os.RemoveAll(baseDir)
				if err != nil {
					return fmt.Errorf("could not remove tmp dir: %w", err)
				}
				repo, err := newBadgerDBInstance(baseDir, e.log)
				if err != nil {
					return err
				}
				defer func() {
					_ = repo.Stop()
				}()

				path.Join(tmpDir, "latestsuppression")
				syncer, err := suppression.NewSyncer(
					config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderstack.com"),
					e.id,
					repo,
					suppression.WithLogger(e.log),
					suppression.WithHttpClient(&http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}),
					suppression.WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
				)
				if err != nil {
					return err
				}
				// manually add token to repo to make sure that we get suppression regulation corresponding to last one month and not older than that.
				token, err := latestToken()
				if err != nil {
					return err
				}
				if err := repo.Add(nil, []byte(token)); err != nil {
					return err
				}
				syncStart := time.Now()
				if err := syncer.Sync(ctx); err != nil {
					return err
				}
				stats.Default.NewStat("suppression_backup_service_latest_sync_time", stats.TimerType).Since(syncStart)
				exportStart := time.Now()
				if err := Export(repo, e.file); err != nil {
					return err
				}
				if err = Export(repo, e.file); err != nil {
					return err
				}
				stats.Default.NewStat("suppression_backup_service_latest_export_time", stats.TimerType).Since(exportStart)

				return nil
			}
			if err := latestExport(); err != nil {
				return err
			}
			if err := misc.SleepCtx(ctx, e.pollIntervalFn()); err != nil {
				return err
			}
		}
	}
}

func latestToken() (string, error) {
	marshalledToken, err := json.Marshal(Token{SyncStartTime: time.Now().Add(-config.GetDuration("SuppressionExporter.latestExportDuration", 30*24, time.Hour)), SyncSeqId: -1})
	if err != nil {
		return "", err
	}
	token := base64.StdEncoding.EncodeToString(marshalledToken)
	return token, nil
}

type Token struct {
	SyncStartTime time.Time
	SyncSeqId     int
}
