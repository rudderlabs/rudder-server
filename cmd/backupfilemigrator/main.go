package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/urfave/cli/v2"

	kitconfig "github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	kitlogger "github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
)

type newFileFormat struct {
	UserID       string          `json:"userId"`
	EventPayload json.RawMessage `json:"payload"`
	CreatedAt    time.Time       `json:"createdAt"`
	MessageID    string          `json:"messageId"`
}

var app = &cli.App{
	Name:  "migration-tool",
	Usage: "Backup file migration tool",
	Commands: []*cli.Command{
		{
			Name:  "migrate",
			Usage: "Backup file migration",
			Flags: []cli.Flag{
				&cli.TimestampFlag{
					Name:     "startTime",
					Layout:   time.RFC3339Nano,
					Usage:    "start time from which we need to convert files in RFC3339 format",
					Required: true,
				},
				&cli.TimestampFlag{
					Name:     "endTime",
					Layout:   time.RFC3339Nano,
					Usage:    "end time upto which we need to convert files in RFC3339 format",
					Required: true,
				},
				&cli.IntFlag{
					Name:     "uploadBatchSize",
					Usage:    "no. of messages to upload in single file",
					Required: true,
				},
				&cli.StringFlag{
					Name:     "backupFileNamePrefix",
					Usage:    "prefix to identify files in the bucket",
					Required: true,
				},
			},
			Action: func(c *cli.Context) error {
				return migrate(c)
			},
		},
	},
}

func init() {
	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func migrate(c *cli.Context) error {
	ctx := c.Context
	startTime := c.Timestamp("startTime")
	endTime := c.Timestamp("endTime")
	uploadBatchSize := c.Int("uploadBatchSize")
	backupFileNamePrefix := c.String("backupFileNamePrefix")
	conf := kitconfig.New()
	logger := kitlogger.NewFactory(conf).NewLogger().Child("backup-file-migrator")

	return run(ctx, conf, logger, *startTime, *endTime, uploadBatchSize, backupFileNamePrefix)
}

func run(
	ctx context.Context,
	conf *kitconfig.Config,
	logger kitlogger.Logger,
	startTime time.Time,
	endTime time.Time,
	uploadBatchSize int,
	backupFileNamePrefix string,
) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	backupFileNamePrefix = strings.TrimSpace(backupFileNamePrefix)
	if backupFileNamePrefix == "" {
		return fmt.Errorf("backupFileNamePrefix should not be empty")
	}

	storageProvider := kitconfig.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
	fileManagerOpts := filemanagerutil.ProviderConfigOpts(ctx, storageProvider, conf)
	fileManagerOpts.Prefix = backupFileNamePrefix
	fileManager, err := filemanager.New(&filemanager.Settings{
		Provider: storageProvider,
		Config:   filemanager.GetProviderConfigFromEnv(fileManagerOpts),
		Conf:     conf,
	})
	if err != nil {
		return fmt.Errorf("creating file manager, err: %w", err)
	}

	migratorConfig := &config{
		startTime:            startTime,
		endTime:              endTime,
		backupFileNamePrefix: backupFileNamePrefix,
		uploadBatchSize:      uploadBatchSize,
	}

	migrator := &fileMigrator{
		conf:        migratorConfig,
		logger:      logger,
		fileManager: fileManager,
	}
	// list all files to migrate
	listOfFiles := migrator.listFilePathToMigrate(ctx)

	// step 2: download data from file
	for _, fileInfo := range listOfFiles {
		err := migrator.processFile(ctx, fileInfo)
		if err != nil {
			logger.Errorn("fail to process file", kitlogger.NewStringField("filePath", fileInfo.filePath), obskit.Error(err))
			return err
		}
	}
	return nil
}
