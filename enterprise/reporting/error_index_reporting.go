package reporting

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	customVal = "error_index"
)

type metadata struct {
	MessageID        string    `json:"messageId"`
	SourceID         string    `json:"sourceId"`
	DestinationID    string    `json:"destinationId"`
	TransformationID string    `json:"transformationId"`
	TrackingPlanID   string    `json:"trackingPlanId"`
	FailedStage      string    `json:"failedStage"`
	ReceivedAt       time.Time `json:"receivedAt"`
	FailedAt         time.Time `json:"failedAt"`
}

type ErrorIndexReporter struct {
	ctx              context.Context
	log              logger.Logger
	configSubscriber *configSubscriber
	errIndexDB       *jobsdb.Handle
	now              func() time.Time

	config struct {
		dsLimit              misc.ValueLoader[int]
		skipMaintenanceError bool
		jobRetention         time.Duration
	}
}

func NewErrorIndexReporter(
	ctx context.Context,
	conf *config.Config,
	log logger.Logger,
	configSubscriber *configSubscriber,
) *ErrorIndexReporter {
	eir := &ErrorIndexReporter{
		ctx:              ctx,
		log:              log,
		configSubscriber: configSubscriber,
		now:              time.Now,
	}

	eir.config.dsLimit = conf.GetReloadableIntVar(0, 1, "Reporting.errorIndexReporting.dsLimit")
	eir.config.skipMaintenanceError = conf.GetBool("Reporting.errorIndexReporting.skipMaintenanceError", false)
	eir.config.jobRetention = conf.GetDurationVar(24, time.Hour, "Reporting.errorIndexReporting.jobRetention")

	eir.errIndexDB = jobsdb.NewForWrite(
		"error_index",
		jobsdb.WithDSLimit(eir.config.dsLimit),
		jobsdb.WithConfig(conf),
		jobsdb.WithSkipMaintenanceErr(eir.config.skipMaintenanceError),
		jobsdb.WithJobMaxAge(
			func() time.Duration {
				return eir.config.jobRetention
			},
		),
	)
	if err := eir.errIndexDB.Start(); err != nil {
		panic(fmt.Sprintf("failed to start error index db: %v", err))
	}

	return eir
}

// Report reports the metrics to the errIndexDB
func (eir *ErrorIndexReporter) Report(metrics []*types.PUReportedMetric, _ *sql.Tx) {
	if len(metrics) == 0 {
		return
	}

	failedJobs := lo.SumBy(metrics, func(metric *types.PUReportedMetric) int {
		if metric.StatusDetail == nil {
			return 0
		}
		return len(metric.StatusDetail.FailedMessages)
	})
	if failedJobs == 0 {
		return
	}

	jobs := make([]*jobsdb.JobT, 0, failedJobs)

	for _, metric := range metrics {
		if metric.StatusDetail == nil {
			continue
		}

		failedMessages := metric.StatusDetail.FailedMessages
		if len(failedMessages) == 0 {
			continue
		}

		for _, failedMessage := range failedMessages {
			metadata := metadata{
				MessageID:        failedMessage.MessageID,
				SourceID:         metric.SourceID,
				DestinationID:    metric.DestinationID,
				TransformationID: metric.TransformationID,
				TrackingPlanID:   metric.TrackingPlanID,
				FailedStage:      metric.PUDetails.PU,
				FailedAt:         eir.now(),
				ReceivedAt:       failedMessage.ReceivedAt,
			}

			metadataJSON, err := json.Marshal(metadata)
			if err != nil {
				panic(err)
			}

			jobs = append(jobs, &jobsdb.JobT{
				UUID:         uuid.New(),
				UserID:       uuid.New().String(), // TODO: Check with @atzoum around what to use here?
				Parameters:   metadataJSON,
				CustomVal:    customVal,
				EventPayload: json.RawMessage(`{}`),
				EventCount:   0,
				WorkspaceId:  eir.configSubscriber.WorkspaceIDFromSource(metric.SourceID),
			})
		}
	}

	if err := eir.errIndexDB.Store(eir.ctx, jobs); err != nil {
		eir.log.Errorw("unable to store error index jobs", "error", err)
	}
}

// DatabaseSyncer returns a syncer that syncs the database
// Since errIndexDB already syncs the database,
// It just returns a function that stops the errIndexDB once the context is done
func (eir *ErrorIndexReporter) DatabaseSyncer(
	types.SyncerConfig,
) types.ReportingSyncer {
	return func() {
		<-eir.ctx.Done()

		eir.errIndexDB.Stop()
	}
}
