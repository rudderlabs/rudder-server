package reporting

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type payload struct {
	MessageID        string    `json:"messageId"`
	SourceID         string    `json:"sourceId"`
	DestinationID    string    `json:"destinationId"`
	TransformationID string    `json:"transformationId"`
	TrackingPlanID   string    `json:"trackingPlanId"`
	FailedStage      string    `json:"failedStage"`
	EventType        string    `json:"eventType"`
	EventName        string    `json:"eventName"`
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

	eir.errIndexDB = jobsdb.NewForReadWrite(
		"err_idx",
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

// Report reports the metrics to the errorIndex JobsDB
func (eir *ErrorIndexReporter) Report(metrics []*types.PUReportedMetric, _ *sql.Tx) error {
	failedAt := eir.now()

	var jobs []*jobsdb.JobT
	for _, metric := range metrics {
		if metric.StatusDetail == nil {
			continue
		}

		for _, failedMessage := range metric.StatusDetail.FailedMessages {
			workspaceID := eir.configSubscriber.WorkspaceIDFromSource(metric.SourceID)

			payload := payload{
				MessageID:        failedMessage.MessageID,
				SourceID:         metric.SourceID,
				DestinationID:    metric.DestinationID,
				TransformationID: metric.TransformationID,
				TrackingPlanID:   metric.TrackingPlanID,
				FailedStage:      metric.PUDetails.PU,
				EventName:        metric.StatusDetail.EventName,
				EventType:        metric.StatusDetail.EventType,
				ReceivedAt:       failedMessage.ReceivedAt,
				FailedAt:         failedAt,
			}
			payloadJSON, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("unable to marshal payload: %v", err)
			}

			params := struct {
				WorkspaceID string `json:"workspaceId"`
				SourceID    string `json:"source_id"`
			}{
				WorkspaceID: workspaceID,
				SourceID:    metric.SourceID,
			}
			paramsJSON, err := json.Marshal(params)
			if err != nil {
				return fmt.Errorf("unable to marshal params: %v", err)
			}

			jobs = append(jobs, &jobsdb.JobT{
				UUID:         uuid.New(),
				Parameters:   paramsJSON,
				EventPayload: payloadJSON,
				EventCount:   1,
				WorkspaceId:  workspaceID,
			})
		}
	}

	if len(jobs) == 0 {
		return nil
	}

	if err := eir.errIndexDB.Store(eir.ctx, jobs); err != nil {
		return fmt.Errorf("failed to store jobs: %v", err)
	}

	return nil
}

// DatabaseSyncer no op for error index reporting
func (eir *ErrorIndexReporter) DatabaseSyncer(
	types.SyncerConfig,
) types.ReportingSyncer {
	return func() {}
}
