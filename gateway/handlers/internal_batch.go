package handlers

import (
	"net/http"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type InternalBatch struct {
	Config *config.Config
	Logger logger.Logger
	Stats  stats.Stats
}

func (ib *InternalBatch) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		ctx          = r.Context()
		reqType      = ctx.Value(gwtypes.CtxParamCallType).(string)
		jobs         []*jobsdb.JobT
		body         []byte
		err          error
		status       int
		errorMessage string
		responseBody string
	)

	// TODO: add tracing
	gw.logger.LogRequest(r)
	body, err = gw.getPayloadFromRequest(r)
	if err != nil {
		stat := gwstats.SourceStat{
			ReqType: reqType,
		}
		stat.RequestFailed("requestBodyReadFailed")
		stat.Report(gw.stats)
		goto requestError
	}
	jobs, err = gw.extractJobsFromInternalBatchPayload(reqType, body)
	if err != nil {
		goto requestError
	}

	if len(jobs) > 0 {
		if err = gw.storeJobs(ctx, jobs); err != nil {
			gw.stats.NewTaggedStat(
				"gateway.write_key_failed_events",
				stats.CountType,
				gw.newReqTypeStatsTagsWithReason(reqType, "storeFailed"),
			).Count(len(jobs))
			goto requestError
		}
		gw.stats.NewTaggedStat(
			"gateway.write_key_successful_events",
			stats.CountType,
			gw.newReqTypeStatsTagsWithReason(reqType, ""),
		).Count(len(jobs))

		// Sending events to config backend
		for _, job := range jobs {
			sourceID := gjson.GetBytes(job.Parameters, "source_id").String()
			writeKey, ok := gw.getWriteKeyFromSourceID(sourceID)
			if !ok {
				gw.logger.Warnn("unable to get writeKey for job",
					logger.NewStringField("uuid", job.UUID.String()),
					obskit.SourceID(sourceID))
				continue
			}
			gw.sourcehandle.RecordEvent(writeKey, job.EventPayload)
		}
	}

	status = http.StatusOK
	responseBody = response.GetStatus(response.Ok)
	gw.stats.NewTaggedStat(
		"gateway.write_key_successful_requests",
		stats.CountType,
		gw.newReqTypeStatsTagsWithReason(reqType, ""),
	).Increment()
	gw.logger.Debugn("response",
		logger.NewStringField("ip", kithttputil.GetRequestIP(r)),
		logger.NewStringField("path", r.URL.Path),
		logger.NewIntField("status", int64(status)),
		logger.NewStringField("body", responseBody),
	)
	_, _ = w.Write([]byte(responseBody))
	return

requestError:
	errorMessage = err.Error()
	status = response.GetErrorStatusCode(errorMessage)
	responseBody = response.GetStatus(errorMessage)
	gw.stats.NewTaggedStat(
		"gateway.write_key_failed_requests",
		stats.CountType,
		gw.newReqTypeStatsTagsWithReason(reqType, errorMessage),
	).Increment()
	gw.logger.Infon("response",
		logger.NewStringField("ip", kithttputil.GetRequestIP(r)),
		logger.NewStringField("path", r.URL.Path),
		logger.NewIntField("status", int64(status)),
		logger.NewStringField("body", responseBody),
	)
	gw.logger.Debugn("response",
		logger.NewStringField("ip", kithttputil.GetRequestIP(r)),
		logger.NewStringField("path", r.URL.Path),
		logger.NewIntField("status", int64(status)),
		logger.NewStringField("body", responseBody),
		logger.NewStringField("request", string(body)),
	)
	http.Error(w, responseBody, status)
}
