package reporting

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/client"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const sampleEventNotAvailableEntityTooLarge = `{"sample_event_not_available":"entity too large"}`

func sendMetricWithPayloadTooLargeSplit(ctx context.Context, commonClient *client.Client, metric *types.Metric) error {
	err := commonClient.Send(ctx, metric)
	if !errors.Is(err, client.ErrPayloadTooLarge) {
		return err
	}

	for _, statusDetail := range metric.StatusDetails {
		individualMetric := metricWithSingleStatusDetail(metric, statusDetail)
		err = commonClient.Send(ctx, individualMetric)
		if errors.Is(err, client.ErrPayloadTooLarge) {
			strippedMetric := metricWithSingleStatusDetail(metric, statusDetail)
			if len(strippedMetric.StatusDetails) > 0 && strippedMetric.StatusDetails[0] != nil {
				strippedMetric.StatusDetails[0].SampleEvent = sampleEventNotAvailableForPayloadTooLarge()
			}
			err = commonClient.SendWithoutPayloadTooLargeSplit(ctx, strippedMetric)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func metricWithSingleStatusDetail(metric *types.Metric, statusDetail *types.StatusDetail) *types.Metric {
	metricCopy := *metric
	if statusDetail == nil {
		metricCopy.StatusDetails = []*types.StatusDetail{nil}
		return &metricCopy
	}
	statusDetailCopy := *statusDetail
	metricCopy.StatusDetails = []*types.StatusDetail{&statusDetailCopy}
	return &metricCopy
}

func sendEDMetricWithPayloadTooLargeSplit(ctx context.Context, commonClient *client.Client, metric *types.EDMetric) error {
	err := commonClient.Send(ctx, metric)
	if !errors.Is(err, client.ErrPayloadTooLarge) {
		return err
	}

	for _, errorDetail := range metric.Errors {
		individualMetric := edMetricWithSingleErrorDetail(metric, errorDetail)
		err = commonClient.Send(ctx, individualMetric)
		if errors.Is(err, client.ErrPayloadTooLarge) {
			strippedMetric := edMetricWithSingleErrorDetail(metric, errorDetail)
			if len(strippedMetric.Errors) > 0 {
				strippedMetric.Errors[0].SampleEvent = sampleEventNotAvailableForPayloadTooLarge()
			}
			err = commonClient.SendWithoutPayloadTooLargeSplit(ctx, strippedMetric)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func edMetricWithSingleErrorDetail(metric *types.EDMetric, errorDetail types.EDErrorDetails) *types.EDMetric {
	metricCopy := *metric
	metricCopy.Errors = []types.EDErrorDetails{errorDetail}
	return &metricCopy
}

func sampleEventNotAvailableForPayloadTooLarge() json.RawMessage {
	return json.RawMessage(sampleEventNotAvailableEntityTooLarge)
}
