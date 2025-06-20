package warehouse

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stringify"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-go-kit/logger"

	wtypes "github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/types"
	"github.com/rudderlabs/rudder-server/processor/types"
)

func (t *Transformer) CompareResponsesAndUpload(ctx context.Context, events []types.TransformerEvent, legacyResponse types.Response) {
	if t.loggedSamplesUploader == nil {
		return
	}
	if t.loggedSamples.Load() >= int64(t.config.maxLoggedEvents.Load()) {
		return
	}
	go func() {
		defer t.stats.comparisonTime.RecordDuration()()
		t.compareResponsesAndUpload(ctx, events, legacyResponse)
	}()
}

func (t *Transformer) compareResponsesAndUpload(ctx context.Context, events []types.TransformerEvent, legacyResponse types.Response) {
	sampleDiff := t.sampleDiff(events, legacyResponse, t.Transform(ctx, events))
	if len(sampleDiff) == 0 {
		return
	}
	logEntries := lo.Map(events, func(item types.TransformerEvent, index int) string {
		return stringify.Any(wtypes.New(&item, t.uuidGenerator, t.now))
	})

	var b bytes.Buffer
	err := write(gzip.NewWriter(&b), append([]string{sampleDiff}, logEntries...))
	if err != nil {
		t.logger.Warnn("Unable to write sample diff", obskit.Error(err))
		return
	}

	objName := path.Join("embedded-wt-samples", t.config.instanceID, uuid.NewString()) + ".log.gz"
	uploadFile, err := t.loggedSamplesUploader.UploadReader(ctx, objName, &b)
	if err != nil {
		t.logger.Warnn("Unable to upload sample diff", obskit.Error(err))
		return
	}

	t.loggedSamples.Add(1)
	t.logger.Infon("Successfully logged events",
		logger.NewIntField("event_count", int64(len(logEntries))),
		logger.NewStringField("location", uploadFile.Location),
		logger.NewStringField("objectName", uploadFile.ObjectName),
	)
}

func getSamplingUploader(conf *config.Config, log logger.Logger) (filemanager.S3Manager, error) {
	var (
		bucket           = conf.GetStringVar("rudder-customer-sample-payloads", "Warehouse.Transformer.Sampling.Bucket")
		regionHint       = conf.GetStringVar("us-east-1", "Warehouse.Transformer.Sampling.RegionHint", "AWS_S3_REGION_HINT")
		endpoint         = conf.GetStringVar("", "Warehouse.Transformer.Sampling.Endpoint")
		accessKeyID      = conf.GetStringVar("", "Warehouse.Transformer.Sampling.AccessKey", "AWS_ACCESS_KEY_ID")
		secretAccessKey  = conf.GetStringVar("", "Warehouse.Transformer.Sampling.SecretAccessKey", "AWS_SECRET_ACCESS_KEY")
		s3ForcePathStyle = conf.GetBoolVar(false, "Warehouse.Transformer.Sampling.S3ForcePathStyle")
		disableSSL       = conf.GetBoolVar(false, "Warehouse.Transformer.Sampling.DisableSSL")
		enableSSE        = conf.GetBoolVar(false, "Warehouse.Transformer.Sampling.EnableSSE", "AWS_ENABLE_SSE")
	)
	s3Config := map[string]any{
		"bucketName":       bucket,
		"regionHint":       regionHint,
		"endpoint":         endpoint,
		"accessKeyID":      accessKeyID,
		"secretAccessKey":  secretAccessKey,
		"s3ForcePathStyle": s3ForcePathStyle,
		"disableSSL":       disableSSL,
		"enableSSE":        enableSSE,
	}
	return filemanager.NewS3Manager(conf, s3Config, log.Withn(logger.NewStringField("component", "wt-uploader")), func() time.Duration {
		return conf.GetDuration("Warehouse.Transformer.Sampling.Timeout", 120, time.Second)
	})
}

func (t *Transformer) sampleDiff(events []types.TransformerEvent, legacyResponse, embeddedResponse types.Response) string {
	if len(legacyResponse.Events) == 0 && len(legacyResponse.FailedEvents) == 0 {
		return "" // Don't diff in case there is no response from transformer
	}

	// If the event counts differ, return all events in the transformation
	if len(legacyResponse.Events) != len(embeddedResponse.Events) || len(legacyResponse.FailedEvents) != len(embeddedResponse.FailedEvents) {
		t.stats.mismatchedEvents.Observe(float64(len(events)))
		return fmt.Sprintf("Mismatch in response for events or failed events with legacy response: %s", stringify.Any(legacyResponse))
	}

	var (
		differedEventsCount int
		sampleDiff          string
	)

	for i := range legacyResponse.Events {
		diff := cmp.Diff(embeddedResponse.Events[i], legacyResponse.Events[i])
		if len(diff) == 0 {
			continue
		}
		// JS converts new Date('0001-01-01 00:00').toISOString() to 2001-01-01T00:00:00.000Z
		// https://www.programiz.com/online-compiler/4SqZcIH5k6Yli
		if strings.Contains(diff, "\"0001-01-01T00:00:00.000Z\"") {
			continue
		}
		// If messageID's are not present, we add it in rudder-transformer
		// https://github.com/rudderlabs/rudder-transformer/blob/develop/src/warehouse/index.js#L675-L677
		if strings.Contains(diff, "\"auto-") {
			continue
		}
		// Ignore Unicode diffs caused by Go vs JavaScript serialization differences
		if unicodePattern.MatchString(diff) {
			continue
		}
		if differedEventsCount == 0 {
			sampleDiff = diff
		}
		differedEventsCount++
	}
	t.stats.matchedEvents.Observe(float64(len(legacyResponse.Events) - differedEventsCount))
	t.stats.mismatchedEvents.Observe(float64(differedEventsCount))
	return sampleDiff
}

func write(w io.WriteCloser, data []string) error {
	for _, entry := range data {
		if _, err := w.Write([]byte(entry + "\n")); err != nil {
			return fmt.Errorf("writing to gzip writer: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("closing gzip writer: %w", err)
	}
	return nil
}
