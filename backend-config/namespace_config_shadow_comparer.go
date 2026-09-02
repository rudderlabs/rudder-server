package backendconfig

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
)

// shadowComparer compares the two sides of a sample and reports what it finds: counters for the
// dashboard, and for a divergence the configs themselves, uploaded for offline inspection.
type shadowComparer struct {
	logger     logger.Logger
	stats      stats.Stats
	namespace  string
	instanceID string

	// nil when unconfigured: divergences are still counted and logged, only the artifacts are lost
	uploader   shadowUploader
	maxUploads config.ValueLoader[int]
	uploaded   atomic.Int64

	compared, matched, sampleDropped, turnSkipped stats.Counter
	comparisonTime                                stats.Timer
}

func newShadowComparer(
	conf *config.Config, log logger.Logger, stat stats.Stats, namespace string, uploader shadowUploader,
) *shadowComparer {
	return &shadowComparer{
		logger:         log,
		stats:          stat,
		namespace:      namespace,
		instanceID:     conf.GetStringVar("", "INSTANCE_ID"),
		uploader:       uploader,
		maxUploads:     conf.GetReloadableIntVar(5, 1, "BackendConfigShadow.maxUploads"),
		compared:       stat.NewStat("bcv2_shadow_compared", stats.CountType),
		matched:        stat.NewStat("bcv2_shadow_matched", stats.CountType),
		sampleDropped:  stat.NewStat("bcv2_shadow_sample_dropped", stats.CountType),
		turnSkipped:    stat.NewStat("bcv2_shadow_turn_skipped", stats.CountType),
		comparisonTime: stat.NewStat("bcv2_shadow_comparison_time", stats.TimerType),
	}
}

func (c *shadowComparer) compare(ctx context.Context, primary, candidate map[string]ConfigT) {
	// membership is counted, not diffed: a workspace added or removed between the two fetches
	// moves no timestamp, so the gate cannot catch it (§3.3.2 in design doc)
	var diverging []string
	for workspaceID, primaryConfig := range primary {
		candidateConfig, ok := candidate[workspaceID]
		if !ok {
			c.membership("v1only")
			continue
		}
		c.compared.Increment()
		fields := divergingFields(shadowNormalize(primaryConfig), shadowNormalize(candidateConfig))
		if len(fields) == 0 {
			c.matched.Increment()
			continue
		}
		for _, field := range fields {
			c.diverged(field)
		}
		diverging = append(diverging, workspaceID)
	}
	for workspaceID := range candidate {
		if _, ok := primary[workspaceID]; !ok {
			c.membership("v2only")
		}
	}
	if len(diverging) > 0 {
		c.report(ctx, diverging, primary, candidate)
	}
}

// shadowSample is the artifact a divergence uploads: the diverging workspaces from both sides -
// never the whole namespace, which can run to hundreds of megabytes - and the diff of their
// normalized forms, so the artifact is readable without a local replay.
type shadowSample struct {
	Namespace    string             `json:"namespace"`
	WorkspaceIDs []string           `json:"workspaceIds"`
	Diff         string             `json:"diff"`
	V1           map[string]ConfigT `json:"v1"`
	V2           map[string]ConfigT `json:"v2"`
}

func (c *shadowComparer) report(ctx context.Context, diverging []string, primary, candidate map[string]ConfigT) {
	if c.uploader == nil {
		c.logger.Warnn("shadow comparison diverged (no uploader configured)",
			logger.NewIntField("workspaces", int64(len(diverging))))
		return
	}
	// a systematic divergence diverges on every workspace of every sample; without a budget that
	// is an unbounded write loop. Failed attempts refund it, so they cannot hide later, genuine
	// divergences
	if c.uploaded.Load() >= int64(c.maxUploads.Load()) {
		return
	}
	c.uploaded.Add(1)

	sample := shadowSample{
		Namespace:    c.namespace,
		WorkspaceIDs: diverging,
		V1:           make(map[string]ConfigT, len(diverging)),
		V2:           make(map[string]ConfigT, len(diverging)),
	}
	var diff bytes.Buffer
	for _, workspaceID := range diverging {
		sample.V1[workspaceID] = primary[workspaceID]
		sample.V2[workspaceID] = candidate[workspaceID]
		fmt.Fprintf(&diff, "workspace %s (-v1 +v2):\n%s\n", workspaceID,
			configDiff(shadowNormalize(primary[workspaceID]), shadowNormalize(candidate[workspaceID])))
	}
	sample.Diff = diff.String()

	payload, err := jsonrs.Marshal(sample)
	if err == nil {
		// the sample carries two full copies of every diverging workspace's config
		payload, err = gzipped(payload)
	}
	if err != nil {
		c.uploaded.Add(-1)
		c.errored("encode")
		c.logger.Errorn("shadow comparison cannot encode sample", obskit.Error(err))
		return
	}
	// partitioned by day so a bucket lifecycle rule can expire old samples, and browsing during an
	// incident starts from a date. The instance id keeps concurrently sampling pods off each
	// other's keys, and names the pod that saw the divergence
	now := time.Now().UTC()
	objName := filepath.Join("bcv2-shadow-samples", now.Format("2006/01/02"), c.namespace,
		fmt.Sprintf("%s-%s.json.gz", now.Format("150405"), c.instanceID))
	file, err := c.uploader.UploadReader(ctx, objName, bytes.NewReader(payload))
	if err != nil {
		c.uploaded.Add(-1)
		c.errored("upload")
		c.logger.Errorn("shadow comparison sample upload failed", obskit.Error(err))
		return
	}
	c.logger.Warnn("shadow comparison diverged",
		logger.NewIntField("workspaces", int64(len(diverging))),
		logger.NewStringField("location", file.Location),
		logger.NewStringField("objectName", file.ObjectName),
	)
}

func (c *shadowComparer) diverged(field string) {
	c.stats.NewTaggedStat("bcv2_shadow_diverged", stats.CountType, stats.Tags{"field": field}).Increment()
}

func (c *shadowComparer) membership(side string) {
	c.stats.NewTaggedStat("bcv2_shadow_membership", stats.CountType, stats.Tags{"side": side}).Increment()
}

func (c *shadowComparer) errored(reason string) {
	c.stats.NewTaggedStat("bcv2_shadow_error", stats.CountType, stats.Tags{"reason": reason}).Increment()
}

func gzipped(payload []byte) ([]byte, error) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(payload); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return compressed.Bytes(), nil
}

// divergingFields names the top-level ConfigT fields on which the two sides differ, empty when
// they agree.
func divergingFields(primary, candidate ConfigT) []string {
	reporter := &topLevelFieldReporter{fields: map[string]struct{}{}}
	cmp.Equal(primary, candidate, append(shadowCmpOptions(), cmp.Reporter(reporter))...)
	return slices.Sorted(maps.Keys(reporter.fields))
}

// configDiff renders the divergence between the two sides, for the uploaded artifact.
func configDiff(primary, candidate ConfigT) string {
	return cmp.Diff(primary, candidate, shadowCmpOptions()...)
}

// configCmpOptions compares two ConfigT documents by value rather than by bytes: v1's raw fields
// arrive as the control plane serialized them, v2's are produced here, so identical configs are
// almost never identical bytes. Slice order is nondeterministic on both sides, since the mapper
// builds them by ranging Go maps. Shared between the shadow comparison and the fixture test.
func configCmpOptions() cmp.Options {
	return cmp.Options{
		cmp.Transformer("rawJSON", func(raw json.RawMessage) any {
			if len(raw) == 0 {
				return nil
			}
			var value any
			if err := jsonrs.Unmarshal(raw, &value); err != nil {
				return string(raw)
			}
			return value
		}),
		cmpopts.SortSlices(func(a, b SourceT) bool { return a.ID < b.ID }),
		cmpopts.SortSlices(func(a, b DestinationT) bool { return a.ID < b.ID }),
		cmpopts.SortSlices(func(a, b TransformationT) bool { return a.ID < b.ID }),
		// A12: the account definition catalogue is namespace global and is handed over whole,
		// where v1 prunes it to what each workspace references purely to keep its copy small
		cmpopts.IgnoreFields(ConfigT{}, "AccountDefinitions"),
	}
}

// shadowCmpOptions is configCmpOptions for the live comparison, which must also ignore UpdatedAt:
// the v1 endpoint moves it with the common config under incremental updates, so the two sides
// disagree on it without either being wrong.
func shadowCmpOptions() cmp.Options {
	return append(configCmpOptions(), cmpopts.IgnoreFields(ConfigT{}, "UpdatedAt"))
}

// topLevelFieldReporter records, for every difference cmp finds, the ConfigT field it sits under.
type topLevelFieldReporter struct {
	path   cmp.Path
	fields map[string]struct{}
}

func (r *topLevelFieldReporter) PushStep(step cmp.PathStep) { r.path = append(r.path, step) }
func (r *topLevelFieldReporter) PopStep()                   { r.path = r.path[:len(r.path)-1] }

func (r *topLevelFieldReporter) Report(result cmp.Result) {
	if result.Equal() {
		return
	}
	for _, step := range r.path {
		if field, ok := step.(cmp.StructField); ok {
			r.fields[field.Name()] = struct{}{}
			return
		}
	}
}

// shadowNormalize returns the config with the fields the comparison must ignore blanked, on a
// copy: the value handed in is the live config the rest of the process is using, and blanking it
// in place would corrupt production config from the observability path. Sources and Connections
// hold value types, so cloning the containers is what makes the blanking below safe.
func shadowNormalize(config ConfigT) ConfigT {
	config.Sources = slices.Clone(config.Sources)
	config.Connections = maps.Clone(config.Connections)
	// before the source configs are blanked: this one reads origin out of them
	blankProfilesTableConnectionConfigs(&config)
	blankEnrichedSourceConfigs(&config)
	return config
}

// unenrichedSourceCategories are the source categories the control plane serves as stored. The
// mapper maps every source the same way, but the control plane enriches cloud, singer-protocol and
// warehouse source configs before serving them (credentials, resources, audiences), and that
// enrichment is not ported (D2 in design doc) - so only the unenriched categories can be compared.
// An allowlist, because enrichment dispatches on a mix of category and config.origin.
var unenrichedSourceCategories = map[string]struct{}{"": {}, "webhook": {}}

// blankEnrichedSourceConfigs clears the config of every source of an enriched category, where the
// control plane's output differs from the mapper's pass-through by design.
func blankEnrichedSourceConfigs(config *ConfigT) {
	for i := range config.Sources {
		if _, ok := unenrichedSourceCategories[config.Sources[i].SourceDefinition.Category]; !ok {
			config.Sources[i].Config = nil
		}
	}
}

// blankProfilesTableConnectionConfigs clears the config of connections whose source is a profiles
// table. B7's clause for those is enrichment dependent and is not ported, so the control plane
// writes the source's table into them and the mapper does not.
func blankProfilesTableConnectionConfigs(config *ConfigT) {
	profilesTableSources := make(map[string]struct{})
	for _, source := range config.Sources {
		if jsonparser.GetStringOrEmpty(source.Config, "origin") == "profiles-table" {
			profilesTableSources[source.ID] = struct{}{}
		}
	}
	for id, connection := range config.Connections {
		if _, ok := profilesTableSources[connection.SourceID]; ok {
			connection.Config = nil
			config.Connections[id] = connection
		}
	}
}
