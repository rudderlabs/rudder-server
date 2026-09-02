package backendconfig

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestShadowConfigFetcher(t *testing.T) {
	t0 := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)

	t.Run("serves the primary untouched", func(t *testing.T) {
		t.Run("result", func(t *testing.T) {
			primary := &shadowFetcherStub{configs: map[string]ConfigT{"ws-1": {WorkspaceID: "ws-1"}}}
			// the candidate's config differs from the primary's, so that the assertions below can
			// tell which of the two sides was served
			candidate := &shadowFetcherStub{configs: map[string]ConfigT{
				"ws-1": {WorkspaceID: "ws-1", Sources: []SourceT{{ID: "s1"}}},
			}}
			fetcher, store := newShadowFetcherForTest(t, primary, candidate)

			configs, err := fetcher.Get(context.Background())
			require.NoError(t, err)
			require.Equal(t, primary.configs, configs)
			require.NotEqual(t, candidate.configs, configs)
			awaitSample(t, fetcher)
			require.EqualValues(t, 1, candidate.calls.Load())
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_compared", nil))
			timer := store.Get("bcv2_shadow_comparison_time", nil)
			require.NotNil(t, timer, "the comparison is timed")
			require.Len(t, timer.Durations(), 1)
		})

		t.Run("error, without spending a sample on it", func(t *testing.T) {
			primary := &shadowFetcherStub{err: errors.New("boom")}
			candidate := &shadowFetcherStub{}
			fetcher, _ := newShadowFetcherForTest(t, primary, candidate)

			_, err := fetcher.Get(context.Background())
			require.ErrorContains(t, err, "boom")
			// lastRun is written by the calling goroutine before a sample is launched, so an
			// untouched one is proof no sample was started, where candidate.calls alone would
			// race the sample goroutine into passing
			require.True(t, fetcher.lastRun.IsZero(), "the failed poll did not spend a sample")
			require.EqualValues(t, 0, candidate.calls.Load())
		})
	})

	t.Run("sampling", func(t *testing.T) {
		t.Run("at most once per interval", func(t *testing.T) {
			primary := &shadowFetcherStub{configs: map[string]ConfigT{}}
			candidate := &shadowFetcherStub{configs: map[string]ConfigT{}}
			fetcher, store := newShadowFetcherForTest(t, primary, candidate)
			fetcher.interval = time.Hour

			_, _ = fetcher.Get(context.Background())
			awaitSample(t, fetcher)
			lastRun := fetcher.lastRun

			_, _ = fetcher.Get(context.Background())
			require.Equal(t, lastRun, fetcher.lastRun, "the second poll did not take a turn")
			require.EqualValues(t, 1, candidate.calls.Load())
			// within the interval is not a skipped turn, it is simply not the time to sample
			require.Equal(t, 0.0, counterValue(store, "bcv2_shadow_turn_skipped", nil))
		})

		t.Run("skips its turn while a sample is in flight", func(t *testing.T) {
			release := make(chan struct{})
			primary := &shadowFetcherStub{configs: map[string]ConfigT{}}
			candidate := &shadowFetcherStub{configs: map[string]ConfigT{}, release: release}
			fetcher, store := newShadowFetcherForTest(t, primary, candidate)

			_, _ = fetcher.Get(context.Background())
			require.Eventually(t, func() bool { return candidate.calls.Load() == 1 },
				time.Second, time.Millisecond)
			_, _ = fetcher.Get(context.Background())
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_turn_skipped", nil))
			require.EqualValues(t, 1, candidate.calls.Load())

			close(release)
			awaitSample(t, fetcher)
		})

		t.Run("recovers a panicking sample", func(t *testing.T) {
			primary := &shadowFetcherStub{configs: map[string]ConfigT{}}
			fetcher, store := newShadowFetcherForTest(t, primary, &panickingFetcher{})

			_, err := fetcher.Get(context.Background())
			require.NoError(t, err)
			awaitSample(t, fetcher)
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_error", stats.Tags{"reason": "panic"}))
		})

		t.Run("counts a failing candidate fetch", func(t *testing.T) {
			primary := &shadowFetcherStub{configs: map[string]ConfigT{}}
			candidate := &shadowFetcherStub{err: errors.New("boom")}
			fetcher, store := newShadowFetcherForTest(t, primary, candidate)

			_, err := fetcher.Get(context.Background())
			require.NoError(t, err)
			awaitSample(t, fetcher)
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_error", stats.Tags{"reason": "fetch"}))
		})
	})

	t.Run("gate", func(t *testing.T) {
		gate := func(t *testing.T, candidate *shadowFetcherStub, dropped float64) {
			t.Helper()
			primary := map[string]ConfigT{"ws-1": {UpdatedAt: t0}}
			fetcher, store := newShadowFetcherForTest(t, &shadowFetcherStub{}, candidate)
			fetcher.runSample(context.Background(), primary)
			require.Equal(t, dropped, counterValue(store, "bcv2_shadow_sample_dropped", nil))
			require.Equal(t, 1-dropped, counterValue(store, "bcv2_shadow_compared", nil))
		}

		t.Run("a workspace written between the fetches drops the sample", func(t *testing.T) {
			gate(t, &shadowFetcherStub{configs: map[string]ConfigT{"ws-1": {UpdatedAt: t0.Add(time.Second)}}}, 1)
		})
		t.Run("a definition written between the fetches drops the sample", func(t *testing.T) {
			gate(t, &shadowFetcherStub{
				configs: map[string]ConfigT{"ws-1": {UpdatedAt: t0}},
				defs:    t0.Add(time.Second),
			}, 1)
		})
		t.Run("unchanged state is compared", func(t *testing.T) {
			gate(t, &shadowFetcherStub{configs: map[string]ConfigT{"ws-1": {UpdatedAt: t0}}, defs: t0}, 0)
		})
	})

	t.Run("comparison", func(t *testing.T) {
		t.Run("counts membership instead of diffing it", func(t *testing.T) {
			store, comparer := newShadowComparerForTest(t, nil, nil)
			comparer.compare(context.Background(),
				map[string]ConfigT{"both": {}, "gone": {}},
				map[string]ConfigT{"both": {}, "new": {}},
			)
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_membership", stats.Tags{"side": "v1only"}))
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_membership", stats.Tags{"side": "v2only"}))
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_compared", nil))
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_matched", nil))
		})

		t.Run("byte level differences are not divergences", func(t *testing.T) {
			uploader := &shadowUploaderStub{}
			store, comparer := newShadowComparerForTest(t, nil, uploader)
			comparer.compare(context.Background(),
				map[string]ConfigT{"ws-1": {
					UpdatedAt: t0, // ignored: v1 moves it with the common config, v2 does not
					Sources:   []SourceT{{ID: "s1", Config: json.RawMessage(`{"a":1,"b":2}`)}},
				}},
				map[string]ConfigT{"ws-1": {
					Sources: []SourceT{{ID: "s1", Config: json.RawMessage(`{"b": 2, "a": 1}`)}},
				}},
			)
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_matched", nil))
			require.Empty(t, uploader.uploads)
		})

		t.Run("a divergence is counted by field and uploaded", func(t *testing.T) {
			uploader := &shadowUploaderStub{}
			store, comparer := newShadowComparerForTest(t, nil, uploader)
			comparer.compare(context.Background(),
				map[string]ConfigT{
					"same":     {Libraries: LibrariesT{{VersionID: "v1"}}},
					"diverged": {Sources: []SourceT{{ID: "s1", Name: "one"}}, Credentials: map[string]Credential{"k": {Key: "k"}}},
				},
				map[string]ConfigT{
					"same":     {Libraries: LibrariesT{{VersionID: "v1"}}},
					"diverged": {Sources: []SourceT{{ID: "s1", Name: "two"}}},
				},
			)
			require.Equal(t, 2.0, counterValue(store, "bcv2_shadow_compared", nil))
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_matched", nil))
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_diverged", stats.Tags{"field": "Sources"}))
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_diverged", stats.Tags{"field": "Credentials"}))

			require.Len(t, uploader.uploads, 1)
			var sample shadowSample
			reader, err := gzip.NewReader(bytes.NewReader(uploader.uploads[0]))
			require.NoError(t, err)
			payload, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, jsonrs.Unmarshal(payload, &sample))
			require.Equal(t, "test-namespace", sample.Namespace)
			require.Equal(t, []string{"diverged"}, sample.WorkspaceIDs)
			require.Contains(t, sample.Diff, `"one"`, "the rendered diff carries the v1 value")
			require.Contains(t, sample.Diff, `"two"`, "the rendered diff carries the v2 value")
			require.Contains(t, sample.V1, "diverged")
			require.Contains(t, sample.V2, "diverged")
			require.Equal(t, "one", sample.V1["diverged"].Sources[0].Name, "v1 is the primary's config")
			require.Equal(t, "two", sample.V2["diverged"].Sources[0].Name, "v2 is the candidate's config")
			require.NotContains(t, sample.V1, "same", "only the diverging workspaces are uploaded")
			require.NotContains(t, sample.V2, "same")

			// the key is what a bucket lifecycle rule expires on, and what keeps two pods
			// sampling the same namespace off each other's objects
			require.Regexp(t,
				`^bcv2-shadow-samples/\d{4}/\d{2}/\d{2}/test-namespace/\d{6}-test-instance\.json\.gz$`,
				uploader.names[0])
		})

		t.Run("without an uploader a divergence is still counted", func(t *testing.T) {
			store, comparer := newShadowComparerForTest(t, nil, nil)
			comparer.compare(context.Background(),
				map[string]ConfigT{"ws-1": {Sources: []SourceT{{ID: "s1"}}}},
				map[string]ConfigT{"ws-1": {}},
			)
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_diverged", stats.Tags{"field": "Sources"}))
		})

		t.Run("uploads stop at the budget", func(t *testing.T) {
			conf := config.New()
			conf.Set("BackendConfigShadow.maxUploads", 1)
			uploader := &shadowUploaderStub{}
			_, comparer := newShadowComparerForTest(t, conf, uploader)
			diverging := map[string]ConfigT{"ws-1": {Sources: []SourceT{{ID: "s1"}}}}
			comparer.compare(context.Background(), diverging, map[string]ConfigT{"ws-1": {}})
			comparer.compare(context.Background(), diverging, map[string]ConfigT{"ws-1": {}})
			require.Len(t, uploader.uploads, 1)
		})

		t.Run("a failed upload refunds the budget", func(t *testing.T) {
			conf := config.New()
			conf.Set("BackendConfigShadow.maxUploads", 1)
			uploader := &shadowUploaderStub{err: errors.New("boom")}
			store, comparer := newShadowComparerForTest(t, conf, uploader)
			diverging := map[string]ConfigT{"ws-1": {Sources: []SourceT{{ID: "s1"}}}}
			comparer.compare(context.Background(), diverging, map[string]ConfigT{"ws-1": {}})
			require.Equal(t, 1.0, counterValue(store, "bcv2_shadow_error", stats.Tags{"reason": "upload"}))

			uploader.err = nil // the failure must not have consumed the only slot
			comparer.compare(context.Background(), diverging, map[string]ConfigT{"ws-1": {}})
			require.Len(t, uploader.uploads, 1)
		})
	})

	t.Run("shadowNormalize leaves the live config untouched", func(t *testing.T) {
		live := ConfigT{
			Sources: []SourceT{
				{ID: "s1", SourceDefinition: SourceDefinitionT{Category: "cloud"}, Config: json.RawMessage(`{"origin":"profiles-table"}`)},
				{ID: "s2", SourceDefinition: SourceDefinitionT{Category: "webhook"}, Config: json.RawMessage(`{"a":1}`)},
			},
			Connections: map[string]Connection{
				"c1": {SourceID: "s1", Config: map[string]any{"table": "t"}},
			},
		}

		normalized := shadowNormalize(live)
		require.Nil(t, normalized.Sources[0].Config, "unported category: blanked")
		require.NotNil(t, normalized.Sources[1].Config, "ported category: kept")
		require.Nil(t, normalized.Connections["c1"].Config, "profiles-table connection: blanked")

		require.NotNil(t, live.Sources[0].Config)
		require.NotNil(t, live.Connections["c1"].Config)
	})
}

// shadowFetcherStub is both a primary and a candidate: a candidate that blocks does so on release.
type shadowFetcherStub struct {
	configs map[string]ConfigT
	err     error
	defs    time.Time
	calls   atomic.Int64
	release chan struct{}
}

func (f *shadowFetcherStub) Get(context.Context) (map[string]ConfigT, error) {
	f.calls.Add(1)
	if f.release != nil {
		<-f.release
	}
	return f.configs, f.err
}

func (f *shadowFetcherStub) definitionsUpdatedAt() time.Time { return f.defs }

type panickingFetcher struct{}

func (*panickingFetcher) Get(context.Context) (map[string]ConfigT, error) { panic("boom") }
func (*panickingFetcher) definitionsUpdatedAt() time.Time                 { return time.Time{} }

type shadowUploaderStub struct {
	err     error
	uploads [][]byte
	names   []string
}

func (u *shadowUploaderStub) UploadReader(_ context.Context, objName string, rdr io.Reader) (filemanager.UploadedFile, error) {
	if u.err != nil {
		return filemanager.UploadedFile{}, u.err
	}
	payload, err := io.ReadAll(rdr)
	if err != nil {
		return filemanager.UploadedFile{}, err
	}
	u.uploads = append(u.uploads, payload)
	u.names = append(u.names, objName)
	return filemanager.UploadedFile{Location: "location", ObjectName: objName}, nil
}

func newShadowFetcherForTest(t *testing.T, primary configFetcher, candidate shadowCandidate) (*shadowConfigFetcher, *memstats.Store) {
	t.Helper()
	store, comparer := newShadowComparerForTest(t, nil, nil)
	return &shadowConfigFetcher{logger: logger.NOP, primary: primary, candidate: candidate, comparer: comparer}, store
}

func newShadowComparerForTest(t *testing.T, conf *config.Config, uploader shadowUploader) (*memstats.Store, *shadowComparer) {
	t.Helper()
	store, err := memstats.New()
	require.NoError(t, err)
	if conf == nil {
		conf = config.New()
	}
	conf.Set("INSTANCE_ID", "test-instance")
	return store, newShadowComparer(conf, logger.NOP, store, "test-namespace", uploader)
}

// awaitSample waits for the in-flight sample, if any, to finish.
func awaitSample(t *testing.T, fetcher *shadowConfigFetcher) {
	t.Helper()
	require.Eventually(t, func() bool { return !fetcher.sampling.Load() },
		time.Second, time.Millisecond)
}

func counterValue(store *memstats.Store, name string, tags stats.Tags) float64 {
	metric := store.Get(name, tags)
	if metric == nil {
		return 0
	}
	return metric.LastValue()
}
