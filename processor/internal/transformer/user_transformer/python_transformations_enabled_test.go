package user_transformer_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/user_transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

// TestPythonTransformationsEnabled covers Processor.UserTransformer.pythonTransformationsEnabled: when it is false a
// Python transformation is aborted (never routed, never panicking), whatever perWorkspacePyTEnabled says.
func TestPythonTransformationsEnabled(t *testing.T) {
	const (
		workspaceID      = "ws-A1"
		transformationID = "test-transformation"
		destinationID    = "dest-1"
		flagKey          = "Processor.UserTransformer.pythonTransformationsEnabled"
		perWSKey         = "Processor.UserTransformer.perWorkspacePyTEnabled"
	)

	type server struct {
		srv  *httptest.Server
		hits atomic.Int32
	}
	// newServer echoes every event back as a success, so a request that is sent anyway is visible in the response.
	newServer := func(t *testing.T) *server {
		s := &server{}
		s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.hits.Add(1)
			var reqBody []types.TransformerEvent
			require.NoError(t, jsonrs.NewDecoder(r.Body).Decode(&reqBody))
			responses := make([]types.TransformerResponse, len(reqBody))
			for i := range reqBody {
				responses[i] = types.TransformerResponse{
					Output: reqBody[i].Message, Metadata: reqBody[i].Metadata, StatusCode: http.StatusOK,
				}
			}
			w.Header().Set("apiVersion", strconv.Itoa(reportingtypes.SupportedTransformerApiVersion))
			require.NoError(t, jsonrs.NewEncoder(w).Encode(responses))
		}))
		t.Cleanup(s.srv.Close)
		return s
	}

	makeEvents := func(language, wsID string, n int) []types.TransformerEvent {
		events := make([]types.TransformerEvent, n)
		for i := range events {
			id := "messageID-" + strconv.Itoa(i)
			events[i] = types.TransformerEvent{
				Metadata: types.Metadata{MessageID: id, WorkspaceID: wsID},
				Message:  map[string]any{"src-key-1": id},
				Destination: backendconfig.DestinationT{
					ID:                    destinationID,
					DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "test-destination"},
					Transformations: []backendconfig.TransformationT{{
						ID: transformationID, VersionID: "v-1", Language: language,
					}},
				},
			}
		}
		return events
	}

	type fixture struct {
		conf  *config.Config
		js    *server
		perWS *server
	}
	newFixture := func(t *testing.T) *fixture {
		f := &fixture{conf: config.New(), js: newServer(t), perWS: newServer(t)}
		f.conf.Set("Processor.UserTransformer.maxRetry", 1)
		f.conf.Set("USER_TRANSFORM_URL", f.js.srv.URL)
		f.conf.Set("Processor.UserTransformer.perWorkspacePyTURLTemplate", f.perWS.srv.URL+"/pyt-{workspaceID}")
		return f
	}
	requireNoHits := func(t *testing.T, f *fixture) {
		t.Helper()
		require.Zero(t, f.js.hits.Load(), "the JS transformer must not be called")
		require.Zero(t, f.perWS.hits.Load(), "the per-workspace PyT must not be called")
	}
	requireAborted := func(t *testing.T, events []types.TransformerEvent, rsp types.Response) {
		t.Helper()
		require.Empty(t, rsp.Events)
		require.Len(t, rsp.FailedEvents, len(events))
		for i, failed := range rsp.FailedEvents {
			require.Equal(t, http.StatusBadRequest, failed.StatusCode)
			require.Equal(t, user_transformer.PythonTransformationsDisabledError, failed.Error)
			require.Equal(t, events[i].Metadata, failed.Metadata, "the processor maps the failure back by metadata")
		}
	}

	t.Run("flag off aborts Python, whatever the routing flag says", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			perWSFlag   any // nil leaves perWorkspacePyTEnabled unset
			workspaceID string
		}{
			{name: "perWorkspacePyTEnabled false (hosted)", perWSFlag: false, workspaceID: workspaceID},
			{name: "perWorkspacePyTEnabled unset", perWSFlag: nil, workspaceID: workspaceID},
			{name: "perWorkspacePyTEnabled true", perWSFlag: true, workspaceID: workspaceID},
			{name: "empty workspaceID, perWorkspacePyTEnabled false", perWSFlag: false, workspaceID: ""},
			{name: "empty workspaceID, perWorkspacePyTEnabled true", perWSFlag: true, workspaceID: ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for _, language := range []string{"python", "pythonfaas", "pythonwithlibs"} {
					f := newFixture(t)
					f.conf.Set(flagKey, false)
					if tc.perWSFlag != nil {
						f.conf.Set(perWSKey, tc.perWSFlag)
					}
					tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
					events := makeEvents(language, tc.workspaceID, 3)
					var rsp types.Response
					require.NotPanics(t, func() { rsp = tr.Transform(context.Background(), events) }, language)
					requireAborted(t, events, rsp)
					requireNoHits(t, f)
				}
			})
		}
	})

	t.Run("flag off aborts every batch of a large Python call", func(t *testing.T) {
		f := newFixture(t)
		f.conf.Set(flagKey, false)
		f.conf.Set("Processor.UserTransformer.batchSize", 2)
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
		events := makeEvents("pythonfaas", workspaceID, 5)
		requireAborted(t, events, tr.Transform(context.Background(), events))
		requireNoHits(t, f)
	})

	t.Run("flag off logs the workspace, transformation and destination, and no payload", func(t *testing.T) {
		f := newFixture(t)
		f.conf.Set(flagKey, false)
		ctrl := gomock.NewController(t)
		log := mock_logger.NewMockLogger(ctrl)
		log.EXPECT().Child(gomock.Any()).Return(log).AnyTimes()
		var fields []logger.Field
		log.EXPECT().Errorn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(
			func(_ string, f ...logger.Field) { fields = f },
		).Times(1)

		tr := user_transformer.New(f.conf, log, stats.NOP)
		tr.Transform(context.Background(), makeEvents("python", workspaceID, 2))

		got := make(map[string]any, len(fields))
		for _, field := range fields {
			got[field.Name()] = field.Value()
		}
		require.Equal(t, map[string]any{
			"workspaceId":      workspaceID,
			"transformationId": transformationID,
			"destinationId":    destinationID,
			"events":           int64(2),
		}, got)
	})

	t.Run("flag off leaves JavaScript untouched", func(t *testing.T) {
		for _, perWS := range []bool{false, true} {
			f := newFixture(t)
			f.conf.Set(flagKey, false)
			f.conf.Set(perWSKey, perWS)
			tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
			// an unset language means javascript
			for _, language := range []string{"javascript", ""} {
				rsp := tr.Transform(context.Background(), makeEvents(language, workspaceID, 2))
				require.Empty(t, rsp.FailedEvents)
				require.Len(t, rsp.Events, 2)
			}
			require.EqualValues(t, 2, f.js.hits.Load())
			require.Zero(t, f.perWS.hits.Load())
		}
	})

	t.Run("flag on keeps the panic when per-workspace PyT is off", func(t *testing.T) {
		for _, explicit := range []bool{true, false} {
			f := newFixture(t)
			if explicit {
				f.conf.Set(flagKey, true)
			} // else: unset, which must behave as true
			tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
			require.PanicsWithValue(t, "python transformation but per-workspace PyT is disabled", func() {
				tr.Transform(context.Background(), makeEvents("pythonfaas", workspaceID, 1))
			})
			requireNoHits(t, f)
		}
	})

	t.Run("flag on keeps the panic on an empty workspaceID", func(t *testing.T) {
		f := newFixture(t)
		f.conf.Set(flagKey, true)
		f.conf.Set(perWSKey, true)
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
		require.PanicsWithValue(t, "per-workspace PyT enabled but workspaceID is empty", func() {
			tr.Transform(context.Background(), makeEvents("pythonfaas", "", 1))
		})
	})

	t.Run("flag on or unset routes Python to the per-workspace PyT", func(t *testing.T) {
		for _, explicit := range []bool{true, false} {
			f := newFixture(t)
			if explicit {
				f.conf.Set(flagKey, true)
			}
			f.conf.Set(perWSKey, true)
			tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
			rsp := tr.Transform(context.Background(), makeEvents("pythonfaas", workspaceID, 2))
			require.Empty(t, rsp.FailedEvents)
			require.Len(t, rsp.Events, 2)
			require.EqualValues(t, 1, f.perWS.hits.Load())
			require.Zero(t, f.js.hits.Load())
		}
	})

	t.Run("flipping the flag takes effect without a restart", func(t *testing.T) {
		f := newFixture(t)
		f.conf.Set(perWSKey, true)
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
		events := makeEvents("pythonfaas", workspaceID, 1)

		require.Len(t, tr.Transform(context.Background(), events).Events, 1)
		require.EqualValues(t, 1, f.perWS.hits.Load())

		f.conf.Set(flagKey, false)
		requireAborted(t, events, tr.Transform(context.Background(), events))
		require.EqualValues(t, 1, f.perWS.hits.Load(), "an aborted call must not reach the PyT")

		f.conf.Set(flagKey, true)
		require.Len(t, tr.Transform(context.Background(), events).Events, 1)
		require.EqualValues(t, 2, f.perWS.hits.Load())
	})
}
