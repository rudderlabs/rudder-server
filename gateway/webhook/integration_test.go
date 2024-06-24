package webhook_test

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-schemas/go/stream"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/gateway/throttler"
	"github.com/rudderlabs/rudder-server/jobsdb"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/destination"

	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
)

type testCase struct {
	Name        string
	Description string
	Input       testCaseInput
	Output      testCaseOutput

	config backendconfig.SourceT
}

type testCaseInput struct {
	Request testCaseRequest
}
type testCaseRequest struct {
	Body    json.RawMessage
	Method  string
	Headers map[string]string
}

type testCaseOutput struct {
	Response testCaseResponse
	Queue    []json.RawMessage
	ErrQueue []json.RawMessage
}

type testCaseResponse struct {
	Body       json.RawMessage
	StatusCode int `json:"status"`
}

func TestIntegrationWebhook(t *testing.T) {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		p                    *postgres.Resource
		transformerContainer *destination.TransformerResource
	)

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		p, err = postgres.Setup(pool, t)
		if err != nil {
			return fmt.Errorf("could not start postgres: %v", err)
		}
		return nil
	})
	g.Go(func() (err error) {
		transformerContainer, err = destination.SetupTransformer(pool, t)
		if err != nil {
			return fmt.Errorf("could not start transformer: %v", err)
		}
		return nil
	})
	err = g.Wait()
	require.NoError(t, err)

	var gw gateway.Handle

	conf := config.New()
	logger := logger.NOP
	stat, err := memstats.New()
	require.NoError(t, err)

	conf.Set("Gateway.enableSuppressUserFeature", false)

	gatewayDB := jobsdb.NewForReadWrite(
		"gateway",
		jobsdb.WithDBHandle(p.DB),
	)
	require.NoError(t, gatewayDB.Start())
	defer gatewayDB.TearDown()

	errDB := jobsdb.NewForReadWrite(
		"err",
		jobsdb.WithDBHandle(p.DB),
	)
	require.NoError(t, errDB.Start())
	defer errDB.TearDown()

	var (
		rateLimiter        throttler.Throttler
		versionHandler     func(w http.ResponseWriter, r *http.Request)
		streamMsgValidator func(message *stream.Message) error
		application        app.App
	)

	transformerFeaturesService := transformer.NewFeaturesService(ctx, conf, transformer.FeaturesServiceOptions{
		PollInterval:             config.GetDuration("Transformer.pollInterval", 10, time.Second),
		TransformerURL:           transformerContainer.TransformURL,
		FeaturesRetryMaxAttempts: 10,
	})
	t.Setenv("DEST_TRANSFORM_URL", transformerContainer.TransformURL)

	<-transformerFeaturesService.Wait()

	bcs := make(map[string]backendconfig.ConfigT)

	tcs := loadTestCases(t)

	for i, tc := range tcs {
		sConfig := backendconfigtest.NewSourceBuilder().
			WithSourceType(strings.ToUpper(tc.Name)).
			WithSourceCategory("webhook").
			WithConnection(
				backendconfigtest.NewDestinationBuilder("WEBHOOK").Build(),
			).
			Build()

		bc := backendconfigtest.NewConfigBuilder().WithSource(
			sConfig,
		).Build()

		// fix this in backendconfigtest
		bc.Sources[0].WorkspaceID = bc.WorkspaceID
		sConfig.WorkspaceID = bc.WorkspaceID
		bcs[bc.WorkspaceID] = bc
		tcs[i].config = sConfig
	}
	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	conf.Set("Gateway.webPort", httpPort)

	err = gw.Setup(ctx,
		conf, logger, stat,
		application,
		backendconfigtest.NewStaticLibrary(bcs),
		gatewayDB, errDB,
		rateLimiter, versionHandler, rsources.NewNoOpService(), transformerFeaturesService, sourcedebugger.NewNoOpService(),
		streamMsgValidator)
	require.NoError(t, err)
	g.Go(func() error {
		return gw.StartWebHandler(ctx)
	})
	defer func() {
		if err := gw.Shutdown(); err != nil {
			require.NoError(t, err)
		}
	}()

	gwURL := fmt.Sprintf("http://localhost:%d", httpPort)

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("%s/health", gwURL))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, time.Millisecond*500, time.Millisecond)

	for _, tc := range tcs {
		writeKey := tc.config.WriteKey
		sourceID := tc.config.ID
		workspaceID := tc.config.WorkspaceID

		t.Run(tc.Description, func(t *testing.T) {
			t.Logf("writeKey: %s", writeKey)
			t.Logf("sourceID: %s", sourceID)
			t.Logf("workspaceID: %s", workspaceID)

			t.Log("Request Body:", string(tc.Input.Request.Body))

			req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/webhook?writeKey=%s", gwURL, writeKey), bytes.NewBuffer(tc.Input.Request.Body))
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, tc.Output.Response.StatusCode, resp.StatusCode)
			if resp.Header.Get("Content-Type") == "application/json" {
				assert.JSONEq(t, string(tc.Output.Response.Body), string(b))
			} else {
				assert.Equal(t, string(tc.Output.Response.Body), fmt.Sprintf("%q", b))
			}
			r, err := gatewayDB.GetUnprocessed(ctx, jobsdb.GetQueryParams{
				WorkspaceID: workspaceID,
				ParameterFilters: []jobsdb.ParameterFilterT{{
					Name:  "source_id",
					Value: sourceID,
				}},
				JobsLimit: 10,
			})
			require.NoError(t, err)

			assert.Len(t, r.Jobs, len(tc.Output.Queue))
			for i, p := range tc.Output.Queue {
				var batch struct {
					Batch []json.RawMessage `json:"batch"`
				}
				err := json.Unmarshal(r.Jobs[i].EventPayload, &batch)
				require.NoError(t, err)
				assert.Len(t, batch.Batch, 1)
				assert.JSONEq(t, string(p), string(batch.Batch[0]))
			}

			r, err = errDB.GetUnprocessed(ctx, jobsdb.GetQueryParams{
				WorkspaceID: workspaceID,
				// ParameterFilters: []jobsdb.ParameterFilterT{{
				// 	Name:  "source_id",
				// 	Value: sourceID,
				// }},
				JobsLimit: 1,
			})
			require.NoError(t, err)
			assert.Len(t, r.Jobs, len(tc.Output.ErrQueue))
			for i, p := range tc.Output.ErrQueue {
				assert.JSONEq(t, string(p), string(r.Jobs[i].EventPayload))
			}
		})

	}

	cancel()
	err = g.Wait()
	require.NoError(t, err)
}

//go:embed testdata/**/*.json
var testdata embed.FS

func loadTestCases(t *testing.T) []testCase {
	t.Helper()

	var tcs []testCase
	err := fs.WalkDir(testdata, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		f, err := testdata.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		var tc testCase
		err = json.NewDecoder(f).Decode(&tc)
		if err != nil {
			return err
		}
		tcs = append(tcs, tc)

		return nil
	})
	require.NoError(t, err)

	return tcs
}
