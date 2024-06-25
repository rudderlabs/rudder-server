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
	"net/url"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	"github.com/rudderlabs/rudder-schemas/go/stream"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/gateway/throttler"
	"github.com/rudderlabs/rudder-server/jobsdb"
	sourcedebugger "github.com/rudderlabs/rudder-server/services/debugger/source"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"

	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
)

type testSetup struct {
	context testContext
	cases   []testCase
}

type testContext struct {
	Now       time.Time
	RequestIP string `json:"request_ip"`
}

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
	Method   string
	RawQuery string `json:"query"`
	Headers  map[string]string
	Body     json.RawMessage
}

type testCaseOutput struct {
	Response testCaseResponse
	Queue    []json.RawMessage
	ErrQueue []json.RawMessage `json:"err_queue"`
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
		transformerContainer *transformertest.Resource
	)

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		p, err = postgres.Setup(pool, t)
		if err != nil {
			return fmt.Errorf("starting postgres: %w", err)
		}
		return nil
	})
	g.Go(func() (err error) {
		transformerContainer, err = transformertest.Setup(pool, t)
		if err != nil {
			return fmt.Errorf("starting transformer: %w", err)
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
		TransformerURL:           transformerContainer.TransformerURL,
		FeaturesRetryMaxAttempts: 10,
	})
	t.Setenv("DEST_TRANSFORM_URL", transformerContainer.TransformerURL)

	<-transformerFeaturesService.Wait()

	bcs := make(map[string]backendconfig.ConfigT)

	testSetup := loadTestSetup(t)

	for i, tc := range testSetup.cases {
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
		testSetup.cases[i].config = sConfig
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
		streamMsgValidator,
		gateway.WithNow(func() time.Time {
			return testSetup.context.Now
		}))
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

	for _, tc := range testSetup.cases {
		writeKey := tc.config.WriteKey
		sourceID := tc.config.ID
		workspaceID := tc.config.WorkspaceID

		t.Run(tc.Description, func(t *testing.T) {
			t.Logf("writeKey: %s", writeKey)
			t.Logf("sourceID: %s", sourceID)
			t.Logf("workspaceID: %s", workspaceID)

			t.Log("Request Body:", string(tc.Input.Request.Body))

			query, err := url.ParseQuery(tc.Input.Request.RawQuery)
			require.NoError(t, err)
			query.Set("writeKey", writeKey)

			t.Log("Request URL:", fmt.Sprintf("%s/v1/webhook?%s", gwURL, query.Encode()))
			req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/webhook?%s", gwURL, query.Encode()), bytes.NewBuffer(tc.Input.Request.Body))
			require.NoError(t, err)

			req.Header.Set("X-Forwarded-For", testSetup.context.RequestIP)
			for k, v := range tc.Input.Request.Headers {
				req.Header.Set(k, v)
			}

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

			assert.Len(t, r.Jobs, len(tc.Output.Queue), "enqueued items mismatch")
			for i, p := range tc.Output.Queue {
				var batch struct {
					Batch []json.RawMessage `json:"batch"`
				}
				err := json.Unmarshal(r.Jobs[i].EventPayload, &batch)
				require.NoError(t, err)
				assert.Len(t, batch.Batch, 1)

				if gjson.GetBytes(p, "messageId").String() == uuid.Nil.String() {
					rawMsgID := gjson.GetBytes(batch.Batch[0], "messageId").String()
					msgID, err := uuid.Parse(rawMsgID)
					assert.NoErrorf(t, err, "messageId (%q) is not a valid UUID", rawMsgID)

					p, err = sjson.SetBytes(p, "messageId", msgID.String())
					require.NoError(t, err)
				}

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
				errPayload, err := json.Marshal(struct {
					Event  json.RawMessage       `json:"event"`
					Source backendconfig.SourceT `json:"source"`
				}{
					Source: tc.config,
					Event:  bytes.ReplaceAll(p, []byte(`{{.WriteKey}}`), []byte(tc.config.WriteKey)),
				})
				require.NoError(t, err)
				errPayload, err = sjson.SetBytes(errPayload, "source.Destinations", nil)
				require.NoError(t, err)

				assert.JSONEq(t, string(errPayload), string(r.Jobs[i].EventPayload))
			}
		})

	}

	cancel()
	err = g.Wait()
	require.NoError(t, err)
}

//go:embed testdata/context.json
var contextData []byte

//go:embed testdata/testcases/**/*.json
var testdata embed.FS

func loadTestSetup(t *testing.T) testSetup {
	t.Helper()

	var tc testContext
	err := json.Unmarshal(contextData, &tc)
	require.NoError(t, err)

	var tcs []testCase
	err = fs.WalkDir(testdata, ".", func(path string, d fs.DirEntry, err error) error {
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

	return testSetup{
		context: tc,
		cases:   tcs,
	}
}
