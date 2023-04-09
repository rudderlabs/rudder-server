package kafka_batching

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/ory/dockertest/v3"
	promClient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/stats/testhelper"
	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/app"
	kafkaClient "github.com/rudderlabs/rudder-server/services/streammanager/kafka/client"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka/client/testutil"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/testhelper/destination/kafka"
	thEtcd "github.com/rudderlabs/rudder-server/testhelper/etcd"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func TestKafkaBatching(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)

	var (
		group, containersCtx = errgroup.WithContext(ctx)

		kafkaContainer       *kafka.Resource
		postgresContainer    *resource.PostgresResource
		etcdContainer        *thEtcd.Resource
		transformerContainer *destination.TransformerResource

		serverInstanceID    = "1"
		workspaceNamespace  = "test-workspace-namespace"
		hostedServiceSecret = "service-secret"
		kafkaTopic          = "foo_bar_topic"
	)
	group.Go(func() error {
		kafkaContainer, err = kafka.Setup(pool, t,
			kafka.WithLogger(&testLogger{t}),
			kafka.WithBrokers(1),
		)
		if err != nil {
			return err
		}
		kafkaCtx, kafkaCancel := context.WithTimeout(containersCtx, 3*time.Minute)
		defer kafkaCancel()
		return waitForKafka(kafkaCtx, t, kafkaTopic, kafkaContainer.Ports[0])
	})
	group.Go(func() (err error) {
		postgresContainer, err = resource.SetupPostgres(pool, t)
		return err
	})
	group.Go(func() (err error) {
		etcdContainer, err = thEtcd.Setup(pool, t)
		return err
	})
	group.Go(func() (err error) {
		transformerContainer, err = destination.SetupTransformer(pool, t)
		return err
	})
	require.NoError(t, group.Wait())

	writeKey := rand.String(27)
	workspaceID := rand.String(27)
	marshalledWorkspaces := th.FillTemplateAndReturn(t, "testdata/backend_config.json", map[string]string{
		"writeKey":    writeKey,
		"workspaceId": workspaceID,
		"kafkaPort":   kafkaContainer.Ports[0],
		"kafkaTopic":  kafkaTopic,
	})
	require.NoError(t, err)

	backendConfRouter := chi.NewRouter()
	if testing.Verbose() {
		backendConfRouter.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("BackendConfig server call: %+v", r)
				next.ServeHTTP(w, r)
			})
		})
	}

	backendConfRouter.
		Get("/data-plane/v1/namespaces/"+workspaceNamespace+"/config", requireAuth(t, hostedServiceSecret,
			func(w http.ResponseWriter, r *http.Request) {
				n, err := w.Write(marshalledWorkspaces.Bytes())
				require.NoError(t, err)
				require.Equal(t, marshalledWorkspaces.Len(), n)
			},
		))
	backendConfRouter.
		Post("/data-plane/v1/namespaces/"+workspaceNamespace+"/settings", requireAuth(t, hostedServiceSecret,
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			},
		))

	backendConfRouter.NotFound(func(w http.ResponseWriter, r *http.Request) {
		require.FailNowf(t, "backend config", "unexpected request to backend config, not found: %+v", r.URL)
		w.WriteHeader(http.StatusNotFound)
	})

	backendConfigSrv := httptest.NewServer(backendConfRouter)
	t.Logf("BackendConfig server listening on: %s", backendConfigSrv.URL)
	t.Cleanup(backendConfigSrv.Close)

	httpPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	debugPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)
	prometheusPort, err := kitHelper.GetFreePort()
	require.NoError(t, err)

	rudderTmpDir, err := os.MkdirTemp("", "rudder_server_*_test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(rudderTmpDir) })

	var (
		done        = make(chan struct{})
		releaseName = t.Name() + "_" + app.EMBEDDED
	)
	go func() {
		defer close(done)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", "run", "../../main.go")
		cmd.Env = append(os.Environ(),
			"APP_TYPE="+app.EMBEDDED,
			"INSTANCE_ID="+serverInstanceID,
			"RELEASE_NAME="+releaseName,
			"ETCD_HOSTS="+etcdContainer.Hosts[0],
			"JOBS_DB_PORT="+postgresContainer.Port,
			"JOBS_DB_USER="+postgresContainer.User,
			"JOBS_DB_DB_NAME="+postgresContainer.Database,
			"JOBS_DB_PASSWORD="+postgresContainer.Password,
			"CONFIG_BACKEND_URL="+backendConfigSrv.URL,
			"RSERVER_ROUTER_KAFKA_ENABLE_BATCHING=true",
			"RSERVER_GATEWAY_WEB_PORT="+strconv.Itoa(httpPort),
			"RSERVER_GATEWAY_ADMIN_WEB_PORT="+strconv.Itoa(httpAdminPort),
			"RSERVER_PROFILER_PORT="+strconv.Itoa(debugPort),
			"RSERVER_ENABLE_STATS=true",
			"RSERVER_RUNTIME_STATS_ENABLED=false",
			"RSERVER_OPEN_TELEMETRY_ENABLED=true",
			"RSERVER_OPEN_TELEMETRY_METRICS_PROMETHEUS_ENABLED=true",
			"RSERVER_OPEN_TELEMETRY_METRICS_PROMETHEUS_PORT="+strconv.Itoa(prometheusPort),
			"RSERVER_OPEN_TELEMETRY_METRICS_EXPORT_INTERVAL=10ms",
			"RSERVER_BACKEND_CONFIG_USE_HOSTED_BACKEND_CONFIG=false",
			"RUDDER_TMPDIR="+rudderTmpDir,
			"DEPLOYMENT_TYPE="+string(deployment.MultiTenantType),
			"DEST_TRANSFORM_URL="+transformerContainer.TransformURL,
			"HOSTED_SERVICE_SECRET="+hostedServiceSecret,
			"WORKSPACE_NAMESPACE="+workspaceNamespace,
			"RSERVER_WAREHOUSE_MODE=off",
		)
		if testing.Verbose() {
			cmd.Env = append(cmd.Env, "LOG_LEVEL=debug")
		}

		stdout, err := cmd.StdoutPipe()
		require.NoError(t, err)
		stderr, err := cmd.StderrPipe()
		require.NoError(t, err)

		defer func() {
			_ = stdout.Close()
			_ = stderr.Close()
		}()
		require.NoError(t, cmd.Start())
		if testing.Verbose() {
			go func() { _, _ = io.Copy(os.Stdout, stdout) }()
			go func() { _, _ = io.Copy(os.Stderr, stderr) }()
		}

		if err = cmd.Wait(); err != nil {
			if err.Error() != "signal: killed" {
				t.Errorf("Error running main.go: %v", err)
				return
			}
		}
		t.Log("main.go exited")
	}()
	t.Cleanup(func() { cancel(); <-done })

	// Waiting for RS to become healthy
	var (
		resp           *http.Response
		respBody       string
		statusCode     int
		healthEndpoint = fmt.Sprintf("http://localhost:%d/health", httpPort)
	)
	require.Eventuallyf(t, func() bool {
		resp, err = http.Get(healthEndpoint)
		if err != nil {
			return false
		}
		defer func() { httputil.CloseResponse(resp) }()

		var buf []byte
		buf, err = io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		respBody = string(buf)
		if statusCode = resp.StatusCode; statusCode != http.StatusOK {
			return false
		}
		return true
	}, time.Minute, 100*time.Millisecond, "RS did not become healthy in time (%s)[%d]: %v", respBody, statusCode, err)

	noOfMessages := 10
	for i := 0; i < noOfMessages; i++ {
		sendEventsToGateway(t, httpPort, writeKey, fmt.Sprintf("msg_%d", i))
	}

	c, err := kafkaClient.New("tcp", []string{"localhost:" + kafkaContainer.Ports[0]}, kafkaClient.Config{
		ClientID: t.Name(),
	})
	require.NoError(t, err)
	require.NoError(t, c.Ping(ctx))

	consumer := c.NewConsumer(kafkaTopic, kafkaClient.ConsumerConfig{
		Logger:      &testLogger{t},
		ErrorLogger: &testLogger{t},
	})

	var (
		receivedMessages          []kafkaClient.Message
		receiveCtx, receiveCancel = context.WithTimeout(ctx, 30*time.Second)
	)
	t.Cleanup(receiveCancel)
	for {
		msg, err := consumer.Receive(receiveCtx)
		if receiveCtx.Err() != nil {
			break
		}

		require.NoError(t, err)
		receivedMessages = append(receivedMessages, msg)
		if len(receivedMessages) == noOfMessages {
			break
		}
	}

	require.Len(t, receivedMessages, noOfMessages)
	for i := 0; i < noOfMessages; i++ {
		// assertion for order of messages
		var m map[string]interface{}
		require.NoError(t, json.Unmarshal(receivedMessages[i].Value, &m))
		require.Equal(t, fmt.Sprintf("msg_%d", i), m["messageId"])
	}

	var buf []byte
	require.Eventuallyf(t, func() bool {
		resp, err = http.Get(fmt.Sprintf("http://localhost:%d/metrics", prometheusPort))
		require.NoError(t, err)
		defer func() { httputil.CloseResponse(resp) }()

		buf, err = io.ReadAll(resp.Body)
		require.NoError(t, err)

		return strings.Contains(string(buf), "router_kafka_batch_size") &&
			strings.Contains(string(buf), "router_batch_num_input_jobs") &&
			strings.Contains(string(buf), "router_batch_num_output_jobs")
	}, time.Minute, 100*time.Millisecond, "Cannot find metrics in time: %s", buf)

	metrics, err := testhelper.ParsePrometheusMetrics(bytes.NewBuffer(buf))
	require.NoError(t, err)

	requireHistogramEqual(t, metrics["router_kafka_batch_size"], histogram{
		name: "router_kafka_batch_size", count: 1, sum: 10,
		buckets: []*promClient.Bucket{
			{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(0.005)},
			{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(0.01)},
			{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(0.025)},
			{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(0.05)},
			{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(0.1)},
			{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(0.25)},
			{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(0.5)},
			{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(1.0)},
			{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(2.5)},
			{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(5.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(10.0)}, // 10 is the number of messages we sent
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(60.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(300.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(600.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(1800.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(10800.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(36000.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(86400.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(259200.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(604800.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(1209600.0)},
			{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(math.Inf(1))},
		},
		labels: []*promClient.LabelPair{
			{Name: ptr("job"), Value: ptr(app.EMBEDDED)},
			{Name: ptr("instance"), Value: &serverInstanceID},
		},
	})

	require.EqualValues(t, ptr("router_batch_num_input_jobs"), metrics["router_batch_num_input_jobs"].Name)
	require.EqualValues(t, ptr(promClient.MetricType_COUNTER), metrics["router_batch_num_input_jobs"].Type)
	require.Len(t, metrics["router_batch_num_input_jobs"].Metric, 1)
	require.EqualValues(t, &promClient.Counter{Value: ptr(10.0)}, metrics["router_batch_num_input_jobs"].Metric[0].Counter)
	require.ElementsMatch(t, []*promClient.LabelPair{
		{Name: ptr("destType"), Value: ptr("KAFKA")},
		{Name: ptr("job"), Value: ptr(app.EMBEDDED)},
		{Name: ptr("instance"), Value: &serverInstanceID},
	}, metrics["router_batch_num_input_jobs"].Metric[0].Label)

	require.EqualValues(t, ptr("router_batch_num_output_jobs"), metrics["router_batch_num_output_jobs"].Name)
	require.EqualValues(t, ptr(promClient.MetricType_COUNTER), metrics["router_batch_num_output_jobs"].Type)
	require.Len(t, metrics["router_batch_num_output_jobs"].Metric, 1)
	require.EqualValues(t, &promClient.Counter{Value: ptr(1.0)}, metrics["router_batch_num_output_jobs"].Metric[0].Counter)
	require.ElementsMatch(t, []*promClient.LabelPair{
		{Name: ptr("destType"), Value: ptr("KAFKA")},
		{Name: ptr("job"), Value: ptr(app.EMBEDDED)},
		{Name: ptr("instance"), Value: &serverInstanceID},
	}, metrics["router_batch_num_output_jobs"].Metric[0].Label)
}

func requireHistogramEqual(t *testing.T, mf *promClient.MetricFamily, h histogram) {
	t.Helper()
	require.EqualValues(t, &h.name, mf.Name)
	require.EqualValues(t, ptr(promClient.MetricType_HISTOGRAM), mf.Type)
	require.Len(t, mf.Metric, 1)
	require.EqualValuesf(t, &h.count, mf.Metric[0].Histogram.SampleCount,
		"Got %d, expected %d", *mf.Metric[0].Histogram.SampleCount, h.count,
	)
	require.EqualValuesf(t, &h.sum, mf.Metric[0].Histogram.SampleSum,
		"Got %.2f, expected %.2f", *mf.Metric[0].Histogram.SampleSum, h.sum,
	)
	require.ElementsMatchf(t, h.buckets, mf.Metric[0].Histogram.Bucket, "Buckets for %q do not match", h.name)
	require.ElementsMatch(t, h.labels, mf.Metric[0].Label)
}

func requireAuth(t *testing.T, secret string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, _, ok := r.BasicAuth()
		require.True(t, ok, "Auth should be present")
		require.Equalf(t, secret, u,
			"Expected HTTP basic authentication to be %q, got %q instead",
			secret, u)

		handler(w, r)
	}
}

func sendEventsToGateway(t testing.TB, httpPort int, writeKey, msgID string) {
	payload1 := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"` + msgID + `",
		"type": "identify",
		"context": {
		  "traits": {
			 "trait1": "new-val"
		  },
		  "ip": "14.5.67.21",
		  "library": {
			  "name": "http"
		  }
		},
		"timestamp": "2020-02-02T00:23:09.544Z"
	}`)
	sendEvent(t, httpPort, payload1, "identify", writeKey)
}

func sendEvent(t testing.TB, httpPort int, payload *strings.Reader, callType, writeKey string) {
	t.Helper()
	t.Logf("Sending %s Event", callType)

	var (
		httpClient = &http.Client{}
		method     = "POST"
		url        = fmt.Sprintf("http://localhost:%d/v1/%s", httpPort, callType)
	)

	req, err := http.NewRequest(method, url, payload)
	require.NoError(t, err)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", b64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("%s:", writeKey)),
	)))
	req.Header.Add("AnonymousId", "anonymousId_header")

	res, err := httpClient.Do(req)
	require.NoError(t, err)
	defer func() { httputil.CloseResponse(res) }()

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	t.Logf("Event Sent Successfully: (%s)", body)
}

func waitForKafka(ctx context.Context, t testing.TB, topic, port string) (err error) {
	tc := testutil.New("tcp", "localhost:"+port)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafka not ready within context (%v): %v", ctx.Err(), err)
		case <-time.After(50 * time.Millisecond):
			var topics []testutil.TopicPartition
			topics, err = tc.ListTopics(ctx)
			if err != nil {
				t.Log(fmt.Errorf("could not list Kafka topics: %v", err))
				continue
			}

			var found bool
			for _, top := range topics {
				if top.Topic == topic {
					found = true
					break
				}
			}
			if found {
				t.Log("Kafka is ready!")
				return nil
			}

			if err = tc.CreateTopic(ctx, topic, 1, 1); err != nil {
				t.Log(fmt.Errorf("could not create Kafka topic %q: %v", topic, err))
				continue
			}
		}
	}
}

func ptr[T any](v T) *T {
	return &v
}

type histogram struct {
	name    string
	count   uint64
	sum     float64
	buckets []*promClient.Bucket
	labels  []*promClient.LabelPair
}

type testLogger struct{ testing.TB }

func (t *testLogger) Log(args ...interface{}) { t.Helper(); t.TB.Log(args...) }
func (t *testLogger) Printf(format string, args ...interface{}) {
	t.Helper()
	t.TB.Logf(format, args...)
}
