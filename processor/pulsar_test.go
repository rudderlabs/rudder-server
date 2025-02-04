package processor

import (
	"bytes"
	"context"
	"testing"
	"text/template"
	"time"

	pc "github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/pulsar"
	"github.com/rudderlabs/rudder-schemas/go/stream"
)

func TestPulsar(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 1 * time.Minute
	network, err := pool.Client.CreateNetwork(docker.CreateNetworkOptions{Name: t.Name()})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Client.RemoveNetwork(network.ID); err != nil {
			t.Logf("Could not remove network %q: %s", network.Name, err)
		}
	})

	var (
		setupGroup      errgroup.Group
		pulsarClient    pc.Client
		pulsarContainer *pulsar.Resource
		topicName       = "pulsar-test"
	)
	setupGroup.Go(func() (err error) {
		pulsarContainer, err = pulsar.Setup(pool, t, pulsar.WithTag("3.3.4"), pulsar.WithNetwork(network))
		if err != nil {
			return err
		}
		pulsarClient, err = pc.NewClient(pc.ClientOptions{
			URL: pulsarContainer.URL,
		})
		t.Cleanup(pulsarClient.Close)
		return err
	})

	var producer pc.Producer
	require.Eventuallyf(t, func() bool {
		producer, err = pulsarClient.CreateProducer(pc.ProducerOptions{
			Topic:           topicName,
			DisableBatching: true,
		})
		return err == nil
	}, 3*time.Minute, time.Second, "Failed to create producer: %s", err)

	ctx := context.Background()
	identify1 := getIdentifyProducerMessage(t, "userId_1", "sourceId_1", "workspaceId_1")
	_, err = producer.Send(ctx, identify1)
	require.NoError(t, err)

	// TODO have the processor read these without the src-router batching
}

func getIdentifyProducerMessage(t testing.TB, userID, sourceID, workspaceID string) *pc.ProducerMessage {
	t.Helper()

	tmpl, err := template.New("identify").Funcs(templateFuncMap).Parse(identify)
	require.NoError(t, err)

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, map[string]any{
		"MessageID":         uuid.New().String(),
		"AnonymousID":       userID,
		"OriginalTimestamp": time.Now().Format(time.RFC3339),
		"SentAt":            time.Now().Format(time.RFC3339),
		"LoadRunID":         "1234",
	})
	require.NoError(t, err)

	properties := stream.MessageProperties{
		RequestType:     "identify",
		RoutingKey:      "anonymousId_header<<>>anonymousId_1<<>>" + userID,
		WorkspaceID:     workspaceID,
		UserID:          userID,
		SourceID:        sourceID,
		ReceivedAt:      time.Now(),
		DestinationID:   "destinationID",
		RequestIP:       "1.1.1.1",
		TraceID:         "traceID",
		SourceJobRunID:  "sourceJobRunID",
		SourceTaskRunID: "sourceTaskRunID",
	}
	return &pc.ProducerMessage{
		Key:        properties.RoutingKey,
		Payload:    buf.Bytes(),
		Properties: stream.ToMapProperties(properties),
	}
}

var templateFuncMap = template.FuncMap{
	"uuid":    func() string { return uuid.New().String() },
	"sub":     func(a, b int) int { return a - b },
	"nowNano": func() int64 { return time.Now().UnixNano() },
	"loop": func(n int) <-chan int {
		ch := make(chan int)
		go func() {
			for i := 0; i < n; i++ {
				ch <- i
			}
			close(ch)
		}()
		return ch
	},
}

var identify = `{
	"userId": "{{$.AnonymousID}}",
	"messageId": "{{uuid}}",
	"anonymousId": "{{$.AnonymousID}}",
	"type": "identify",
	"context": {
		"load_run_id": "{{$.LoadRunID}}",
		"traits": {
			"activation_api_experience": false
		},
		"sessionId": {{nowNano}},
		"app": {
			"name": "RudderLabs JavaScript SDK",
			"namespace": "com.rudderlabs.javascript",
			"version": "3.0.3"
		},
		"library": {
			"name": "RudderLabs JavaScript SDK",
			"version": "3.0.3"
		},
		"userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
		"os": {
			"name": "",
			"version": ""
		},
		"locale": "en-GB",
		"screen": {
			"width": 1728,
			"height": 1117,
			"density": 2,
			"innerWidth": 1210,
			"innerHeight": 992
		},
		"campaign": {},
		"page": {
			"path": "/request-demo/",
			"referrer": "https://www.rudderstack.com/",
			"referring_domain": "www.rudderstack.com",
			"search": "",
			"title": "Schedule a Quick Demo With RudderStack Team",
			"url": "https://www.rudderstack.com/request-demo/",
			"tab_url": "https://www.rudderstack.com/request-demo/",
			"initial_referrer": "https://www.google.com/",
			"initial_referring_domain": "www.google.com"
		},
		"timezone": "GMT+0100"
	},
	"channel": "web",
	"originalTimestamp": "{{$.OriginalTimestamp}}",
	"sentAt": "{{$.SentAt}}"
}`
