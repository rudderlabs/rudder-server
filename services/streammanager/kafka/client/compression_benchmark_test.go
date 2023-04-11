package client

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka/client/testutil"
	"github.com/rudderlabs/rudder-server/testhelper/destination/kafka"
	"github.com/rudderlabs/rudder-server/utils/tcpproxy"
)

func BenchmarkCompression(b *testing.B) {
	proxyPort, err := testhelper.GetFreePort()
	require.NoError(b, err)

	var (
		ctx       = context.Background()
		topic     = "foo_bar_topic"
		proxyHost = "localhost:" + strconv.Itoa(proxyPort)
	)

	setupKafka := func(b *testing.B) string {
		pool, err := dockertest.NewPool("")
		require.NoError(b, err)

		kafkaContainer, err := kafka.Setup(pool, b, kafka.WithCustomAdvertisedListener(proxyHost))
		require.NoError(b, err)

		return "localhost:" + kafkaContainer.Ports[0]
	}

	setupProxy := func(b *testing.B, kafkaAddr string, c Compression, bs int, bt time.Duration) (
		*tcpproxy.Proxy,
		*Producer,
	) {
		proxy := &tcpproxy.Proxy{
			LocalAddr:  proxyHost,
			RemoteAddr: kafkaAddr,
		}
		go proxy.Start(b)

		client, err := New("tcp", []string{proxy.LocalAddr}, Config{})
		require.NoError(b, err)
		require.Eventuallyf(b, func() bool {
			err = client.Ping(ctx)
			return err == nil
		}, 30*time.Second, 100*time.Millisecond, "failed to connect to kafka: %v", err)

		producer, err := client.NewProducer(ProducerConfig{
			Compression:  c,
			BatchSize:    bs,
			BatchTimeout: bt,
		})
		require.NoError(b, err)

		return proxy, producer
	}

	run := func(addr string, comp Compression, value string, batchSize int, batchTimeout time.Duration) func(*testing.B) {
		return func(b *testing.B) {
			proxy, producer := setupProxy(b, addr, comp, batchSize, batchTimeout)

			kafkaCtx, kafkaCtxCancel := context.WithTimeout(context.Background(), 3*time.Minute)
			err = waitForKafka(kafkaCtx, topic, addr)
			kafkaCtxCancel()
			require.NoError(b, err)

			var (
				noOfErrors int
				messages   = make([]Message, 0, batchSize)
			)
			for i := 0; i < batchSize; i++ {
				messages = append(messages, Message{
					Key:   []byte("my-key"),
					Value: []byte(value),
					Topic: topic,
				})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := producer.Publish(ctx, messages...); err != nil {
					noOfErrors++
				}
			}
			b.StopTimer()

			_ = producer.Close(ctx)
			proxy.Stop() // stopping the proxy here to properly gather the metrics

			b.SetBytes(proxy.BytesReceived.Load())
			b.ReportMetric(float64(proxy.BytesReceived.Load())/float64(b.N)/1024, "kb/op")
			b.ReportMetric(float64(noOfErrors), "errors")
		}
	}

	var (
		compressionTypes    = []Compression{CompressionNone, CompressionGzip, CompressionSnappy, CompressionLz4, CompressionZstd}
		compressionTypesMap = map[Compression]string{
			CompressionNone: "none", CompressionGzip: "gzip", CompressionSnappy: "snappy", CompressionLz4: "lz4", CompressionZstd: "zstd",
		}
		batchSizes    = []int{1, 100, 1000}
		batchTimeouts = []time.Duration{time.Nanosecond, time.Millisecond}
		values        = []string{rand.String(1 << 10), rand.String(10 << 10), rand.String(100 << 10)}
	)
	for _, comp := range compressionTypes {
		b.Run(compressionTypesMap[comp], func(b *testing.B) {
			kafkaAddr := setupKafka(b) // setup kafka only once per compression combination
			for _, value := range values {
				for _, batchSize := range batchSizes {
					for _, batchTimeout := range batchTimeouts {
						b.Run(
							fmt.Sprintf("%s-%d-%s", byteCount(len(value)), batchSize, batchTimeout),
							run(kafkaAddr, comp, value, batchSize, batchTimeout),
						)
					}
				}
			}
		})
	}
}

func byteCount(b int) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func waitForKafka(ctx context.Context, topic, addr string) (err error) {
	tc := testutil.New("tcp", addr)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafka not ready within context (%v): %v", ctx.Err(), err)
		case <-time.After(50 * time.Millisecond):
			var topics []testutil.TopicPartition
			topics, err = tc.ListTopics(ctx)
			if err != nil {
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
				return nil
			}

			if err = tc.CreateTopic(ctx, topic, 1, 1); err != nil {
				continue
			}
		}
	}
}
