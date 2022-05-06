package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/services/streammanager/kafka/client"
)

func TestMain(m *testing.M) {
	pkgLogger = &nopLogger{}
	os.Exit(m.Run())
}

func TestNewProducer(t *testing.T) {
	t.Skip("TODO")
}

func TestNewProducerForAzureEventHubs(t *testing.T) {
	t.Skip("TODO")
}

func TestProducerForConfluentCloud(t *testing.T) {
	t.Skip("TODO")
}

func TestPrepareBatchOfMessages(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		now := time.Now()
		var data []map[string]interface{}
		batch, err := prepareBatchOfMessages("some-topic", data, now)
		require.NoError(t, err)
		require.Nil(t, batch)
	})

	t.Run("no message", func(t *testing.T) {
		now := time.Now()
		data := []map[string]interface{}{{
			"not-interesting": "some value",
		}}
		batch, err := prepareBatchOfMessages("some-topic", data, now)
		require.NoError(t, err)
		require.Nil(t, batch)
	})

	t.Run("with message and user id", func(t *testing.T) {
		now := time.Now()
		data := []map[string]interface{}{
			{"not-interesting": "some value"},
			{"message": "msg01"},
			{"message": "msg02", "userId": "123"},
			{"message": map[string]interface{}{"a": 1, "b": 2}, "userId": "456"},
		}
		batch, err := prepareBatchOfMessages("some-topic", data, now)
		require.NoError(t, err)
		require.ElementsMatch(t, []client.Message{
			{
				Key:       []byte("123"),
				Value:     []byte(`"msg02"`),
				Topic:     "some-topic",
				Timestamp: now,
			},
			{
				Key:       []byte("456"),
				Value:     []byte(`{"a":1,"b":2}`),
				Topic:     "some-topic",
				Timestamp: now,
			},
		}, batch)
	})
}

func TestCloseProducer(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		err := CloseProducer(context.Background(), nil)
		require.EqualError(t, err, "error while closing producer")
	})
	t.Run("not initialized", func(t *testing.T) {
		p := &client.Producer{}
		err := CloseProducer(context.Background(), p)
		require.NoError(t, err)
	})
	t.Run("correct", func(t *testing.T) {
		c, err := client.New("tcp", "localhost:9092", client.Config{})
		require.NoError(t, err)
		p, err := c.NewProducer("some-topic", client.ProducerConfig{})
		require.NoError(t, err)
		err = CloseProducer(context.Background(), p)
		require.NoError(t, err)
	})
	t.Run("error", func(t *testing.T) {
		var p producer = &pMockErr{fmt.Errorf("a bad error")}
		err := CloseProducer(context.Background(), p)
		require.EqualError(t, err, "a bad error")
	})
}

func TestProduce(t *testing.T) {
	t.Skip("TODO")
}

func TestSendBatchedMessage(t *testing.T) {
	t.Skip("TODO")
}

func TestSendMessage(t *testing.T) {
	t.Skip("TODO")
}

// Mocks
type pMockErr struct{ error }

func (p *pMockErr) Close(_ context.Context) error                        { return p }
func (p *pMockErr) Publish(_ context.Context, _ ...client.Message) error { return p }

type nopLogger struct{}

func (*nopLogger) Error(...interface{})          {}
func (*nopLogger) Errorf(string, ...interface{}) {}
