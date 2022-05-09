package kafka

import (
	"context"
	"encoding/json"
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

	t.Run("with empty user id and allow empty", func(t *testing.T) {
		now := time.Now()
		allowReqsWithoutUserIDAndAnonymousID = true
		data := []map[string]interface{}{
			{"not-interesting": "some value"},
			{"message": "msg01"},
		}
		batch, err := prepareBatchOfMessages("some-topic", data, now)
		require.NoError(t, err)
		require.ElementsMatch(t, []client.Message{
			{
				Key:       []byte(""),
				Value:     []byte(`"msg01"`),
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
		var p producer = &pMockErr{error: fmt.Errorf("a bad error")}
		err := CloseProducer(context.Background(), p)
		require.EqualError(t, err, "a bad error")
	})
}

func TestProduce(t *testing.T) {
	t.Skip("TODO")
}

func TestSendBatchedMessage(t *testing.T) {
	t.Run("invalid json", func(t *testing.T) {
		sc, res, err := sendBatchedMessage(
			json.RawMessage("{{{"),
			nil,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Error while unmarshalling json data: "+
			"invalid character '{' looking for beginning of object key string", err)
	})

	t.Run("invalid data", func(t *testing.T) {
		sc, res, err := sendBatchedMessage(
			json.RawMessage(`{"message":"ciao"}`), // not a slice of map[string]interface{}
			nil,
			"some-topic",
		)
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Error while unmarshalling json data: "+
			"json: cannot unmarshal object into Go value of type []map[string]interface {}", err)
	})

	t.Run("publisher error", func(t *testing.T) {
		p := &pMockErr{error: fmt.Errorf("something bad")}
		sc, res, err := sendBatchedMessage(
			json.RawMessage(`[{"message":"ciao","userId":"123"}]`),
			p,
			"some-topic",
		)
		require.Equal(t, 500, sc)
		require.Equal(t, "something bad error occurred.", res)
		require.Equal(t, "something bad", err)
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte("123"), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})

	t.Run("ok", func(t *testing.T) {
		p := &pMockErr{error: nil}
		sc, res, err := sendBatchedMessage(
			json.RawMessage(`[{"message":"ciao","userId":"123"}]`),
			p,
			"some-topic",
		)
		require.Equal(t, 200, sc)
		require.Equal(t, "Kafka: Message delivered in batch", res)
		require.Equal(t, "Kafka: Message delivered in batch", err)
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte("123"), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})
}

func TestSendMessage(t *testing.T) {
	t.Run("invalid json", func(t *testing.T) {
		sc, res, err := sendMessage(json.RawMessage("{{{"), nil, "some-topic")
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Invalid message", err)
	})

	t.Run("no message", func(t *testing.T) {
		sc, res, err := sendMessage(json.RawMessage("{}"), nil, "some-topic")
		require.Equal(t, 400, sc)
		require.Equal(t, "Failure", res)
		require.Equal(t, "Invalid message", err)
	})

	t.Run("no userId", func(t *testing.T) {
		p := &pMockErr{error: nil}
		sc, res, err := sendMessage(json.RawMessage(`{"message":"ciao"}`), p, "some-topic")
		require.Equal(t, 200, sc)
		require.Equal(t, "Message delivered to topic: some-topic", res)
		require.Equal(t, "Message delivered to topic: some-topic", err)
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte(""), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})

	t.Run("publisher error", func(t *testing.T) {
		p := &pMockErr{error: fmt.Errorf("something bad")}
		sc, res, err := sendMessage(
			json.RawMessage(`{"message":"ciao","userId":"123"}`),
			p,
			"some-topic",
		)
		require.Equal(t, 500, sc)
		require.Equal(t, "something bad error occurred.", res)
		require.Equal(t, "something bad", err)
		require.Len(t, p.calls, 1)
		require.Len(t, p.calls[0], 1)
		require.Equal(t, []byte("123"), p.calls[0][0].Key)
		require.Equal(t, []byte(`"ciao"`), p.calls[0][0].Value)
		require.Equal(t, "some-topic", p.calls[0][0].Topic)
		require.InDelta(t, time.Now().Unix(), p.calls[0][0].Timestamp.Unix(), 1)
	})
}

// Mocks
type pMockErr struct {
	error error
	calls [][]client.Message
}

func (p *pMockErr) Close(_ context.Context) error { return p.error }
func (p *pMockErr) Publish(_ context.Context, msgs ...client.Message) error {
	p.calls = append(p.calls, msgs)
	return p.error
}

type nopLogger struct{}

func (*nopLogger) Error(...interface{})          {}
func (*nopLogger) Errorf(string, ...interface{}) {}
