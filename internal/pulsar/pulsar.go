package pulsar

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsarLog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

//go:generate mockgen -destination=../../mocks/pulsar/mock_pulsar.go -package=mock_pulsar "github.com/rudderlabs/rudder-server/internal/pulsar" ProducerAdapter

type ClientConf struct {
	url               string
	operationTimeout  time.Duration
	connectionTimeout time.Duration
}

type ProducerConf struct {
	topic string
}

type Producer struct {
	pulsar.Producer
}

type ProducerAdapter interface {
	SendMessage(ctx context.Context, key, orderingKey string, msg []byte) error
	SendMessageAsync(ctx context.Context, key, orderingKey string, msg []byte, statusFunc func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error))
	Close()
	Flush() error
}

type Client struct {
	pulsar.Client
}

func NewClient(config *config.Config) (Client, error) {
	log := logger.NewLogger().Child("pulsar")
	client, err := newPulsarClient(getClientConf(config), log)
	if err != nil {
		return Client{}, fmt.Errorf("error creating pulsar client : %v", err)
	}
	return client, nil
}

func (c *Client) NewProducer(config *config.Config) (ProducerAdapter, error) {
	conf := getProducerConf(config)
	producer, err := c.CreateProducer(pulsar.ProducerOptions{
		Topic: conf.topic,
	})
	if err != nil {
		return nil, err
	}
	return &Producer{
		producer,
	}, nil
}

func newPulsarClient(conf ClientConf, log logger.Logger) (Client, error) {
	if conf.url == "" {
		return Client{}, errors.New("pulsar url is empty")
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               conf.url,
		OperationTimeout:  conf.operationTimeout,
		ConnectionTimeout: conf.connectionTimeout,
		Logger:            &pulsarLogAdapter{Logger: log},
	})
	if err != nil {
		return Client{}, err
	}
	return Client{client}, nil
}

func (p *Producer) SendMessage(ctx context.Context, key, orderingKey string, msg []byte) error {
	if p == nil {
		return errors.New("producer is nil")
	}
	_, err := p.Send(ctx, &pulsar.ProducerMessage{
		Key:         key,
		OrderingKey: orderingKey,
		Payload:     msg,
	})
	return err
}

func (p *Producer) SendMessageAsync(ctx context.Context, key, orderingKey string, msg []byte, statusfunc func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error)) {
	if p == nil {
		return
	}
	p.SendAsync(ctx, &pulsar.ProducerMessage{
		Payload:     msg,
		Key:         key,
		OrderingKey: orderingKey,
	}, statusfunc)
}

func getProducerConf(config *config.Config) ProducerConf {
	return ProducerConf{
		topic: config.GetString("Pulsar.Producer.topic", ""),
	}
}

func getClientConf(config *config.Config) ClientConf {
	return ClientConf{
		url:               config.GetString("Pulsar.Client.url", "pulsar://localhost:6650"),
		operationTimeout:  config.GetDuration("Pulsar.Client.operationTimeout", 30, time.Second),
		connectionTimeout: config.GetDuration("Pulsar.Client.connectionTimeout", 30, time.Second),
	}
}

type pulsarLogAdapter struct {
	logger.Logger
}

func (pl *pulsarLogAdapter) SubLogger(fields pulsarLog.Fields) pulsarLog.Logger {
	farr := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		farr = append(farr, k, v)
	}
	return &pulsarLogAdapter{pl.Logger.With(farr...)}
}

func (pl *pulsarLogAdapter) WithFields(fields pulsarLog.Fields) pulsarLog.Entry {
	return pl.SubLogger(fields)
}

func (pl *pulsarLogAdapter) WithField(name string, value interface{}) pulsarLog.Entry {
	return &pulsarLogAdapter{pl.Logger.With(name, value)}
}

func (pl *pulsarLogAdapter) WithError(err error) pulsarLog.Entry {
	return pl.WithField("error", err)
}
