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
	client   pulsar.Client
	producer pulsar.Producer
	log      logger.Logger
}

type ProducerAdapter interface {
	SendMessage(ctx context.Context, key, orderingKey string, msg []byte) error
	SendMessageAsync(ctx context.Context, key, orderingKey string, msg []byte, statusFunc func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error))
	Close()
	Flush() error
}

type Client struct {
	client pulsar.Client
}

func New(config *config.Config) (ProducerAdapter, error) {
	log := logger.NewLogger().Child("pulsar")
	client, err := newPulsarClient(getClientConf(config), log)
	if err != nil {
		return nil, fmt.Errorf("error creating pulsar client : %v", err)
	}
	producer, err := newProducer(client, getProducerConf(config), log)
	if err != nil {
		return nil, fmt.Errorf("error creating pulsar producer : %v", err)
	}
	return producer, nil
}

func newPulsarClient(conf ClientConf, log logger.Logger) (*Client, error) {
	if conf.url == "" {
		return nil, errors.New("pulsar url is empty")
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               conf.url,
		OperationTimeout:  conf.operationTimeout,
		ConnectionTimeout: conf.connectionTimeout,
		Logger:            &pulsarLogAdapter{Logger: log},
	})
	if err != nil {
		return nil, err
	}
	return &Client{client: client}, nil
}

func newProducer(client *Client, conf ProducerConf, log logger.Logger) (*Producer, error) {
	producer, err := client.client.CreateProducer(pulsar.ProducerOptions{
		Topic: conf.topic,
	})
	if err != nil {
		return nil, err
	}
	return &Producer{
		producer: producer,
		log:      log,
		client:   client.client,
	}, nil
}

func (p *Producer) SendMessage(ctx context.Context, key, orderingKey string, msg []byte) error {
	if p == nil {
		return errors.New("producer is nil")
	}
	_, err := p.producer.Send(ctx, &pulsar.ProducerMessage{
		Key:         key,
		OrderingKey: orderingKey,
		Payload:     msg,
	})
	return err
}

func (p *Producer) SendMessageAsync(ctx context.Context, key, orderingKey string, msg []byte, statusfunc func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error)) {
	if p.producer == nil {
		return
	}
	p.producer.SendAsync(ctx, &pulsar.ProducerMessage{
		Payload:     msg,
		Key:         key,
		OrderingKey: orderingKey,
	}, statusfunc)
}

func (p *Producer) Close() {
	if p == nil {
		return
	}
	p.producer.Close()
	p.client.Close()
}

func (p *Producer) Flush() error {
	if p == nil {
		return nil
	}
	return p.producer.Flush()
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
