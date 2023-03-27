package pulsar

import (
	"context"
	"errors"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsarLog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type ClientConf struct {
	Url               string
	OperationTimeout  time.Duration
	ConnectionTimeout time.Duration
}

type ProducerConf struct {
	Topic                string
	SendTimeout          time.Duration
	DisableBatching      bool
	MaxReconnectToBroker int
	BatchingMaxMessages  int
	BatchingMaxSize      int
}

type Producer struct {
	Producer pulsar.Producer
}

func (p *Producer) Close() {
	if p != nil && p.Producer != nil {
		p.Producer.Close()
	}
}

type Client struct {
	Client pulsar.Client
}

func (c *Client) Close() {
	if c != nil && c.Client != nil {
		c.Client.Close()
	}
}

func NewPulsarClient(conf ClientConf, log logger.Logger) (Client, error) {
	if conf.Url == "" {
		return Client{}, errors.New("pulsar url is empty")
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               conf.Url,
		OperationTimeout:  conf.OperationTimeout,
		ConnectionTimeout: conf.ConnectionTimeout,
		Logger:            &pulsarLogAdapter{Logger: log},
	})
	if err != nil {
		return Client{}, err
	}
	return Client{Client: client}, nil
}

func NewProducer(client Client, conf ProducerConf) (Producer, error) {
	if conf.Topic == "" {
		return Producer{}, errors.New("pulsar topic is empty")
	}
	producer, err := client.Client.CreateProducer(pulsar.ProducerOptions{
		Topic:               conf.Topic,
		SendTimeout:         conf.SendTimeout,
		DisableBatching:     conf.DisableBatching,
		BatchingMaxMessages: uint(conf.BatchingMaxMessages),
		BatchingMaxSize:     uint(conf.BatchingMaxSize),
	})
	if err != nil {
		return Producer{}, err
	}
	return Producer{
		Producer: producer,
	}, nil
}

func (p *Producer) SendMessage(ctx context.Context, key, orderingKey string, msg []byte) error {
	if p == nil {
		return errors.New("producer is nil")
	}
	_, err := p.Producer.Send(ctx, &pulsar.ProducerMessage{
		Key:         key,
		OrderingKey: orderingKey,
		Payload:     msg,
	})
	return err
}

func GetProducerConf() ProducerConf {
	return ProducerConf{
		Topic:                config.GetString("Pulsar.Producer.topic", ""),
		SendTimeout:          config.GetDuration("Pulsar.Producer.sendTimeout", 30, time.Second),
		DisableBatching:      config.GetBool("Pulsar.Producer.disableBatching", false),
		MaxReconnectToBroker: config.GetInt("Pulsar.Producer.maxReconnectToBroker", 3),
		BatchingMaxMessages:  config.GetInt("Pulsar.Producer.batchingMaxMessages", 1000),
		BatchingMaxSize:      config.GetInt("Pulsar.Producer.batchingMaxSize", 128*1024),
	}
}

func GetClientConf() ClientConf {
	return ClientConf{
		Url:               config.GetString("Pulsar.Client.url", "pulsar://localhost:6650"),
		OperationTimeout:  config.GetDuration("Pulsar.Client.operationTimeout", 30, time.Second),
		ConnectionTimeout: config.GetDuration("Pulsar.Client.connectionTimeout", 30, time.Second),
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
