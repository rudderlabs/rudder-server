package processor

import (
	"errors"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsarlog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type ClientOption func(*pulsar.ClientOptions)

func WithCustomMetricsLabels(labels map[string]string) ClientOption {
	return func(o *pulsar.ClientOptions) {
		o.CustomMetricsLabels = labels
	}
}

func NewPulsarClient(conf *config.Config, log logger.Logger, opts ...ClientOption) (pulsar.Client, error) {
	pulsarURL := conf.GetString("Pulsar.Client.url", "pulsar://localhost:6650")

	var authentication pulsar.Authentication
	if conf.GetBool("Pulsar.Client.OAuth2.enabled", false) {
		authentication = pulsar.NewAuthenticationOAuth2(map[string]string{
			"type":      conf.GetString("Pulsar.Client.OAuth2.type", "client_credentials"),
			"issuerUrl": conf.GetString("Pulsar.Client.OAuth2.issuerUrl", "https://auth.streamnative.cloud/"),
			"audience":  conf.GetString("Pulsar.Client.OAuth2.audience", ""),
			// Absolute path of your downloaded key file e.g. file:///YOUR-KEY-FILE-PATH
			"privateKey": conf.GetString("Pulsar.Client.OAuth2.privateKey", ""),
		})
	}

	noOfConsumers := conf.GetInt("Pulsar.Client.noOfConsumers", 5)
	if noOfConsumers < 1 {
		return nil, errors.New("number of consumers must be at least 1")
	}

	maxConnectionsPerBroker := conf.GetInt("Pulsar.Client.maxConnectionsPerBroker", 6)
	if noOfConsumers > maxConnectionsPerBroker {
		log.Warnn("Number of consumers is greater than max connections per broker",
			logger.NewIntField("noOfConsumers", int64(noOfConsumers)),
			logger.NewIntField("maxConnectionsPerBroker", int64(maxConnectionsPerBroker)),
		)
		// We assume by default we're handling a topic with a single partition.
		// If the topic has multiple partitions, please set these variables manually like:
		// maxConnectionsPerBroker = (noOfConsumers * noOfPartitions) + 1
		maxConnectionsPerBroker = noOfConsumers + 1
	}

	clientOptions := pulsar.ClientOptions{
		URL:                     pulsarURL,
		Authentication:          authentication,
		OperationTimeout:        conf.GetDuration("Pulsar.Client.operationTimeout", 30, time.Second),
		ConnectionTimeout:       conf.GetDuration("Pulsar.Client.connectionTimeout", 10, time.Second),
		MaxConnectionsPerBroker: maxConnectionsPerBroker,
		MemoryLimitBytes:        conf.GetInt64("Pulsar.Client.memoryLimitBytes", 64*1024*1024 /* 64MB */),
		Logger:                  &pulsarLogAdapter{Logger: log},
		MetricsRegisterer:       prometheus.DefaultRegisterer, // TODO can we get the registerer from stats?
		// EnableTransaction:       true,
	}
	for _, opt := range opts {
		opt(&clientOptions)
	}

	pc, err := pulsar.NewClient(clientOptions)

	// Example of using transactions
	//tx, err := pc.NewTransaction(time.Minute)
	//
	//producer, err := pc.CreateProducer(pulsar.ProducerOptions{})
	//producer.Send(context.Background(), &pulsar.ProducerMessage{Transaction: tx})
	//
	//consumer, err := pc.Subscribe(pulsar.ConsumerOptions{})
	//consumer.AckWithTxn(&pulsar.ConsumerMessage{}, tx)

	return pc, err
}

type pulsarLogAdapter struct {
	logger.Logger
}

func (pl *pulsarLogAdapter) SubLogger(fields pulsarlog.Fields) pulsarlog.Logger {
	logFields := make([]logger.Field, 0, len(fields))
	for k, v := range fields {
		logFields = append(logFields, logger.NewField(k, v))
	}
	return &pulsarLogAdapter{pl.Logger.Withn(logFields...)}
}

func (pl *pulsarLogAdapter) WithFields(fields pulsarlog.Fields) pulsarlog.Entry {
	return pl.SubLogger(fields)
}

func (pl *pulsarLogAdapter) WithField(name string, value interface{}) pulsarlog.Entry {
	return &pulsarLogAdapter{pl.Logger.Withn(logger.NewField(name, value))}
}

func (pl *pulsarLogAdapter) WithError(err error) pulsarlog.Entry {
	return pl.WithField("error", err)
}
