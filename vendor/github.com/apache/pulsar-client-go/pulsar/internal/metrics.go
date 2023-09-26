// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	metricsLevel      int
	messagesPublished *prometheus.CounterVec
	bytesPublished    *prometheus.CounterVec
	messagesPending   *prometheus.GaugeVec
	bytesPending      *prometheus.GaugeVec
	publishErrors     *prometheus.CounterVec
	publishLatency    *prometheus.HistogramVec
	publishRPCLatency *prometheus.HistogramVec

	messagesReceived   *prometheus.CounterVec
	bytesReceived      *prometheus.CounterVec
	prefetchedMessages *prometheus.GaugeVec
	prefetchedBytes    *prometheus.GaugeVec
	acksCounter        *prometheus.CounterVec
	nacksCounter       *prometheus.CounterVec
	dlqCounter         *prometheus.CounterVec
	processingTime     *prometheus.HistogramVec

	producersOpened            *prometheus.CounterVec
	producersClosed            *prometheus.CounterVec
	producersReconnectFailure  *prometheus.CounterVec
	producersReconnectMaxRetry *prometheus.CounterVec
	producersPartitions        *prometheus.GaugeVec
	consumersOpened            *prometheus.CounterVec
	consumersClosed            *prometheus.CounterVec
	consumersReconnectFailure  *prometheus.CounterVec
	consumersReconnectMaxRetry *prometheus.CounterVec
	consumersPartitions        *prometheus.GaugeVec
	readersOpened              *prometheus.CounterVec
	readersClosed              *prometheus.CounterVec

	// Metrics that are not labeled with specificity are immediately available
	ConnectionsOpened                     prometheus.Counter
	ConnectionsClosed                     prometheus.Counter
	ConnectionsEstablishmentErrors        prometheus.Counter
	ConnectionsHandshakeErrors            prometheus.Counter
	LookupRequestsCount                   prometheus.Counter
	PartitionedTopicMetadataRequestsCount prometheus.Counter
	RPCRequestCount                       prometheus.Counter
}

type LeveledMetrics struct {
	MessagesPublished        prometheus.Counter
	BytesPublished           prometheus.Counter
	MessagesPending          prometheus.Gauge
	BytesPending             prometheus.Gauge
	PublishErrorsTimeout     prometheus.Counter
	PublishErrorsMsgTooLarge prometheus.Counter
	PublishLatency           prometheus.Observer
	PublishRPCLatency        prometheus.Observer

	MessagesReceived   prometheus.Counter
	BytesReceived      prometheus.Counter
	PrefetchedMessages prometheus.Gauge
	PrefetchedBytes    prometheus.Gauge
	AcksCounter        prometheus.Counter
	NacksCounter       prometheus.Counter
	DlqCounter         prometheus.Counter
	ProcessingTime     prometheus.Observer

	ProducersOpened            prometheus.Counter
	ProducersClosed            prometheus.Counter
	ProducersReconnectFailure  prometheus.Counter
	ProducersReconnectMaxRetry prometheus.Counter
	ProducersPartitions        prometheus.Gauge
	ConsumersOpened            prometheus.Counter
	ConsumersClosed            prometheus.Counter
	ConsumersReconnectFailure  prometheus.Counter
	ConsumersReconnectMaxRetry prometheus.Counter
	ConsumersPartitions        prometheus.Gauge
	ReadersOpened              prometheus.Counter
	ReadersClosed              prometheus.Counter
}

// NewMetricsProvider returns metrics registered to registerer.
func NewMetricsProvider(metricsCardinality int, userDefinedLabels map[string]string,
	registerer prometheus.Registerer) *Metrics {
	constLabels := map[string]string{
		"client": "go",
	}
	for k, v := range userDefinedLabels {
		constLabels[k] = v
	}
	var metricsLevelLabels []string

	// note: ints here mirror MetricsCardinality in client.go to avoid import cycle
	switch metricsCardinality {
	case 1: //MetricsCardinalityNone
		metricsLevelLabels = []string{}
	case 2: //MetricsCardinalityTenant
		metricsLevelLabels = []string{"pulsar_tenant"}
	case 3: //MetricsCardinalityNamespace
		metricsLevelLabels = []string{"pulsar_tenant", "pulsar_namespace"}
	case 4: //MetricsCardinalityTopic
		metricsLevelLabels = []string{"pulsar_tenant", "pulsar_namespace", "topic"}
	default: //Anything else is namespace
		metricsLevelLabels = []string{"pulsar_tenant", "pulsar_namespace"}
	}

	metrics := &Metrics{
		metricsLevel: metricsCardinality,
		messagesPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_messages_published",
			Help:        "Counter of messages published by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		bytesPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_bytes_published",
			Help:        "Counter of messages published by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		messagesPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_producer_pending_messages",
			Help:        "Counter of messages pending to be published by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		bytesPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_producer_pending_bytes",
			Help:        "Counter of bytes pending to be published by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		publishErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_producer_errors",
			Help:        "Counter of publish errors",
			ConstLabels: constLabels,
		}, append(metricsLevelLabels, "error")),

		publishLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "pulsar_client_producer_latency_seconds",
			Help:        "Publish latency experienced by the client",
			ConstLabels: constLabels,
			Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, metricsLevelLabels),

		publishRPCLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "pulsar_client_producer_rpc_latency_seconds",
			Help:        "Publish RPC latency experienced internally by the client when sending data to receiving an ack",
			ConstLabels: constLabels,
			Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, metricsLevelLabels),

		producersOpened: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_producers_opened",
			Help:        "Counter of producers created by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		producersClosed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_producers_closed",
			Help:        "Counter of producers closed by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		producersPartitions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_producers_partitions_active",
			Help:        "Counter of individual partitions the producers are currently active",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		producersReconnectFailure: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_producers_reconnect_failure",
			Help:        "Counter of reconnect failure of producers",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		producersReconnectMaxRetry: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_producers_reconnect_max_retry",
			Help:        "Counter of producer reconnect max retry reached",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		consumersOpened: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumers_opened",
			Help:        "Counter of consumers created by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		consumersClosed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumers_closed",
			Help:        "Counter of consumers closed by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		consumersReconnectFailure: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumers_reconnect_failure",
			Help:        "Counter of reconnect failure of consumers",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		consumersReconnectMaxRetry: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumers_reconnect_max_retry",
			Help:        "Counter of consumer reconnect max retry reached",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		consumersPartitions: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_consumers_partitions_active",
			Help:        "Counter of individual partitions the consumers are currently active",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		messagesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_messages_received",
			Help:        "Counter of messages received by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		bytesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_bytes_received",
			Help:        "Counter of bytes received by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		prefetchedMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_consumer_prefetched_messages",
			Help:        "Number of messages currently sitting in the consumer pre-fetch queue",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		prefetchedBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "pulsar_client_consumer_prefetched_bytes",
			Help:        "Total number of bytes currently sitting in the consumer pre-fetch queue",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		acksCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumer_acks",
			Help:        "Counter of messages acked by client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		nacksCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumer_nacks",
			Help:        "Counter of messages nacked by client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		dlqCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_consumer_dlq_messages",
			Help:        "Counter of messages sent to Dead letter queue",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		processingTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "pulsar_client_consumer_processing_time_seconds",
			Help:        "Time it takes for application to process messages",
			Buckets:     []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		readersOpened: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_readers_opened",
			Help:        "Counter of readers created by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		readersClosed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "pulsar_client_readers_closed",
			Help:        "Counter of readers closed by the client",
			ConstLabels: constLabels,
		}, metricsLevelLabels),

		ConnectionsOpened: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_connections_opened",
			Help:        "Counter of connections created by the client",
			ConstLabels: constLabels,
		}),

		ConnectionsClosed: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_connections_closed",
			Help:        "Counter of connections closed by the client",
			ConstLabels: constLabels,
		}),

		ConnectionsEstablishmentErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_connections_establishment_errors",
			Help:        "Counter of errors in connections establishment",
			ConstLabels: constLabels,
		}),

		ConnectionsHandshakeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_connections_handshake_errors",
			Help:        "Counter of errors in connections handshake (eg: authz)",
			ConstLabels: constLabels,
		}),

		LookupRequestsCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_lookup_count",
			Help:        "Counter of lookup requests made by the client",
			ConstLabels: constLabels,
		}),

		PartitionedTopicMetadataRequestsCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_partitioned_topic_metadata_count",
			Help:        "Counter of partitioned_topic_metadata requests made by the client",
			ConstLabels: constLabels,
		}),

		RPCRequestCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "pulsar_client_rpc_count",
			Help:        "Counter of RPC requests made by the client",
			ConstLabels: constLabels,
		}),
	}

	err := registerer.Register(metrics.messagesPublished)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.messagesPublished = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.bytesPublished)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.bytesPublished = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.messagesPending)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.messagesPending = are.ExistingCollector.(*prometheus.GaugeVec)
		}
	}
	err = registerer.Register(metrics.bytesPending)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.bytesPending = are.ExistingCollector.(*prometheus.GaugeVec)
		}
	}
	err = registerer.Register(metrics.publishErrors)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.publishErrors = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.publishLatency)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.publishLatency = are.ExistingCollector.(*prometheus.HistogramVec)
		}
	}
	err = registerer.Register(metrics.publishRPCLatency)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.publishRPCLatency = are.ExistingCollector.(*prometheus.HistogramVec)
		}
	}
	err = registerer.Register(metrics.messagesReceived)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.messagesReceived = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.bytesReceived)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.bytesReceived = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.prefetchedMessages)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.prefetchedMessages = are.ExistingCollector.(*prometheus.GaugeVec)
		}
	}
	err = registerer.Register(metrics.prefetchedBytes)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.prefetchedBytes = are.ExistingCollector.(*prometheus.GaugeVec)
		}
	}
	err = registerer.Register(metrics.acksCounter)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.acksCounter = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.nacksCounter)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.nacksCounter = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.dlqCounter)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.dlqCounter = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.processingTime)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.processingTime = are.ExistingCollector.(*prometheus.HistogramVec)
		}
	}
	err = registerer.Register(metrics.producersOpened)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.producersOpened = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.producersClosed)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.producersClosed = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.producersReconnectFailure)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.producersReconnectFailure = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.producersReconnectMaxRetry)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.producersReconnectMaxRetry = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.producersPartitions)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.producersPartitions = are.ExistingCollector.(*prometheus.GaugeVec)
		}
	}
	err = registerer.Register(metrics.consumersOpened)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.consumersOpened = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.consumersClosed)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.consumersClosed = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.consumersReconnectFailure)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.consumersReconnectFailure = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.consumersReconnectMaxRetry)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.consumersReconnectMaxRetry = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.consumersPartitions)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.consumersPartitions = are.ExistingCollector.(*prometheus.GaugeVec)
		}
	}
	err = registerer.Register(metrics.readersOpened)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.readersOpened = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.readersClosed)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.readersClosed = are.ExistingCollector.(*prometheus.CounterVec)
		}
	}
	err = registerer.Register(metrics.ConnectionsOpened)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.ConnectionsOpened = are.ExistingCollector.(prometheus.Counter)
		}
	}
	err = registerer.Register(metrics.ConnectionsClosed)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.ConnectionsClosed = are.ExistingCollector.(prometheus.Counter)
		}
	}
	err = registerer.Register(metrics.ConnectionsEstablishmentErrors)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.ConnectionsEstablishmentErrors = are.ExistingCollector.(prometheus.Counter)
		}
	}
	err = registerer.Register(metrics.ConnectionsHandshakeErrors)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.ConnectionsHandshakeErrors = are.ExistingCollector.(prometheus.Counter)
		}
	}
	err = registerer.Register(metrics.LookupRequestsCount)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.LookupRequestsCount = are.ExistingCollector.(prometheus.Counter)
		}
	}
	err = registerer.Register(metrics.PartitionedTopicMetadataRequestsCount)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.PartitionedTopicMetadataRequestsCount = are.ExistingCollector.(prometheus.Counter)
		}
	}
	err = registerer.Register(metrics.RPCRequestCount)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metrics.RPCRequestCount = are.ExistingCollector.(prometheus.Counter)
		}
	}
	return metrics
}

func (mp *Metrics) GetLeveledMetrics(t string) *LeveledMetrics {
	labels := make(map[string]string, 3)
	tn, err := ParseTopicName(t)
	if err != nil {
		return nil
	}
	topic := TopicNameWithoutPartitionPart(tn)
	switch mp.metricsLevel {
	case 4:
		labels["topic"] = topic
		fallthrough
	case 3:
		labels["pulsar_namespace"] = tn.Namespace
		fallthrough
	case 2:
		labels["pulsar_tenant"] = tn.Tenant
	}

	lm := &LeveledMetrics{
		MessagesPublished:        mp.messagesPublished.With(labels),
		BytesPublished:           mp.bytesPublished.With(labels),
		MessagesPending:          mp.messagesPending.With(labels),
		BytesPending:             mp.bytesPending.With(labels),
		PublishErrorsTimeout:     mp.publishErrors.With(mergeMaps(labels, map[string]string{"error": "timeout"})),
		PublishErrorsMsgTooLarge: mp.publishErrors.With(mergeMaps(labels, map[string]string{"error": "msg_too_large"})),
		PublishLatency:           mp.publishLatency.With(labels),
		PublishRPCLatency:        mp.publishRPCLatency.With(labels),

		MessagesReceived:   mp.messagesReceived.With(labels),
		BytesReceived:      mp.bytesReceived.With(labels),
		PrefetchedMessages: mp.prefetchedMessages.With(labels),
		PrefetchedBytes:    mp.prefetchedBytes.With(labels),
		AcksCounter:        mp.acksCounter.With(labels),
		NacksCounter:       mp.nacksCounter.With(labels),
		DlqCounter:         mp.dlqCounter.With(labels),
		ProcessingTime:     mp.processingTime.With(labels),

		ProducersOpened:            mp.producersOpened.With(labels),
		ProducersClosed:            mp.producersClosed.With(labels),
		ProducersReconnectFailure:  mp.producersReconnectFailure.With(labels),
		ProducersReconnectMaxRetry: mp.producersReconnectMaxRetry.With(labels),
		ProducersPartitions:        mp.producersPartitions.With(labels),
		ConsumersOpened:            mp.consumersOpened.With(labels),
		ConsumersClosed:            mp.consumersClosed.With(labels),
		ConsumersReconnectFailure:  mp.consumersReconnectFailure.With(labels),
		ConsumersReconnectMaxRetry: mp.consumersReconnectMaxRetry.With(labels),
		ConsumersPartitions:        mp.consumersPartitions.With(labels),
		ReadersOpened:              mp.readersOpened.With(labels),
		ReadersClosed:              mp.readersClosed.With(labels),
	}

	return lm
}

func mergeMaps(a, b map[string]string) map[string]string {
	res := make(map[string]string)
	for k, v := range a {
		res[k] = v
	}
	for k, v := range b {
		res[k] = v
	}

	return res
}
