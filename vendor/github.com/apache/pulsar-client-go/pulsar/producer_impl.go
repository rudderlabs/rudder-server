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

package pulsar

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

const (
	// defaultSendTimeout init default timeout for ack since sent.
	defaultSendTimeout = 30 * time.Second

	// defaultBatchingMaxPublishDelay init default for maximum delay to batch messages
	defaultBatchingMaxPublishDelay = 10 * time.Millisecond

	// defaultMaxBatchSize init default for maximum number of bytes per batch
	defaultMaxBatchSize = 128 * 1024

	// defaultMaxMessagesPerBatch init default num of entries in per batch.
	defaultMaxMessagesPerBatch = 1000

	// defaultPartitionsAutoDiscoveryInterval init default time interval for partitions auto discovery
	defaultPartitionsAutoDiscoveryInterval = 1 * time.Minute
)

type producer struct {
	sync.RWMutex
	client        *client
	options       *ProducerOptions
	topic         string
	producers     []Producer
	producersPtr  unsafe.Pointer
	numPartitions uint32
	messageRouter func(*ProducerMessage, TopicMetadata) int
	closeOnce     sync.Once
	stopDiscovery func()
	log           log.Logger
	metrics       *internal.LeveledMetrics
}

func getHashingFunction(s HashingScheme) func(string) uint32 {
	switch s {
	case JavaStringHash:
		return internal.JavaStringHash
	case Murmur3_32Hash:
		return internal.Murmur3_32Hash
	default:
		return internal.JavaStringHash
	}
}

func newProducer(client *client, options *ProducerOptions) (*producer, error) {
	if options.Topic == "" {
		return nil, newError(InvalidTopicName, "Topic name is required for producer")
	}

	if options.SendTimeout == 0 {
		options.SendTimeout = defaultSendTimeout
	}
	if options.BatchingMaxMessages == 0 {
		options.BatchingMaxMessages = defaultMaxMessagesPerBatch
	}
	if options.BatchingMaxSize == 0 {
		options.BatchingMaxSize = defaultMaxBatchSize
	}
	if options.BatchingMaxPublishDelay <= 0 {
		options.BatchingMaxPublishDelay = defaultBatchingMaxPublishDelay
	}
	if options.PartitionsAutoDiscoveryInterval <= 0 {
		options.PartitionsAutoDiscoveryInterval = defaultPartitionsAutoDiscoveryInterval
	}

	if !options.DisableBatching && options.EnableChunking {
		return nil, fmt.Errorf("batching and chunking can not be enabled together")
	}

	p := &producer{
		options: options,
		topic:   options.Topic,
		client:  client,
		log:     client.log.SubLogger(log.Fields{"topic": options.Topic}),
		metrics: client.metrics.GetLeveledMetrics(options.Topic),
	}

	if options.Interceptors == nil {
		options.Interceptors = defaultProducerInterceptors
	}

	if options.MessageRouter == nil {
		internalRouter := NewDefaultRouter(
			getHashingFunction(options.HashingScheme),
			options.BatchingMaxMessages,
			options.BatchingMaxSize,
			options.BatchingMaxPublishDelay,
			options.DisableBatching)
		p.messageRouter = func(message *ProducerMessage, metadata TopicMetadata) int {
			return internalRouter(message, metadata.NumPartitions())
		}
	} else {
		p.messageRouter = options.MessageRouter
	}

	if options.Schema != nil && options.Schema.GetSchemaInfo() != nil {
		if options.Schema.GetSchemaInfo().Type == NONE {
			options.Schema = NewBytesSchema(nil)
		}
	}

	encryption := options.Encryption
	// add default message crypto if not provided
	if encryption != nil && len(encryption.Keys) > 0 {
		if encryption.KeyReader == nil {
			return nil, fmt.Errorf("encryption is enabled, KeyReader can not be nil")
		}

		if encryption.MessageCrypto == nil {
			logCtx := fmt.Sprintf("[%v] [%v]", p.topic, p.options.Name)
			messageCrypto, err := crypto.NewDefaultMessageCrypto(logCtx,
				true,
				client.log.SubLogger(log.Fields{"topic": p.topic}))
			if err != nil {
				return nil, fmt.Errorf("unable to get MessageCrypto instance. Producer creation is abandoned. %v", err)
			}
			p.options.Encryption.MessageCrypto = messageCrypto
		}
	}

	err := p.internalCreatePartitionsProducers()
	if err != nil {
		return nil, err
	}

	p.stopDiscovery = p.runBackgroundPartitionDiscovery(options.PartitionsAutoDiscoveryInterval)

	p.metrics.ProducersOpened.Inc()
	return p, nil
}

func (p *producer) runBackgroundPartitionDiscovery(period time.Duration) (cancel func()) {
	var wg sync.WaitGroup
	stopDiscoveryCh := make(chan struct{})
	ticker := time.NewTicker(period)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopDiscoveryCh:
				return
			case <-ticker.C:
				p.log.Debug("Auto discovering new partitions")
				p.internalCreatePartitionsProducers()
			}
		}
	}()

	return func() {
		ticker.Stop()
		close(stopDiscoveryCh)
		wg.Wait()
	}
}

func (p *producer) internalCreatePartitionsProducers() error {
	partitions, err := p.client.TopicPartitions(p.topic)
	if err != nil {
		return err
	}

	oldNumPartitions := 0
	newNumPartitions := len(partitions)

	p.Lock()
	defer p.Unlock()

	oldProducers := p.producers
	oldNumPartitions = len(oldProducers)

	if oldProducers != nil {
		if oldNumPartitions == newNumPartitions {
			p.log.Debug("Number of partitions in topic has not changed")
			return nil
		}

		p.log.WithField("old_partitions", oldNumPartitions).
			WithField("new_partitions", newNumPartitions).
			Info("Changed number of partitions in topic")

	}

	p.producers = make([]Producer, newNumPartitions)

	// When for some reason (eg: forced deletion of sub partition) causes oldNumPartitions> newNumPartitions,
	// we need to rebuild the cache of new producers, otherwise the array will be out of bounds.
	if oldProducers != nil && oldNumPartitions < newNumPartitions {
		// Copy over the existing consumer instances
		for i := 0; i < oldNumPartitions; i++ {
			p.producers[i] = oldProducers[i]
		}
	}

	type ProducerError struct {
		partition int
		prod      Producer
		err       error
	}

	startPartition := oldNumPartitions
	partitionsToAdd := newNumPartitions - oldNumPartitions
	if partitionsToAdd < 0 {
		partitionsToAdd = newNumPartitions
		startPartition = 0
	}
	c := make(chan ProducerError, partitionsToAdd)

	for partitionIdx := startPartition; partitionIdx < newNumPartitions; partitionIdx++ {
		partition := partitions[partitionIdx]

		go func(partitionIdx int, partition string) {
			prod, e := newPartitionProducer(p.client, partition, p.options, partitionIdx, p.metrics)
			c <- ProducerError{
				partition: partitionIdx,
				prod:      prod,
				err:       e,
			}
		}(partitionIdx, partition)
	}

	for i := 0; i < partitionsToAdd; i++ {
		pe, ok := <-c
		if ok {
			if pe.err != nil {
				err = pe.err
			} else {
				p.producers[pe.partition] = pe.prod
			}
		}
	}

	if err != nil {
		// Since there were some failures, cleanup all the partitions that succeeded in creating the producers
		for _, producer := range p.producers {
			if producer != nil {
				producer.Close()
			}
		}
		return err
	}

	if newNumPartitions < oldNumPartitions {
		p.metrics.ProducersPartitions.Set(float64(newNumPartitions))
	} else {
		p.metrics.ProducersPartitions.Add(float64(partitionsToAdd))
	}
	atomic.StorePointer(&p.producersPtr, unsafe.Pointer(&p.producers))
	atomic.StoreUint32(&p.numPartitions, uint32(len(p.producers)))
	return nil
}

func (p *producer) Topic() string {
	return p.topic
}

func (p *producer) Name() string {
	p.RLock()
	defer p.RUnlock()

	return p.producers[0].Name()
}

func (p *producer) NumPartitions() uint32 {
	return atomic.LoadUint32(&p.numPartitions)
}

func (p *producer) Send(ctx context.Context, msg *ProducerMessage) (MessageID, error) {
	return p.getPartition(msg).Send(ctx, msg)
}

func (p *producer) SendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error)) {
	p.getPartition(msg).SendAsync(ctx, msg, callback)
}

func (p *producer) getPartition(msg *ProducerMessage) Producer {
	// Since partitions can only increase, it's ok if the producers list
	// is updated in between. The numPartition is updated only after the list.
	partition := p.messageRouter(msg, p)
	producers := *(*[]Producer)(atomic.LoadPointer(&p.producersPtr))
	if partition >= len(producers) {
		// We read the old producers list while the count was already
		// updated
		partition %= len(producers)
	}
	return producers[partition]
}

func (p *producer) LastSequenceID() int64 {
	p.RLock()
	defer p.RUnlock()

	var maxSeq int64 = -1
	for _, pp := range p.producers {
		s := pp.LastSequenceID()
		if s > maxSeq {
			maxSeq = s
		}
	}
	return maxSeq
}

func (p *producer) Flush() error {
	p.RLock()
	defer p.RUnlock()

	for _, pp := range p.producers {
		if err := pp.Flush(); err != nil {
			return err
		}

	}
	return nil
}

func (p *producer) Close() {
	p.closeOnce.Do(func() {
		p.stopDiscovery()

		p.Lock()
		defer p.Unlock()

		for _, pp := range p.producers {
			pp.Close()
		}
		p.client.handlers.Del(p)
		p.metrics.ProducersPartitions.Sub(float64(len(p.producers)))
		p.metrics.ProducersClosed.Inc()
	})
}
