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
	"time"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

const (
	defaultReceiverQueueSize = 1000
)

type reader struct {
	sync.Mutex
	client              *client
	pc                  *partitionConsumer
	messageCh           chan ConsumerMessage
	lastMessageInBroker *trackingMessageID
	log                 log.Logger
	metrics             *internal.LeveledMetrics
}

func newReader(client *client, options ReaderOptions) (Reader, error) {
	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is required")
	}

	if options.StartMessageID == nil {
		return nil, newError(InvalidConfiguration, "StartMessageID is required")
	}

	var startMessageID *trackingMessageID
	if !checkMessageIDType(options.StartMessageID) {
		// a custom type satisfying MessageID may not be a messageID or trackingMessageID
		// so re-create messageID using its data
		deserMsgID, err := deserializeMessageID(options.StartMessageID.Serialize())
		if err != nil {
			return nil, err
		}
		// de-serialized MessageID is a messageID
		startMessageID = toTrackingMessageID(deserMsgID)
	} else {
		startMessageID = toTrackingMessageID(options.StartMessageID)
	}

	subscriptionName := options.SubscriptionName
	if subscriptionName == "" {
		subscriptionName = options.SubscriptionRolePrefix
		if subscriptionName == "" {
			subscriptionName = "reader"
		}
		subscriptionName += "-" + generateRandomName()
	}

	receiverQueueSize := options.ReceiverQueueSize
	if receiverQueueSize <= 0 {
		receiverQueueSize = defaultReceiverQueueSize
	}

	// decryption is enabled, use default message crypto if not provided
	if options.Decryption != nil && options.Decryption.MessageCrypto == nil {
		messageCrypto, err := crypto.NewDefaultMessageCrypto("decrypt",
			false,
			client.log.SubLogger(log.Fields{"topic": options.Topic}))
		if err != nil {
			return nil, err
		}
		options.Decryption.MessageCrypto = messageCrypto
	}

	if options.MaxPendingChunkedMessage == 0 {
		options.MaxPendingChunkedMessage = 100
	}

	if options.ExpireTimeOfIncompleteChunk == 0 {
		options.ExpireTimeOfIncompleteChunk = time.Minute
	}

	consumerOptions := &partitionConsumerOpts{
		topic:                       options.Topic,
		consumerName:                options.Name,
		subscription:                subscriptionName,
		subscriptionType:            Exclusive,
		receiverQueueSize:           receiverQueueSize,
		startMessageID:              startMessageID,
		startMessageIDInclusive:     options.StartMessageIDInclusive,
		subscriptionMode:            NonDurable,
		readCompacted:               options.ReadCompacted,
		metadata:                    options.Properties,
		nackRedeliveryDelay:         defaultNackRedeliveryDelay,
		replicateSubscriptionState:  false,
		decryption:                  options.Decryption,
		schema:                      options.Schema,
		backoffPolicy:               options.BackoffPolicy,
		maxPendingChunkedMessage:    options.MaxPendingChunkedMessage,
		expireTimeOfIncompleteChunk: options.ExpireTimeOfIncompleteChunk,
		autoAckIncompleteChunk:      options.AutoAckIncompleteChunk,
	}

	reader := &reader{
		client:    client,
		messageCh: make(chan ConsumerMessage),
		log:       client.log.SubLogger(log.Fields{"topic": options.Topic}),
		metrics:   client.metrics.GetLeveledMetrics(options.Topic),
	}

	// Provide dummy dlq router with not dlq policy
	dlq, err := newDlqRouter(client, nil, client.log)
	if err != nil {
		return nil, err
	}

	pc, err := newPartitionConsumer(nil, client, consumerOptions, reader.messageCh, dlq, reader.metrics)
	if err != nil {
		close(reader.messageCh)
		return nil, err
	}

	reader.pc = pc
	reader.metrics.ReadersOpened.Inc()
	return reader, nil
}

func (r *reader) Topic() string {
	return r.pc.topic
}

func (r *reader) Next(ctx context.Context) (Message, error) {
	for {
		select {
		case cm, ok := <-r.messageCh:
			if !ok {
				return nil, newError(ConsumerClosed, "consumer closed")
			}

			// Acknowledge message immediately because the reader is based on non-durable subscription. When it reconnects,
			// it will specify the subscription position anyway
			msgID := cm.Message.ID()
			mid := toTrackingMessageID(msgID)
			r.pc.lastDequeuedMsg = mid
			r.pc.AckID(mid)
			return cm.Message, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *reader) HasNext() bool {
	if r.lastMessageInBroker != nil && r.hasMoreMessages() {
		return true
	}

	for {
		lastMsgID, err := r.pc.getLastMessageID()
		if err != nil {
			r.log.WithError(err).Error("Failed to get last message id from broker")
			continue
		} else {
			r.lastMessageInBroker = lastMsgID
			break
		}
	}

	return r.hasMoreMessages()
}

func (r *reader) hasMoreMessages() bool {
	if r.pc.lastDequeuedMsg != nil {
		return r.lastMessageInBroker.isEntryIDValid() && r.lastMessageInBroker.greater(r.pc.lastDequeuedMsg.messageID)
	}

	if r.pc.options.startMessageIDInclusive {
		return r.lastMessageInBroker.isEntryIDValid() &&
			r.lastMessageInBroker.greaterEqual(r.pc.startMessageID.get().messageID)
	}

	// Non-inclusive
	return r.lastMessageInBroker.isEntryIDValid() &&
		r.lastMessageInBroker.greater(r.pc.startMessageID.get().messageID)
}

func (r *reader) Close() {
	r.pc.Close()
	r.client.handlers.Del(r)
	r.metrics.ReadersClosed.Inc()
}

func (r *reader) messageID(msgID MessageID) *trackingMessageID {
	mid := toTrackingMessageID(msgID)

	partition := int(mid.partitionIdx)
	// did we receive a valid partition index?
	if partition < 0 {
		r.log.Warnf("invalid partition index %d expected", partition)
		return nil
	}

	return mid
}

func (r *reader) Seek(msgID MessageID) error {
	r.Lock()
	defer r.Unlock()

	if !checkMessageIDType(msgID) {
		r.log.Warnf("invalid message id type %T", msgID)
		return fmt.Errorf("invalid message id type %T", msgID)
	}

	mid := r.messageID(msgID)
	if mid == nil {
		return nil
	}

	return r.pc.Seek(mid)
}

func (r *reader) SeekByTime(time time.Time) error {
	r.Lock()
	defer r.Unlock()

	return r.pc.SeekByTime(time)
}
