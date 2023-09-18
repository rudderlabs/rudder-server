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
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

const (
	DlqTopicSuffix    = "-DLQ"
	RetryTopicSuffix  = "-RETRY"
	MaxReconsumeTimes = 16

	SysPropertyDelayTime       = "DELAY_TIME"
	SysPropertyRealTopic       = "REAL_TOPIC"
	SysPropertyRetryTopic      = "RETRY_TOPIC"
	SysPropertyReconsumeTimes  = "RECONSUMETIMES"
	SysPropertyOriginMessageID = "ORIGIN_MESSAGE_IDY_TIME"
	PropertyOriginMessageID    = "ORIGIN_MESSAGE_ID"
)

type RetryMessage struct {
	producerMsg ProducerMessage
	consumerMsg ConsumerMessage
}

type retryRouter struct {
	client    Client
	producer  Producer
	policy    *DLQPolicy
	messageCh chan RetryMessage
	closeCh   chan interface{}
	log       log.Logger
}

func newRetryRouter(client Client, policy *DLQPolicy, retryEnabled bool, logger log.Logger) (*retryRouter, error) {
	r := &retryRouter{
		client: client,
		policy: policy,
		log:    logger,
	}

	if policy != nil && retryEnabled {
		if policy.MaxDeliveries <= 0 {
			return nil, newError(InvalidConfiguration, "DLQPolicy.MaxDeliveries needs to be > 0")
		}

		if policy.RetryLetterTopic == "" {
			return nil, newError(InvalidConfiguration, "DLQPolicy.RetryLetterTopic needs to be set to a valid topic name")
		}

		r.messageCh = make(chan RetryMessage)
		r.closeCh = make(chan interface{}, 1)
		r.log = logger.SubLogger(log.Fields{"rlq-topic": policy.RetryLetterTopic})
		go r.run()
	}
	return r, nil
}

func (r *retryRouter) Chan() chan RetryMessage {
	return r.messageCh
}

func (r *retryRouter) run() {
	for {
		select {
		case rm := <-r.messageCh:
			r.log.WithField("msgID", rm.consumerMsg.ID()).Debug("Got message for RLQ")
			producer := r.getProducer()

			msgID := rm.consumerMsg.ID()
			producer.SendAsync(context.Background(), &rm.producerMsg, func(messageID MessageID,
				producerMessage *ProducerMessage, err error) {
				if err != nil {
					r.log.WithError(err).WithField("msgID", msgID).Error("Failed to send message to RLQ")
					rm.consumerMsg.Consumer.Nack(rm.consumerMsg)
				} else {
					r.log.WithField("msgID", msgID).Debug("Succeed to send message to RLQ")
					rm.consumerMsg.Consumer.AckID(msgID)
				}
			})

		case <-r.closeCh:
			if r.producer != nil {
				r.producer.Close()
			}
			r.log.Debug("Closed RLQ router")
			return
		}
	}
}

func (r *retryRouter) close() {
	// Attempt to write on the close channel, without blocking
	select {
	case r.closeCh <- nil:
	default:
	}
}

func (r *retryRouter) getProducer() Producer {
	if r.producer != nil {
		// Producer was already initialized
		return r.producer
	}

	// Retry to create producer indefinitely
	backoff := &internal.DefaultBackoff{}
	for {
		opt := r.policy.ProducerOptions
		opt.Topic = r.policy.RetryLetterTopic
		// the origin code sets to LZ4 compression with no options
		// so the new design allows compression type to be overwritten but still set lz4 by default
		if r.policy.ProducerOptions.CompressionType == NoCompression {
			opt.CompressionType = LZ4
		}

		producer, err := r.client.CreateProducer(opt)

		if err != nil {
			r.log.WithError(err).Error("Failed to create RLQ producer")
			time.Sleep(backoff.Next())
			continue
		} else {
			r.producer = producer
			return producer
		}
	}
}
