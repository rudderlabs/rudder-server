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

type dlqRouter struct {
	client    Client
	producer  Producer
	policy    *DLQPolicy
	messageCh chan ConsumerMessage
	closeCh   chan interface{}
	log       log.Logger
}

func newDlqRouter(client Client, policy *DLQPolicy, logger log.Logger) (*dlqRouter, error) {
	r := &dlqRouter{
		client: client,
		policy: policy,
		log:    logger,
	}

	if policy != nil {
		if policy.MaxDeliveries <= 0 {
			return nil, newError(InvalidConfiguration, "DLQPolicy.MaxDeliveries needs to be > 0")
		}

		if policy.DeadLetterTopic == "" {
			return nil, newError(InvalidConfiguration, "DLQPolicy.Topic needs to be set to a valid topic name")
		}

		r.messageCh = make(chan ConsumerMessage)
		r.closeCh = make(chan interface{}, 1)
		r.log = logger.SubLogger(log.Fields{"dlq-topic": policy.DeadLetterTopic})
		go r.run()
	}
	return r, nil
}

func (r *dlqRouter) shouldSendToDlq(cm *ConsumerMessage) bool {
	if r.policy == nil {
		return false
	}

	msg := cm.Message.(*message)
	r.log.WithField("count", msg.redeliveryCount).
		WithField("max", r.policy.MaxDeliveries).
		WithField("msgId", msg.msgID).
		Debug("Should route to DLQ?")

	// We use >= here because we're comparing the number of re-deliveries with
	// the number of deliveries. So:
	//  * the user specifies that wants to process a message up to 10 times.
	//  * the first time, the redeliveryCount == 0, then 1 and so on
	//  * when we receive the message and redeliveryCount == 10, it means
	//    that the application has already got (and Nack())  the message 10
	//    times, so this time we should just go to DLQ.

	return msg.redeliveryCount >= r.policy.MaxDeliveries
}

func (r *dlqRouter) Chan() chan ConsumerMessage {
	return r.messageCh
}

func (r *dlqRouter) run() {
	for {
		select {
		case cm := <-r.messageCh:
			r.log.WithField("msgID", cm.ID()).Debug("Got message for DLQ")
			producer := r.getProducer(cm.Consumer.(*consumer).options.Schema)
			msg := cm.Message.(*message)
			msgID := msg.ID()

			// properties associated with original message
			properties := msg.Properties()

			// include orinal message id in string format in properties
			properties[PropertyOriginMessageID] = msgID.String()

			// include original topic name of the message in properties
			properties[SysPropertyRealTopic] = msg.Topic()

			producer.SendAsync(context.Background(), &ProducerMessage{
				Payload:             msg.Payload(),
				Key:                 msg.Key(),
				OrderingKey:         msg.OrderingKey(),
				Properties:          properties,
				EventTime:           msg.EventTime(),
				ReplicationClusters: msg.replicationClusters,
			}, func(messageID MessageID, producerMessage *ProducerMessage, err error) {
				if err == nil {
					r.log.WithField("msgID", msgID).Debug("Succeed to send message to DLQ")
					// The Producer ack might be coming from the connection go-routine that
					// is also used by the consumer. In that case we would get a dead-lock
					// if we'd try to ack.
					go cm.Consumer.AckID(msgID)
				} else {
					r.log.WithError(err).WithField("msgID", msgID).Debug("Failed to send message to DLQ")
					go cm.Consumer.Nack(cm)
				}
			})

		case <-r.closeCh:
			if r.producer != nil {
				r.producer.Close()
			}
			r.log.Debug("Closed DLQ router")
			return
		}
	}
}

func (r *dlqRouter) close() {
	// Attempt to write on the close channel, without blocking
	select {
	case r.closeCh <- nil:
	default:
	}
}

func (r *dlqRouter) getProducer(schema Schema) Producer {
	if r.producer != nil {
		// Producer was already initialized
		return r.producer
	}

	// Retry to create producer indefinitely
	backoff := &internal.DefaultBackoff{}
	for {
		opt := r.policy.ProducerOptions
		opt.Topic = r.policy.DeadLetterTopic
		opt.Schema = schema

		// the origin code sets to LZ4 compression with no options
		// so the new design allows compression type to be overwritten but still set lz4 by default
		if r.policy.ProducerOptions.CompressionType == NoCompression {
			opt.CompressionType = LZ4
		}
		producer, err := r.client.CreateProducer(opt)

		if err != nil {
			r.log.WithError(err).Error("Failed to create DLQ producer")
			time.Sleep(backoff.Next())
			continue
		} else {
			r.producer = producer
			return producer
		}
	}
}
