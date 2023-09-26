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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	pkgerrors "github.com/pkg/errors"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

const (
	defaultAutoDiscoveryDuration = 1 * time.Minute
)

type regexConsumer struct {
	client *client
	dlq    *dlqRouter
	rlq    *retryRouter

	options ConsumerOptions

	messageCh chan ConsumerMessage

	namespace string
	pattern   *regexp.Regexp

	consumersLock sync.Mutex
	consumers     map[string]Consumer
	subscribeCh   chan []string
	unsubscribeCh chan []string

	closeOnce sync.Once
	closeCh   chan struct{}

	ticker *time.Ticker

	log log.Logger

	consumerName string
}

func newRegexConsumer(c *client, opts ConsumerOptions, tn *internal.TopicName, pattern *regexp.Regexp,
	msgCh chan ConsumerMessage, dlq *dlqRouter, rlq *retryRouter) (Consumer, error) {
	rc := &regexConsumer{
		client:    c,
		dlq:       dlq,
		rlq:       rlq,
		options:   opts,
		messageCh: msgCh,

		namespace: tn.Namespace,
		pattern:   pattern,

		consumers:     make(map[string]Consumer),
		subscribeCh:   make(chan []string, 1),
		unsubscribeCh: make(chan []string, 1),

		closeCh: make(chan struct{}),

		log:          c.log.SubLogger(log.Fields{"topic": tn.Name}),
		consumerName: opts.Name,
	}

	topics, err := rc.topics()
	if err != nil {
		return nil, err
	}

	var errs error
	for ce := range subscriber(c, topics, opts, msgCh, dlq, rlq) {
		if ce.err != nil {
			errs = pkgerrors.Wrapf(ce.err, "unable to subscribe to topic=%s", ce.topic)
		} else {
			rc.consumers[ce.topic] = ce.consumer
		}
	}

	if errs != nil {
		for _, c := range rc.consumers {
			c.Close()
		}
		return nil, errs
	}

	// set up timer
	duration := opts.AutoDiscoveryPeriod
	if duration <= 0 {
		duration = defaultAutoDiscoveryDuration
	}
	rc.ticker = time.NewTicker(duration)

	go rc.monitor()

	return rc, nil
}

func (c *regexConsumer) Subscription() string {
	return c.options.SubscriptionName
}

func (c *regexConsumer) Unsubscribe() error {
	var errs error
	c.consumersLock.Lock()
	defer c.consumersLock.Unlock()

	for topic, consumer := range c.consumers {
		if err := consumer.Unsubscribe(); err != nil {
			msg := fmt.Sprintf("unable to unsubscribe from topic=%s subscription=%s",
				topic, c.Subscription())
			errs = pkgerrors.Wrap(err, msg)
		}
	}
	return errs
}

func (c *regexConsumer) Receive(ctx context.Context) (message Message, err error) {
	for {
		select {
		case <-c.closeCh:
			return nil, newError(ConsumerClosed, "consumer closed")
		case cm, ok := <-c.messageCh:
			if !ok {
				return nil, newError(ConsumerClosed, "consumer closed")
			}
			return cm.Message, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Chan return the messages chan to user
func (c *regexConsumer) Chan() <-chan ConsumerMessage {
	return c.messageCh
}

// Ack the consumption of a single message
func (c *regexConsumer) Ack(msg Message) error {
	return c.AckID(msg.ID())
}

func (c *regexConsumer) ReconsumeLater(msg Message, delay time.Duration) {
	c.log.Warnf("regexp consumer not support ReconsumeLater yet.")
}

func (c *regexConsumer) ReconsumeLaterWithCustomProperties(msg Message, customProperties map[string]string,
	delay time.Duration) {
	c.log.Warnf("regexp consumer not support ReconsumeLaterWithCustomProperties yet.")
}

// AckID the consumption of a single message, identified by its MessageID
func (c *regexConsumer) AckID(msgID MessageID) error {
	if !checkMessageIDType(msgID) {
		c.log.Warnf("invalid message id type %T", msgID)
		return fmt.Errorf("invalid message id type %T", msgID)
	}

	mid := toTrackingMessageID(msgID)

	if mid.consumer == nil {
		c.log.Warnf("unable to ack messageID=%+v can not determine topic", msgID)
		return errors.New("consumer is nil in consumer_regex")
	}

	if c.options.AckWithResponse {
		return mid.consumer.AckIDWithResponse(msgID)
	}

	return mid.consumer.AckID(msgID)
}

// AckID the consumption of a single message, identified by its MessageID
func (c *regexConsumer) AckWithTxn(msg Message, txn Transaction) error {
	msgID := msg.ID()
	if !checkMessageIDType(msgID) {
		c.log.Warnf("invalid message id type %T", msgID)
		return fmt.Errorf("invalid message id type %T", msgID)
	}

	mid := toTrackingMessageID(msgID)

	if mid.consumer == nil {
		c.log.Warnf("unable to ack messageID=%+v can not determine topic", msgID)
		return errors.New("consumer is nil in consumer_regex")
	}

	return mid.consumer.AckIDWithTxn(msgID, txn)
}

// AckCumulative the reception of all the messages in the stream up to (and including)
// the provided message.
func (c *regexConsumer) AckCumulative(msg Message) error {
	return c.AckIDCumulative(msg.ID())
}

// AckIDCumulative the reception of all the messages in the stream up to (and including)
// the provided message, identified by its MessageID
func (c *regexConsumer) AckIDCumulative(msgID MessageID) error {
	if !checkMessageIDType(msgID) {
		c.log.Warnf("invalid message id type %T", msgID)
		return fmt.Errorf("invalid message id type %T", msgID)
	}

	mid := toTrackingMessageID(msgID)

	if mid.consumer == nil {
		c.log.Warnf("unable to ack messageID=%+v can not determine topic", msgID)
		return errors.New("unable to ack message because consumer is nil")
	}

	if c.options.AckWithResponse {
		return mid.consumer.AckIDWithResponseCumulative(msgID)
	}

	return mid.consumer.AckIDCumulative(msgID)
}

func (c *regexConsumer) Nack(msg Message) {
	if c.options.EnableDefaultNackBackoffPolicy || c.options.NackBackoffPolicy != nil {
		msgID := msg.ID()
		if !checkMessageIDType(msgID) {
			c.log.Warnf("invalid message id type %T", msgID)
			return
		}
		mid := toTrackingMessageID(msgID)

		if mid.consumer == nil {
			c.log.Warnf("unable to nack messageID=%+v can not determine topic", msgID)
			return
		}
		mid.NackByMsg(msg)
		return
	}

	c.NackID(msg.ID())
}

func (c *regexConsumer) NackID(msgID MessageID) {
	if !checkMessageIDType(msgID) {
		c.log.Warnf("invalid message id type %T", msgID)
		return
	}

	mid := toTrackingMessageID(msgID)

	if mid.consumer == nil {
		c.log.Warnf("unable to nack messageID=%+v can not determine topic", msgID)
		return
	}

	mid.consumer.NackID(msgID)
}

func (c *regexConsumer) Close() {
	c.closeOnce.Do(func() {
		c.ticker.Stop()
		close(c.closeCh)

		var wg sync.WaitGroup
		c.consumersLock.Lock()
		defer c.consumersLock.Unlock()
		wg.Add(len(c.consumers))
		for _, con := range c.consumers {
			go func(consumer Consumer) {
				defer wg.Done()
				consumer.Close()
			}(con)
		}
		wg.Wait()
		c.client.handlers.Del(c)
		c.dlq.close()
		c.rlq.close()
	})
}

func (c *regexConsumer) Seek(msgID MessageID) error {
	return newError(SeekFailed, "seek command not allowed for regex consumer")
}

func (c *regexConsumer) SeekByTime(time time.Time) error {
	return newError(SeekFailed, "seek command not allowed for regex consumer")
}

// Name returns the name of consumer.
func (c *regexConsumer) Name() string {
	return c.consumerName
}

func (c *regexConsumer) closed() bool {
	select {
	case <-c.closeCh:
		return true
	default:
		return false
	}
}

func (c *regexConsumer) monitor() {
	for {
		select {
		case <-c.closeCh:
			return
		case <-c.ticker.C:
			c.log.Debug("Auto discovering topics")
			if !c.closed() {
				c.discover()
			}
		case topics := <-c.subscribeCh:
			if len(topics) > 0 && !c.closed() {
				c.subscribe(topics, c.dlq, c.rlq)
			}
		case topics := <-c.unsubscribeCh:
			if len(topics) > 0 && !c.closed() {
				c.unsubscribe(topics)
			}
		}
	}
}

func (c *regexConsumer) discover() {
	topics, err := c.topics()
	if err != nil {
		c.log.WithError(err).Errorf("Failed to discover topics")
		return
	}
	known := c.knownTopics()
	newTopics := topicsDiff(topics, known)
	staleTopics := topicsDiff(known, topics)

	c.log.
		WithFields(log.Fields{
			"new_topics": newTopics,
			"old_topics": staleTopics,
		}).
		Debug("discover topics")

	c.unsubscribeCh <- staleTopics
	c.subscribeCh <- newTopics
}

func (c *regexConsumer) knownTopics() []string {
	c.consumersLock.Lock()
	defer c.consumersLock.Unlock()
	topics := make([]string, len(c.consumers))
	n := 0
	for t := range c.consumers {
		topics[n] = t
		n++
	}

	return topics
}

func (c *regexConsumer) subscribe(topics []string, dlq *dlqRouter, rlq *retryRouter) {
	c.log.WithField("topics", topics).Debug("subscribe")
	consumers := make(map[string]Consumer, len(topics))
	for ce := range subscriber(c.client, topics, c.options, c.messageCh, dlq, rlq) {
		if ce.err != nil {
			c.log.Warnf("Failed to subscribe to topic=%s", ce.topic)
		} else {
			consumers[ce.topic] = ce.consumer
		}
	}

	c.consumersLock.Lock()
	defer c.consumersLock.Unlock()
	for t, consumer := range consumers {
		c.consumers[t] = consumer
	}
}

func (c *regexConsumer) unsubscribe(topics []string) {
	c.log.WithField("topics", topics).Debug("unsubscribe")

	consumers := make(map[string]Consumer, len(topics))
	c.consumersLock.Lock()

	for _, t := range topics {
		if consumer, ok := c.consumers[t]; ok {
			consumers[t] = consumer
			delete(c.consumers, t)
		}
	}
	c.consumersLock.Unlock()

	for t, consumer := range consumers {
		c.log.Debugf("unsubscribe from topic=%s subscription=%s", t, c.options.SubscriptionName)
		if err := consumer.Unsubscribe(); err != nil {
			c.log.Warnf("unable to unsubscribe from topic=%s subscription=%s",
				t, c.options.SubscriptionName)
		}
		consumer.Close()
	}
}

func (c *regexConsumer) topics() ([]string, error) {
	topics, err := c.client.lookupService.GetTopicsOfNamespace(c.namespace, internal.Persistent)
	if err != nil {
		return nil, err
	}

	filtered := filterTopics(topics, c.pattern)
	return filtered, nil
}

type consumerError struct {
	err      error
	topic    string
	consumer Consumer
}

func subscriber(c *client, topics []string, opts ConsumerOptions, ch chan ConsumerMessage,
	dlq *dlqRouter, rlq *retryRouter) <-chan consumerError {
	consumerErrorCh := make(chan consumerError, len(topics))
	var wg sync.WaitGroup
	wg.Add(len(topics))
	go func() {
		wg.Wait()
		close(consumerErrorCh)
	}()

	for _, t := range topics {
		go func(topic string) {
			defer wg.Done()
			c, err := newInternalConsumer(c, opts, topic, ch, dlq, rlq, true)
			consumerErrorCh <- consumerError{
				err:      err,
				topic:    topic,
				consumer: c,
			}
		}(t)
	}

	return consumerErrorCh
}

func filterTopics(topics []string, regex *regexp.Regexp) []string {
	matches := make(map[string]bool)
	matching := make([]string, 0)
	for _, t := range topics {
		tn, _ := internal.ParseTopicName(t)
		topic := internal.TopicNameWithoutPartitionPart(tn)
		if _, ok := matches[topic]; ok {
			continue
		}
		if regex.MatchString(topic) {
			matches[topic] = true
			matching = append(matching, topic)
		}
	}

	return matching
}

// topicDiff returns all topics in topics1 that are not in topics2
func topicsDiff(topics1 []string, topics2 []string) []string {
	if len(topics2) == 0 {
		return topics1
	}
	diff := make([]string, 0)
	topics := make(map[string]bool, len(topics2))
	for _, t := range topics2 {
		topics[t] = true
	}

	for _, t := range topics1 {
		if !topics[t] {
			diff = append(diff, t)
		}
	}

	return diff
}

func extractTopicPattern(tn *internal.TopicName) (*regexp.Regexp, error) {
	idx := strings.Index(tn.Name, tn.Namespace)
	if idx > 0 {
		return regexp.Compile(tn.Name[idx:])
	}

	return regexp.Compile(tn.Name)
}
