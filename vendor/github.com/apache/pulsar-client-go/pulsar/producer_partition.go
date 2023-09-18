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
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	internalcrypto "github.com/apache/pulsar-client-go/pulsar/internal/crypto"

	"google.golang.org/protobuf/proto"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"

	uAtomic "go.uber.org/atomic"
)

type producerState int32

const (
	// producer states
	producerInit = iota
	producerReady
	producerClosing
	producerClosed
)

var (
	errFailAddToBatch     = newError(AddToBatchFailed, "message add to batch failed")
	errSendTimeout        = newError(TimeoutError, "message send timeout")
	errSendQueueIsFull    = newError(ProducerQueueIsFull, "producer send queue is full")
	errContextExpired     = newError(TimeoutError, "message send context expired")
	errMessageTooLarge    = newError(MessageTooBig, "message size exceeds MaxMessageSize")
	errMetaTooLarge       = newError(InvalidMessage, "message metadata size exceeds MaxMessageSize")
	errProducerClosed     = newError(ProducerClosed, "producer already been closed")
	errMemoryBufferIsFull = newError(ClientMemoryBufferIsFull, "client memory buffer is full")

	buffersPool sync.Pool
)

var errTopicNotFount = "TopicNotFound"

type partitionProducer struct {
	state  uAtomic.Int32
	client *client
	topic  string
	log    log.Logger

	conn uAtomic.Value

	options                  *ProducerOptions
	producerName             string
	userProvidedProducerName bool
	producerID               uint64
	batchBuilder             internal.BatchBuilder
	sequenceIDGenerator      *uint64
	batchFlushTicker         *time.Ticker
	encryptor                internalcrypto.Encryptor
	compressionProvider      compression.Provider

	// Channel where app is posting messages to be published
	dataChan        chan *sendRequest
	cmdChan         chan interface{}
	connectClosedCh chan connectionClosed

	publishSemaphore internal.Semaphore
	pendingQueue     internal.BlockingQueue
	lastSequenceID   int64
	schemaInfo       *SchemaInfo
	partitionIdx     int32
	metrics          *internal.LeveledMetrics
	epoch            uint64
	schemaCache      *schemaCache
	topicEpoch       *uint64
}

type schemaCache struct {
	lock    sync.RWMutex
	schemas map[uint64][]byte
}

func newSchemaCache() *schemaCache {
	return &schemaCache{
		schemas: make(map[uint64][]byte),
	}
}

func (s *schemaCache) Put(schema *SchemaInfo, schemaVersion []byte) {
	s.lock.Lock()
	defer s.lock.Unlock()

	key := schema.hash()
	s.schemas[key] = schemaVersion
}

func (s *schemaCache) Get(schema *SchemaInfo) (schemaVersion []byte) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.schemas[schema.hash()]
}

func newPartitionProducer(client *client, topic string, options *ProducerOptions, partitionIdx int,
	metrics *internal.LeveledMetrics) (
	*partitionProducer, error) {
	var batchingMaxPublishDelay time.Duration
	if options.BatchingMaxPublishDelay != 0 {
		batchingMaxPublishDelay = options.BatchingMaxPublishDelay
	} else {
		batchingMaxPublishDelay = defaultBatchingMaxPublishDelay
	}

	var maxPendingMessages int
	if options.MaxPendingMessages == 0 {
		maxPendingMessages = 1000
	} else {
		maxPendingMessages = options.MaxPendingMessages
	}

	logger := client.log.SubLogger(log.Fields{"topic": topic})

	p := &partitionProducer{
		client:           client,
		topic:            topic,
		log:              logger,
		options:          options,
		producerID:       client.rpcClient.NewProducerID(),
		dataChan:         make(chan *sendRequest, maxPendingMessages),
		cmdChan:          make(chan interface{}, 10),
		connectClosedCh:  make(chan connectionClosed, 10),
		batchFlushTicker: time.NewTicker(batchingMaxPublishDelay),
		compressionProvider: internal.GetCompressionProvider(pb.CompressionType(options.CompressionType),
			compression.Level(options.CompressionLevel)),
		publishSemaphore: internal.NewSemaphore(int32(maxPendingMessages)),
		pendingQueue:     internal.NewBlockingQueue(maxPendingMessages),
		lastSequenceID:   -1,
		partitionIdx:     int32(partitionIdx),
		metrics:          metrics,
		epoch:            0,
		schemaCache:      newSchemaCache(),
	}
	if p.options.DisableBatching {
		p.batchFlushTicker.Stop()
	}
	p.setProducerState(producerInit)

	if options.Schema != nil && options.Schema.GetSchemaInfo() != nil {
		p.schemaInfo = options.Schema.GetSchemaInfo()
	} else {
		p.schemaInfo = nil
	}

	if options.Name != "" {
		p.producerName = options.Name
		p.userProvidedProducerName = true
	} else {
		p.userProvidedProducerName = false
	}
	err := p.grabCnx()
	if err != nil {
		p.batchFlushTicker.Stop()
		logger.WithError(err).Error("Failed to create producer at newPartitionProducer")
		return nil, err
	}

	p.log = p.log.SubLogger(log.Fields{
		"producer_name": p.producerName,
		"producerID":    p.producerID,
	})

	p.log.WithField("cnx", p._getConn().ID()).Info("Created producer")
	p.setProducerState(producerReady)

	if p.options.SendTimeout > 0 {
		go p.failTimeoutMessages()
	}

	go p.runEventsLoop()

	return p, nil
}

func (p *partitionProducer) grabCnx() error {
	lr, err := p.client.lookupService.Lookup(p.topic)
	if err != nil {
		p.log.WithError(err).Warn("Failed to lookup topic")
		return err
	}

	p.log.Debug("Lookup result: ", lr)
	id := p.client.rpcClient.NewRequestID()

	// set schema info for producer

	var pbSchema *pb.Schema
	if p.schemaInfo != nil {
		tmpSchemaType := pb.Schema_Type(int32(p.schemaInfo.Type))
		pbSchema = &pb.Schema{
			Name:       proto.String(p.schemaInfo.Name),
			Type:       &tmpSchemaType,
			SchemaData: []byte(p.schemaInfo.Schema),
			Properties: internal.ConvertFromStringMap(p.schemaInfo.Properties),
		}
		p.log.Debugf("The partition producer schema name is: %s", pbSchema.Name)
	} else {
		p.log.Debug("The partition producer schema is nil")
	}

	cmdProducer := &pb.CommandProducer{
		RequestId:                proto.Uint64(id),
		Topic:                    proto.String(p.topic),
		Encrypted:                nil,
		ProducerId:               proto.Uint64(p.producerID),
		Schema:                   pbSchema,
		Epoch:                    proto.Uint64(atomic.LoadUint64(&p.epoch)),
		UserProvidedProducerName: proto.Bool(p.userProvidedProducerName),
		ProducerAccessMode:       toProtoProducerAccessMode(p.options.ProducerAccessMode).Enum(),
	}

	if p.topicEpoch != nil {
		cmdProducer.TopicEpoch = proto.Uint64(*p.topicEpoch)
	}

	if p.producerName != "" {
		cmdProducer.ProducerName = proto.String(p.producerName)
	}

	if len(p.options.Properties) > 0 {
		cmdProducer.Metadata = toKeyValues(p.options.Properties)
	}
	res, err := p.client.rpcClient.Request(lr.LogicalAddr, lr.PhysicalAddr, id, pb.BaseCommand_PRODUCER, cmdProducer)
	if err != nil {
		p.log.WithError(err).Error("Failed to create producer at send PRODUCER request")
		return err
	}

	p.producerName = res.Response.ProducerSuccess.GetProducerName()
	nextTopicEpoch := res.Response.ProducerSuccess.GetTopicEpoch()
	p.topicEpoch = &nextTopicEpoch

	if p.options.Encryption != nil {
		p.encryptor = internalcrypto.NewProducerEncryptor(p.options.Encryption.Keys,
			p.options.Encryption.KeyReader,
			p.options.Encryption.MessageCrypto,
			p.options.Encryption.ProducerCryptoFailureAction, p.log)
	} else {
		p.encryptor = internalcrypto.NewNoopEncryptor()
	}

	if p.sequenceIDGenerator == nil {
		nextSequenceID := uint64(res.Response.ProducerSuccess.GetLastSequenceId() + 1)
		p.sequenceIDGenerator = &nextSequenceID
	}

	schemaVersion := res.Response.ProducerSuccess.GetSchemaVersion()
	if len(schemaVersion) != 0 {
		p.schemaCache.Put(p.schemaInfo, schemaVersion)
	}

	p._setConn(res.Cnx)
	err = p._getConn().RegisterListener(p.producerID, p)
	if err != nil {
		return err
	}

	if !p.options.DisableBatching && p.batchBuilder == nil {
		provider, err := GetBatcherBuilderProvider(p.options.BatcherBuilderType)
		if err != nil {
			return err
		}
		maxMessageSize := uint32(p._getConn().GetMaxMessageSize())
		p.batchBuilder, err = provider(p.options.BatchingMaxMessages, p.options.BatchingMaxSize,
			maxMessageSize, p.producerName, p.producerID, pb.CompressionType(p.options.CompressionType),
			compression.Level(p.options.CompressionLevel),
			p,
			p.log,
			p.encryptor)
		if err != nil {
			return err
		}
	}

	p.log.WithFields(log.Fields{
		"cnx":   res.Cnx.ID(),
		"epoch": atomic.LoadUint64(&p.epoch),
	}).Info("Connected producer")

	pendingItems := p.pendingQueue.ReadableSlice()
	viewSize := len(pendingItems)
	if viewSize > 0 {
		p.log.Infof("Resending %d pending batches", viewSize)
		lastViewItem := pendingItems[viewSize-1].(*pendingItem)

		// iterate at most pending items
		for i := 0; i < viewSize; i++ {
			item := p.pendingQueue.Poll()
			if item == nil {
				continue
			}
			pi := item.(*pendingItem)
			// when resending pending batches, we update the sendAt timestamp and put to the back of queue
			// to avoid pending item been removed by failTimeoutMessages and cause race condition
			pi.Lock()
			pi.sentAt = time.Now()
			pi.Unlock()
			p.pendingQueue.Put(pi)
			p._getConn().WriteData(pi.buffer)

			if pi == lastViewItem {
				break
			}
		}
	}
	return nil
}

type connectionClosed struct{}

func (p *partitionProducer) GetBuffer() internal.Buffer {
	b, ok := buffersPool.Get().(internal.Buffer)
	if ok {
		b.Clear()
	}
	return b
}

func (p *partitionProducer) ConnectionClosed() {
	// Trigger reconnection in the produce goroutine
	p.log.WithField("cnx", p._getConn().ID()).Warn("Connection was closed")
	p.connectClosedCh <- connectionClosed{}
}

func (p *partitionProducer) getOrCreateSchema(schemaInfo *SchemaInfo) (schemaVersion []byte, err error) {

	tmpSchemaType := pb.Schema_Type(int32(schemaInfo.Type))
	pbSchema := &pb.Schema{
		Name:       proto.String(schemaInfo.Name),
		Type:       &tmpSchemaType,
		SchemaData: []byte(schemaInfo.Schema),
		Properties: internal.ConvertFromStringMap(schemaInfo.Properties),
	}
	id := p.client.rpcClient.NewRequestID()
	req := &pb.CommandGetOrCreateSchema{
		RequestId: proto.Uint64(id),
		Topic:     proto.String(p.topic),
		Schema:    pbSchema,
	}
	res, err := p.client.rpcClient.RequestOnCnx(p._getConn(), id, pb.BaseCommand_GET_OR_CREATE_SCHEMA, req)
	if err != nil {
		return
	}
	if res.Response.Error != nil {
		err = errors.New(res.Response.GetError().String())
		return
	}
	if res.Response.GetOrCreateSchemaResponse.ErrorCode != nil {
		err = errors.New(*res.Response.GetOrCreateSchemaResponse.ErrorMessage)
		return
	}
	return res.Response.GetOrCreateSchemaResponse.SchemaVersion, nil
}

func (p *partitionProducer) reconnectToBroker() {
	var maxRetry int
	if p.options.MaxReconnectToBroker == nil {
		maxRetry = -1
	} else {
		maxRetry = int(*p.options.MaxReconnectToBroker)
	}

	for maxRetry != 0 {
		if p.getProducerState() != producerReady {
			// Producer is already closing
			p.log.Info("producer state not ready, exit reconnect")
			return
		}

		var (
			delayReconnectTime time.Duration
			defaultBackoff     = internal.DefaultBackoff{}
		)

		if p.options.BackoffPolicy == nil {
			delayReconnectTime = defaultBackoff.Next()
		} else {
			delayReconnectTime = p.options.BackoffPolicy.Next()
		}
		p.log.Info("Reconnecting to broker in ", delayReconnectTime)
		time.Sleep(delayReconnectTime)
		atomic.AddUint64(&p.epoch, 1)
		err := p.grabCnx()
		if err == nil {
			// Successfully reconnected
			p.log.WithField("cnx", p._getConn().ID()).Info("Reconnected producer to broker")
			return
		}
		p.log.WithError(err).Error("Failed to create producer at reconnect")
		errMsg := err.Error()
		if strings.Contains(errMsg, errTopicNotFount) {
			// when topic is deleted, we should give up reconnection.
			p.log.Warn("Topic Not Found.")
			break
		}

		if maxRetry > 0 {
			maxRetry--
		}
		p.metrics.ProducersReconnectFailure.Inc()
		if maxRetry == 0 || defaultBackoff.IsMaxBackoffReached() {
			p.metrics.ProducersReconnectMaxRetry.Inc()
		}
	}
}

func (p *partitionProducer) runEventsLoop() {
	for {
		select {
		case data := <-p.dataChan:
			p.internalSend(data)
		case i := <-p.cmdChan:
			switch v := i.(type) {
			case *flushRequest:
				p.internalFlush(v)
			case *closeProducer:
				p.internalClose(v)
				return
			}
		case <-p.connectClosedCh:
			p.log.Info("runEventsLoop will reconnect in producer")
			p.reconnectToBroker()
		case <-p.batchFlushTicker.C:
			p.internalFlushCurrentBatch()
		}
	}
}

func (p *partitionProducer) Topic() string {
	return p.topic
}

func (p *partitionProducer) Name() string {
	return p.producerName
}

func runCallback(cb func(MessageID, *ProducerMessage, error), id MessageID, msg *ProducerMessage, err error) {
	if cb == nil {
		return
	}

	cb(id, msg, err)
}

func (p *partitionProducer) internalSend(request *sendRequest) {
	p.log.Debug("Received send request: ", *request.msg)

	msg := request.msg

	// read payload from message
	uncompressedPayload := msg.Payload
	uncompressedPayloadSize := int64(len(uncompressedPayload))

	var schemaPayload []byte
	var err error
	if msg.Value != nil && msg.Payload != nil {
		p.log.Error("Can not set Value and Payload both")
		runCallback(request.callback, nil, request.msg, errors.New("can not set Value and Payload both"))
		return
	}

	// The block chan must be closed when returned with exception
	defer request.stopBlock()
	if !p.canAddToQueue(request, uncompressedPayloadSize) {
		return
	}

	if p.options.DisableMultiSchema {
		if msg.Schema != nil && p.options.Schema != nil &&
			msg.Schema.GetSchemaInfo().hash() != p.options.Schema.GetSchemaInfo().hash() {
			p.releaseSemaphoreAndMem(uncompressedPayloadSize)
			runCallback(request.callback, nil, request.msg, fmt.Errorf("msg schema can not match with producer schema"))
			p.log.WithError(err).Errorf("The producer %s of the topic %s is disabled the `MultiSchema`", p.producerName, p.topic)
			return
		}
	}
	var schema Schema
	var schemaVersion []byte
	if msg.Schema != nil {
		schema = msg.Schema
	} else if p.options.Schema != nil {
		schema = p.options.Schema
	}
	if msg.Value != nil {
		// payload and schema are mutually exclusive
		// try to get payload from schema value only if payload is not set
		if uncompressedPayload == nil && schema != nil {
			schemaPayload, err = schema.Encode(msg.Value)
			if err != nil {
				p.releaseSemaphoreAndMem(uncompressedPayloadSize)
				runCallback(request.callback, nil, request.msg, newError(SchemaFailure, err.Error()))
				p.log.WithError(err).Errorf("Schema encode message failed %s", msg.Value)
				return
			}
		}
	}
	if uncompressedPayload == nil {
		uncompressedPayload = schemaPayload
	}

	if schema != nil {
		schemaVersion = p.schemaCache.Get(schema.GetSchemaInfo())
		if schemaVersion == nil {
			schemaVersion, err = p.getOrCreateSchema(schema.GetSchemaInfo())
			if err != nil {
				p.releaseSemaphoreAndMem(uncompressedPayloadSize)
				p.log.WithError(err).Error("get schema version fail")
				runCallback(request.callback, nil, request.msg, fmt.Errorf("get schema version fail, err: %w", err))
				return
			}
			p.schemaCache.Put(schema.GetSchemaInfo(), schemaVersion)
		}
	}

	uncompressedSize := len(uncompressedPayload)

	deliverAt := msg.DeliverAt
	if msg.DeliverAfter.Nanoseconds() > 0 {
		deliverAt = time.Now().Add(msg.DeliverAfter)
	}

	mm := p.genMetadata(msg, uncompressedSize, deliverAt)

	// set default ReplicationClusters when DisableReplication
	if msg.DisableReplication {
		msg.ReplicationClusters = []string{"__local__"}
	}

	sendAsBatch := !p.options.DisableBatching &&
		msg.ReplicationClusters == nil &&
		deliverAt.UnixNano() < 0

	// Once the batching is enabled, it can close blockCh early to make block finish
	if sendAsBatch {
		request.stopBlock()
	} else {
		// update sequence id for metadata, make the size of msgMetadata more accurate
		// batch sending will update sequence ID in the BatchBuilder
		p.updateMetadataSeqID(mm, msg)
	}

	maxMessageSize := int(p._getConn().GetMaxMessageSize())

	// compress payload if not batching
	var compressedPayload []byte
	var compressedSize int
	var checkSize int
	if !sendAsBatch {
		compressedPayload = p.compressionProvider.Compress(nil, uncompressedPayload)
		compressedSize = len(compressedPayload)
		checkSize = compressedSize

		// set the compress type in msgMetaData
		compressionType := pb.CompressionType(p.options.CompressionType)
		if compressionType != pb.CompressionType_NONE {
			mm.Compression = &compressionType
		}
	} else {
		// final check for batching message is in serializeMessage
		// this is a double check
		checkSize = uncompressedSize
	}

	// if msg is too large and chunking is disabled
	if checkSize > maxMessageSize && !p.options.EnableChunking {
		p.releaseSemaphoreAndMem(uncompressedPayloadSize)
		runCallback(request.callback, nil, request.msg, errMessageTooLarge)
		p.log.WithError(errMessageTooLarge).
			WithField("size", checkSize).
			WithField("properties", msg.Properties).
			Errorf("MaxMessageSize %d", maxMessageSize)
		p.metrics.PublishErrorsMsgTooLarge.Inc()
		return
	}

	var totalChunks int
	// max chunk payload size
	var payloadChunkSize int
	if sendAsBatch || !p.options.EnableChunking {
		totalChunks = 1
		payloadChunkSize = int(p._getConn().GetMaxMessageSize())
	} else {
		payloadChunkSize = int(p._getConn().GetMaxMessageSize()) - proto.Size(mm)
		if payloadChunkSize <= 0 {
			p.releaseSemaphoreAndMem(uncompressedPayloadSize)
			runCallback(request.callback, nil, msg, errMetaTooLarge)
			p.log.WithError(errMetaTooLarge).
				WithField("metadata size", proto.Size(mm)).
				WithField("properties", msg.Properties).
				Errorf("MaxMessageSize %d", int(p._getConn().GetMaxMessageSize()))
			p.metrics.PublishErrorsMsgTooLarge.Inc()
			return
		}
		// set ChunkMaxMessageSize
		if p.options.ChunkMaxMessageSize != 0 {
			payloadChunkSize = int(math.Min(float64(payloadChunkSize), float64(p.options.ChunkMaxMessageSize)))
		}
		totalChunks = int(math.Max(1, math.Ceil(float64(compressedSize)/float64(payloadChunkSize))))
	}

	// set total chunks to send request
	request.totalChunks = totalChunks

	if !sendAsBatch {
		if totalChunks > 1 {
			var lhs, rhs int
			uuid := fmt.Sprintf("%s-%s", p.producerName, strconv.FormatUint(*mm.SequenceId, 10))
			mm.Uuid = proto.String(uuid)
			mm.NumChunksFromMsg = proto.Int32(int32(totalChunks))
			mm.TotalChunkMsgSize = proto.Int32(int32(compressedSize))
			cr := newChunkRecorder()
			for chunkID := 0; chunkID < totalChunks; chunkID++ {
				lhs = chunkID * payloadChunkSize
				if rhs = lhs + payloadChunkSize; rhs > compressedSize {
					rhs = compressedSize
				}
				// update chunk id
				mm.ChunkId = proto.Int32(int32(chunkID))
				nsr := &sendRequest{
					ctx:              request.ctx,
					msg:              request.msg,
					callback:         request.callback,
					callbackOnce:     request.callbackOnce,
					publishTime:      request.publishTime,
					blockCh:          request.blockCh,
					closeBlockChOnce: request.closeBlockChOnce,
					totalChunks:      totalChunks,
					chunkID:          chunkID,
					uuid:             uuid,
					chunkRecorder:    cr,
					transaction:      request.transaction,
				}
				// the permit of first chunk has acquired
				if chunkID != 0 && !p.canAddToQueue(nsr, 0) {
					p.releaseSemaphoreAndMem(uncompressedPayloadSize - int64(rhs))
					return
				}
				p.internalSingleSend(mm, compressedPayload[lhs:rhs], nsr, uint32(maxMessageSize))
			}
			// close the blockCh when all the chunks acquired permits
			request.stopBlock()
		} else {
			// close the blockCh when totalChunks is 1 (it has acquired permits)
			request.stopBlock()
			p.internalSingleSend(mm, compressedPayload, request, uint32(maxMessageSize))
		}
	} else {
		smm := p.genSingleMessageMetadataInBatch(msg, uncompressedSize)
		multiSchemaEnabled := !p.options.DisableMultiSchema
		added := addRequestToBatch(smm, p, uncompressedPayload, request, msg, deliverAt, schemaVersion,
			multiSchemaEnabled)
		if !added {
			// The current batch is full. flush it and retry

			p.internalFlushCurrentBatch()

			// after flushing try again to add the current payload
			if ok := addRequestToBatch(smm, p, uncompressedPayload, request, msg, deliverAt, schemaVersion,
				multiSchemaEnabled); !ok {
				p.releaseSemaphoreAndMem(uncompressedPayloadSize)
				runCallback(request.callback, nil, request.msg, errFailAddToBatch)
				p.log.WithField("size", uncompressedSize).
					WithField("properties", msg.Properties).
					Error("unable to add message to batch")
				return
			}
		}
		if request.flushImmediately {

			p.internalFlushCurrentBatch()

		}
	}
}

func addRequestToBatch(smm *pb.SingleMessageMetadata, p *partitionProducer,
	uncompressedPayload []byte,
	request *sendRequest, msg *ProducerMessage, deliverAt time.Time,
	schemaVersion []byte, multiSchemaEnabled bool) bool {
	var useTxn bool
	var mostSigBits uint64
	var leastSigBits uint64
	if request.transaction != nil {
		txnID := request.transaction.GetTxnID()
		useTxn = true
		mostSigBits = txnID.MostSigBits
		leastSigBits = txnID.LeastSigBits
	}

	return p.batchBuilder.Add(smm, p.sequenceIDGenerator, uncompressedPayload, request,
		msg.ReplicationClusters, deliverAt, schemaVersion, multiSchemaEnabled, useTxn, mostSigBits,
		leastSigBits)
}

func (p *partitionProducer) genMetadata(msg *ProducerMessage,
	uncompressedSize int,
	deliverAt time.Time) (mm *pb.MessageMetadata) {
	mm = &pb.MessageMetadata{
		ProducerName:     &p.producerName,
		PublishTime:      proto.Uint64(internal.TimestampMillis(time.Now())),
		ReplicateTo:      msg.ReplicationClusters,
		UncompressedSize: proto.Uint32(uint32(uncompressedSize)),
	}

	if !msg.EventTime.IsZero() {
		mm.EventTime = proto.Uint64(internal.TimestampMillis(msg.EventTime))
	}

	if msg.Key != "" {
		mm.PartitionKey = proto.String(msg.Key)
	}

	if len(msg.OrderingKey) != 0 {
		mm.OrderingKey = []byte(msg.OrderingKey)
	}

	if msg.Properties != nil {
		mm.Properties = internal.ConvertFromStringMap(msg.Properties)
	}

	if deliverAt.UnixNano() > 0 {
		mm.DeliverAtTime = proto.Int64(int64(internal.TimestampMillis(deliverAt)))
	}

	return
}

func (p *partitionProducer) updateMetadataSeqID(mm *pb.MessageMetadata, msg *ProducerMessage) {
	if msg.SequenceID != nil {
		mm.SequenceId = proto.Uint64(uint64(*msg.SequenceID))
	} else {
		mm.SequenceId = proto.Uint64(internal.GetAndAdd(p.sequenceIDGenerator, 1))
	}
}

func (p *partitionProducer) updateSingleMessageMetadataSeqID(smm *pb.SingleMessageMetadata, msg *ProducerMessage) {
	if msg.SequenceID != nil {
		smm.SequenceId = proto.Uint64(uint64(*msg.SequenceID))
	} else {
		smm.SequenceId = proto.Uint64(internal.GetAndAdd(p.sequenceIDGenerator, 1))
	}
}

func (p *partitionProducer) genSingleMessageMetadataInBatch(msg *ProducerMessage,
	uncompressedSize int) (smm *pb.SingleMessageMetadata) {
	smm = &pb.SingleMessageMetadata{
		PayloadSize: proto.Int32(int32(uncompressedSize)),
	}

	if !msg.EventTime.IsZero() {
		smm.EventTime = proto.Uint64(internal.TimestampMillis(msg.EventTime))
	}

	if msg.Key != "" {
		smm.PartitionKey = proto.String(msg.Key)
	}

	if len(msg.OrderingKey) != 0 {
		smm.OrderingKey = []byte(msg.OrderingKey)
	}

	if msg.Properties != nil {
		smm.Properties = internal.ConvertFromStringMap(msg.Properties)
	}

	p.updateSingleMessageMetadataSeqID(smm, msg)

	return
}

func (p *partitionProducer) internalSingleSend(mm *pb.MessageMetadata,
	compressedPayload []byte,
	request *sendRequest,
	maxMessageSize uint32) {
	msg := request.msg

	payloadBuf := internal.NewBuffer(len(compressedPayload))
	payloadBuf.Write(compressedPayload)

	buffer := p.GetBuffer()
	if buffer == nil {
		buffer = internal.NewBuffer(int(payloadBuf.ReadableBytes() * 3 / 2))
	}

	sid := *mm.SequenceId
	var useTxn bool
	var mostSigBits uint64
	var leastSigBits uint64

	if request.transaction != nil {
		txnID := request.transaction.GetTxnID()
		useTxn = true
		mostSigBits = txnID.MostSigBits
		leastSigBits = txnID.LeastSigBits
	}

	err := internal.SingleSend(
		buffer,
		p.producerID,
		sid,
		mm,
		payloadBuf,
		p.encryptor,
		maxMessageSize,
		useTxn,
		mostSigBits,
		leastSigBits,
	)

	if err != nil {
		runCallback(request.callback, nil, request.msg, err)
		p.releaseSemaphoreAndMem(int64(len(msg.Payload)))
		p.log.WithError(err).Errorf("Single message serialize failed %s", msg.Value)
		return
	}

	p.pendingQueue.Put(&pendingItem{
		sentAt:       time.Now(),
		buffer:       buffer,
		sequenceID:   sid,
		sendRequests: []interface{}{request},
	})
	p._getConn().WriteData(buffer)
}

type pendingItem struct {
	sync.Mutex
	buffer       internal.Buffer
	sequenceID   uint64
	sentAt       time.Time
	sendRequests []interface{}
	completed    bool
}

func (p *partitionProducer) internalFlushCurrentBatch() {
	if p.batchBuilder.IsMultiBatches() {
		p.internalFlushCurrentBatches()
		return
	}

	batchData, sequenceID, callbacks, err := p.batchBuilder.Flush()
	if batchData == nil {
		return
	}

	// error occurred in batch flush
	// report it using callback
	if err != nil {
		for _, cb := range callbacks {
			if sr, ok := cb.(*sendRequest); ok {
				runCallback(sr.callback, nil, sr.msg, err)
			}
		}
		if errors.Is(err, internal.ErrExceedMaxMessageSize) {
			p.log.WithError(errMessageTooLarge).
				Errorf("internal err: %s", err)
			p.metrics.PublishErrorsMsgTooLarge.Inc()
			return
		}
		return
	}

	p.pendingQueue.Put(&pendingItem{
		sentAt:       time.Now(),
		buffer:       batchData,
		sequenceID:   sequenceID,
		sendRequests: callbacks,
	})
	p._getConn().WriteData(batchData)
}

func (p *partitionProducer) failTimeoutMessages() {
	diff := func(sentAt time.Time) time.Duration {
		return p.options.SendTimeout - time.Since(sentAt)
	}

	t := time.NewTimer(p.options.SendTimeout)
	defer t.Stop()

	for range t.C {
		state := p.getProducerState()
		if state == producerClosing || state == producerClosed {
			return
		}

		item := p.pendingQueue.Peek()
		if item == nil {
			// pending queue is empty
			t.Reset(p.options.SendTimeout)
			continue
		}
		oldestItem := item.(*pendingItem)
		if nextWaiting := diff(oldestItem.sentAt); nextWaiting > 0 {
			// none of these pending messages have timed out, wait and retry
			t.Reset(nextWaiting)
			continue
		}

		// since pending queue is not thread safe because of there is no global iteration lock
		// to control poll from pending queue, current goroutine and connection receipt handler
		// iterate pending queue at the same time, this maybe a performance trade-off
		// see https://github.com/apache/pulsar-client-go/pull/301
		curViewItems := p.pendingQueue.ReadableSlice()
		viewSize := len(curViewItems)
		if viewSize <= 0 {
			// double check
			t.Reset(p.options.SendTimeout)
			continue
		}
		p.log.Infof("Failing %d messages on timeout %s", viewSize, p.options.SendTimeout)
		lastViewItem := curViewItems[viewSize-1].(*pendingItem)

		// iterate at most viewSize items
		for i := 0; i < viewSize; i++ {
			tickerNeedWaiting := time.Duration(0)
			item := p.pendingQueue.CompareAndPoll(
				func(m interface{}) bool {
					if m == nil {
						return false
					}

					pi := m.(*pendingItem)
					pi.Lock()
					defer pi.Unlock()
					if nextWaiting := diff(pi.sentAt); nextWaiting > 0 {
						// current and subsequent items not timeout yet, stop iterating
						tickerNeedWaiting = nextWaiting
						return false
					}
					return true
				})

			if item == nil {
				t.Reset(p.options.SendTimeout)
				break
			}

			if tickerNeedWaiting > 0 {
				t.Reset(tickerNeedWaiting)
				break
			}

			pi := item.(*pendingItem)
			pi.Lock()

			for _, i := range pi.sendRequests {
				sr := i.(*sendRequest)
				if sr.msg != nil {
					size := len(sr.msg.Payload)
					p.releaseSemaphoreAndMem(int64(size))
					p.metrics.MessagesPending.Dec()
					p.metrics.BytesPending.Sub(float64(size))
					p.metrics.PublishErrorsTimeout.Inc()
					p.log.WithError(errSendTimeout).
						WithField("size", size).
						WithField("properties", sr.msg.Properties)
				}

				if sr.callback != nil {
					sr.callbackOnce.Do(func() {
						runCallback(sr.callback, nil, sr.msg, errSendTimeout)
					})
				}
				if sr.transaction != nil {
					sr.transaction.endSendOrAckOp(nil)
				}
			}

			// flag the sending has completed with error, flush make no effect
			pi.Complete()
			pi.Unlock()

			// finally reached the last view item, current iteration ends
			if pi == lastViewItem {
				t.Reset(p.options.SendTimeout)
				break
			}
		}
	}
}

func (p *partitionProducer) internalFlushCurrentBatches() {
	batchesData, sequenceIDs, callbacks, errs := p.batchBuilder.FlushBatches()
	if batchesData == nil {
		return
	}

	for i := range batchesData {
		// error occurred in processing batch
		// report it using callback
		if errs[i] != nil {
			for _, cb := range callbacks[i] {
				if sr, ok := cb.(*sendRequest); ok {
					runCallback(sr.callback, nil, sr.msg, errs[i])
				}
			}
			if errors.Is(errs[i], internal.ErrExceedMaxMessageSize) {
				p.log.WithError(errMessageTooLarge).
					Errorf("internal err: %s", errs[i])
				p.metrics.PublishErrorsMsgTooLarge.Inc()
				return
			}
			continue
		}
		if batchesData[i] == nil {
			continue
		}
		p.pendingQueue.Put(&pendingItem{
			sentAt:       time.Now(),
			buffer:       batchesData[i],
			sequenceID:   sequenceIDs[i],
			sendRequests: callbacks[i],
		})
		p._getConn().WriteData(batchesData[i])
	}

}

func (p *partitionProducer) internalFlush(fr *flushRequest) {

	p.internalFlushCurrentBatch()

	pi, ok := p.pendingQueue.PeekLast().(*pendingItem)
	if !ok {
		close(fr.doneCh)
		return
	}

	// lock the pending request while adding requests
	// since the ReceivedSendReceipt func iterates over this list
	pi.Lock()
	defer pi.Unlock()

	if pi.completed {
		// The last item in the queue has been completed while we were
		// looking at it. It's safe at this point to assume that every
		// message enqueued before Flush() was called are now persisted
		close(fr.doneCh)
		return
	}

	sendReq := &sendRequest{
		msg: nil,
		callback: func(id MessageID, message *ProducerMessage, e error) {
			fr.err = e
			close(fr.doneCh)
		},
	}

	pi.sendRequests = append(pi.sendRequests, sendReq)
}

func (p *partitionProducer) Send(ctx context.Context, msg *ProducerMessage) (MessageID, error) {
	var err error
	var msgID MessageID

	// use atomic bool to avoid race
	isDone := uAtomic.NewBool(false)
	doneCh := make(chan struct{})

	p.internalSendAsync(ctx, msg, func(ID MessageID, message *ProducerMessage, e error) {
		if isDone.CAS(false, true) {
			err = e
			msgID = ID
			close(doneCh)
		}
	}, true)

	// wait for send request to finish
	<-doneCh

	return msgID, err
}

func (p *partitionProducer) SendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error)) {
	p.internalSendAsync(ctx, msg, callback, false)
}

func (p *partitionProducer) internalSendAsync(ctx context.Context, msg *ProducerMessage,
	callback func(MessageID, *ProducerMessage, error), flushImmediately bool) {
	// Register transaction operation to transaction and the transaction coordinator.
	var newCallback func(MessageID, *ProducerMessage, error)
	var txn *transaction
	if msg.Transaction != nil {
		transactionImpl := (msg.Transaction).(*transaction)
		txn = transactionImpl
		if transactionImpl.state != TxnOpen {
			p.log.WithField("state", transactionImpl.state).Error("Failed to send message" +
				" by a non-open transaction.")
			runCallback(callback, nil, msg, newError(InvalidStatus, "Failed to send message by a non-open transaction."))
			return
		}

		if err := transactionImpl.registerProducerTopic(p.topic); err != nil {
			runCallback(callback, nil, msg, err)
			return
		}
		if err := transactionImpl.registerSendOrAckOp(); err != nil {
			runCallback(callback, nil, msg, err)
		}
		newCallback = func(id MessageID, producerMessage *ProducerMessage, err error) {
			runCallback(callback, id, producerMessage, err)
			transactionImpl.endSendOrAckOp(err)
		}
	} else {
		newCallback = callback
	}
	if p.getProducerState() != producerReady {
		// Producer is closing
		runCallback(newCallback, nil, msg, errProducerClosed)
		return
	}

	// bc only works when DisableBlockIfQueueFull is false
	bc := make(chan struct{})

	// callbackOnce make sure the callback is only invoked once in chunking
	callbackOnce := &sync.Once{}
	sr := &sendRequest{
		ctx:              ctx,
		msg:              msg,
		callback:         newCallback,
		callbackOnce:     callbackOnce,
		flushImmediately: flushImmediately,
		publishTime:      time.Now(),
		blockCh:          bc,
		closeBlockChOnce: &sync.Once{},
		transaction:      txn,
	}
	p.options.Interceptors.BeforeSend(p, msg)

	p.dataChan <- sr

	if !p.options.DisableBlockIfQueueFull {
		// block if queue full
		<-bc
	}
}

func (p *partitionProducer) ReceivedSendReceipt(response *pb.CommandSendReceipt) {
	pi, ok := p.pendingQueue.Peek().(*pendingItem)

	if !ok {
		// if we receive a receipt although the pending queue is empty, the state of the broker and the producer differs.
		p.log.Warnf("Got ack %v for timed out msg", response.GetMessageId())
		return
	}

	if pi.sequenceID < response.GetSequenceId() {
		// Force connection closing so that messages can be re-transmitted in a new connection
		p.log.Warnf("Received ack for %v on sequenceId %v - expected: %v, closing connection", response.GetMessageId(),
			response.GetSequenceId(), pi.sequenceID)
		p._getConn().Close()
		return
	} else if pi.sequenceID > response.GetSequenceId() {
		// Ignoring the ack since it's referring to a message that has already timed out.
		p.log.Warnf("Received ack for %v on sequenceId %v - expected: %v, closing connection", response.GetMessageId(),
			response.GetSequenceId(), pi.sequenceID)
		return
	} else {
		// The ack was indeed for the expected item in the queue, we can remove it and trigger the callback
		p.pendingQueue.Poll()

		now := time.Now().UnixNano()

		// lock the pending item while sending the requests
		pi.Lock()
		defer pi.Unlock()
		p.metrics.PublishRPCLatency.Observe(float64(now-pi.sentAt.UnixNano()) / 1.0e9)
		batchSize := int32(0)
		for _, i := range pi.sendRequests {
			sr := i.(*sendRequest)
			if sr.msg != nil {
				batchSize = batchSize + 1
			} else { // Flush request
				break
			}
		}
		for idx, i := range pi.sendRequests {
			sr := i.(*sendRequest)
			if sr.msg != nil {
				atomic.StoreInt64(&p.lastSequenceID, int64(pi.sequenceID))
				p.releaseSemaphoreAndMem(int64(len(sr.msg.Payload)))
				p.metrics.PublishLatency.Observe(float64(now-sr.publishTime.UnixNano()) / 1.0e9)
				p.metrics.MessagesPublished.Inc()
				p.metrics.MessagesPending.Dec()
				payloadSize := float64(len(sr.msg.Payload))
				p.metrics.BytesPublished.Add(payloadSize)
				p.metrics.BytesPending.Sub(payloadSize)
			}

			if sr.callback != nil || len(p.options.Interceptors) > 0 {
				msgID := newMessageID(
					int64(response.MessageId.GetLedgerId()),
					int64(response.MessageId.GetEntryId()),
					int32(idx),
					p.partitionIdx,
					batchSize,
				)

				if sr.totalChunks > 1 {
					if sr.chunkID == 0 {
						sr.chunkRecorder.setFirstChunkID(
							&messageID{
								int64(response.MessageId.GetLedgerId()),
								int64(response.MessageId.GetEntryId()),
								-1,
								p.partitionIdx,
								0,
							})
					} else if sr.chunkID == sr.totalChunks-1 {
						sr.chunkRecorder.setLastChunkID(
							&messageID{
								int64(response.MessageId.GetLedgerId()),
								int64(response.MessageId.GetEntryId()),
								-1,
								p.partitionIdx,
								0,
							})
						// use chunkMsgID to set msgID
						msgID = &sr.chunkRecorder.chunkedMsgID
					}
				}

				if sr.totalChunks <= 1 || sr.chunkID == sr.totalChunks-1 {
					runCallback(sr.callback, msgID, sr.msg, nil)
					p.options.Interceptors.OnSendAcknowledgement(p, sr.msg, msgID)
				}
			}
			if sr.transaction != nil {
				sr.transaction.endSendOrAckOp(nil)
			}
		}

		// Mark this pending item as done
		pi.Complete()
	}
}

func (p *partitionProducer) internalClose(req *closeProducer) {
	defer close(req.doneCh)
	if !p.casProducerState(producerReady, producerClosing) {
		return
	}

	p.log.Info("Closing producer")

	id := p.client.rpcClient.NewRequestID()
	_, err := p.client.rpcClient.RequestOnCnx(p._getConn(), id, pb.BaseCommand_CLOSE_PRODUCER, &pb.CommandCloseProducer{
		ProducerId: &p.producerID,
		RequestId:  &id,
	})

	if err != nil {
		p.log.WithError(err).Warn("Failed to close producer")
	} else {
		p.log.Info("Closed producer")
	}

	if p.batchBuilder != nil {
		if err = p.batchBuilder.Close(); err != nil {
			p.log.WithError(err).Warn("Failed to close batch builder")
		}
	}

	p.setProducerState(producerClosed)
	p._getConn().UnregisterListener(p.producerID)
	p.batchFlushTicker.Stop()
}

func (p *partitionProducer) LastSequenceID() int64 {
	return atomic.LoadInt64(&p.lastSequenceID)
}

func (p *partitionProducer) Flush() error {
	flushReq := &flushRequest{
		doneCh: make(chan struct{}),
		err:    nil,
	}
	p.cmdChan <- flushReq

	// wait for the flush request to complete
	<-flushReq.doneCh
	return flushReq.err
}

func (p *partitionProducer) getProducerState() producerState {
	return producerState(p.state.Load())
}

func (p *partitionProducer) setProducerState(state producerState) {
	p.state.Swap(int32(state))
}

// set a new consumerState and return the last state
// returns bool if the new state has been set or not
func (p *partitionProducer) casProducerState(oldState, newState producerState) bool {
	return p.state.CAS(int32(oldState), int32(newState))
}

func (p *partitionProducer) Close() {
	if p.getProducerState() != producerReady {
		// Producer is closing
		return
	}

	cp := &closeProducer{doneCh: make(chan struct{})}
	p.cmdChan <- cp

	// wait for close producer request to complete
	<-cp.doneCh
}

type sendRequest struct {
	ctx              context.Context
	msg              *ProducerMessage
	callback         func(MessageID, *ProducerMessage, error)
	callbackOnce     *sync.Once
	publishTime      time.Time
	flushImmediately bool
	blockCh          chan struct{}
	closeBlockChOnce *sync.Once
	totalChunks      int
	chunkID          int
	uuid             string
	chunkRecorder    *chunkRecorder
	transaction      *transaction
}

// stopBlock can be invoked multiple times safety
func (sr *sendRequest) stopBlock() {
	sr.closeBlockChOnce.Do(func() {
		close(sr.blockCh)
	})
}

type closeProducer struct {
	doneCh chan struct{}
}

type flushRequest struct {
	doneCh chan struct{}
	err    error
}

func (i *pendingItem) Complete() {
	if i.completed {
		return
	}
	i.completed = true
	buffersPool.Put(i.buffer)
}

// _setConn sets the internal connection field of this partition producer atomically.
// Note: should only be called by this partition producer when a new connection is available.
func (p *partitionProducer) _setConn(conn internal.Connection) {
	p.conn.Store(conn)
}

// _getConn returns internal connection field of this partition producer atomically.
// Note: should only be called by this partition producer before attempting to use the connection
func (p *partitionProducer) _getConn() internal.Connection {
	// Invariant: The conn must be non-nil for the lifetime of the partitionProducer.
	//            For this reason we leave this cast unchecked and panic() if the
	//            invariant is broken
	return p.conn.Load().(internal.Connection)
}

func (p *partitionProducer) releaseSemaphoreAndMem(size int64) {
	p.publishSemaphore.Release()
	p.client.memLimit.ReleaseMemory(size)
}

func (p *partitionProducer) canAddToQueue(sr *sendRequest, uncompressedPayloadSize int64) bool {
	if p.options.DisableBlockIfQueueFull {
		if !p.publishSemaphore.TryAcquire() {
			runCallback(sr.callback, nil, sr.msg, errSendQueueIsFull)
			return false
		}
		if !p.client.memLimit.TryReserveMemory(uncompressedPayloadSize) {
			p.publishSemaphore.Release()
			runCallback(sr.callback, nil, sr.msg, errMemoryBufferIsFull)
			return false
		}

	} else {
		if !p.publishSemaphore.Acquire(sr.ctx) {
			runCallback(sr.callback, nil, sr.msg, errContextExpired)
			return false
		}
		if !p.client.memLimit.ReserveMemory(sr.ctx, uncompressedPayloadSize) {
			p.publishSemaphore.Release()
			runCallback(sr.callback, nil, sr.msg, errContextExpired)
			return false
		}
	}
	p.metrics.MessagesPending.Inc()
	p.metrics.BytesPending.Add(float64(len(sr.msg.Payload)))
	return true
}

type chunkRecorder struct {
	chunkedMsgID chunkMessageID
}

func newChunkRecorder() *chunkRecorder {
	return &chunkRecorder{
		chunkedMsgID: chunkMessageID{},
	}
}

func (c *chunkRecorder) setFirstChunkID(msgID *messageID) {
	c.chunkedMsgID.firstChunkID = msgID
}

func (c *chunkRecorder) setLastChunkID(msgID *messageID) {
	c.chunkedMsgID.messageID = msgID
}

func toProtoProducerAccessMode(accessMode ProducerAccessMode) pb.ProducerAccessMode {
	switch accessMode {
	case ProducerAccessModeShared:
		return pb.ProducerAccessMode_Shared
	case ProducerAccessModeExclusive:
		return pb.ProducerAccessMode_Exclusive
	case ProducerAccessModeWaitForExclusive:
		return pb.ProducerAccessMode_WaitForExclusive
	}

	return pb.ProducerAccessMode_Shared
}
