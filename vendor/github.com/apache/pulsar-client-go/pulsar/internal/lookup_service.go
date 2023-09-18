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
	"errors"
	"fmt"
	"net/url"

	"google.golang.org/protobuf/proto"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

// LookupResult encapsulates a struct for lookup a request, containing two parts: LogicalAddr, PhysicalAddr.
type LookupResult struct {
	LogicalAddr  *url.URL
	PhysicalAddr *url.URL
}

// GetTopicsOfNamespaceMode for CommandGetTopicsOfNamespace_Mode
type GetTopicsOfNamespaceMode string

const (
	Persistent    GetTopicsOfNamespaceMode = "PERSISTENT"
	NonPersistent                          = "NON_PERSISTENT"
	All                                    = "ALL"
)

// PartitionedTopicMetadata encapsulates a struct for metadata of a partitioned topic
type PartitionedTopicMetadata struct {
	Partitions int `json:"partitions"` // Number of partitions for the topic
}

// LookupService is a interface of lookup service.
type LookupService interface {
	// Lookup perform a lookup for the given topic, confirm the location of the broker
	// where the topic is located, and return the LookupResult.
	Lookup(topic string) (*LookupResult, error)

	// GetPartitionedTopicMetadata perform a CommandPartitionedTopicMetadata request for
	// the given topic, returns the CommandPartitionedTopicMetadataResponse as the result.
	GetPartitionedTopicMetadata(topic string) (*PartitionedTopicMetadata, error)

	// GetTopicsOfNamespace returns all the topics name for a given namespace.
	GetTopicsOfNamespace(namespace string, mode GetTopicsOfNamespaceMode) ([]string, error)

	// GetSchema returns schema for a given version.
	GetSchema(topic string, schemaVersion []byte) (schema *pb.Schema, err error)

	// Closable Allow Lookup Service's internal client to be able to closed
	Closable
}

type lookupService struct {
	rpcClient           RPCClient
	serviceNameResolver ServiceNameResolver
	tlsEnabled          bool
	listenerName        string
	log                 log.Logger
	metrics             *Metrics
}

// NewLookupService init a lookup service struct and return an object of LookupService.
func NewLookupService(rpcClient RPCClient, serviceURL *url.URL, serviceNameResolver ServiceNameResolver,
	tlsEnabled bool, listenerName string, logger log.Logger, metrics *Metrics) LookupService {
	return &lookupService{
		rpcClient:           rpcClient,
		serviceNameResolver: serviceNameResolver,
		tlsEnabled:          tlsEnabled,
		log:                 logger.SubLogger(log.Fields{"serviceURL": serviceURL}),
		metrics:             metrics,
		listenerName:        listenerName,
	}
}

func (ls *lookupService) GetSchema(topic string, schemaVersion []byte) (schema *pb.Schema, err error) {
	id := ls.rpcClient.NewRequestID()
	req := &pb.CommandGetSchema{
		RequestId:     proto.Uint64(id),
		Topic:         proto.String(topic),
		SchemaVersion: schemaVersion,
	}
	res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_GET_SCHEMA, req)
	if err != nil {
		return nil, err
	}
	if res.Response.Error != nil {
		return nil, errors.New(res.Response.GetError().String())
	}
	return res.Response.GetSchemaResponse.Schema, nil
}

func (ls *lookupService) getBrokerAddress(lr *pb.CommandLookupTopicResponse) (logicalAddress *url.URL,
	physicalAddress *url.URL, err error) {
	if ls.tlsEnabled {
		logicalAddress, err = url.ParseRequestURI(lr.GetBrokerServiceUrlTls())
	} else {
		logicalAddress, err = url.ParseRequestURI(lr.GetBrokerServiceUrl())
	}

	if err != nil {
		return nil, nil, err
	}

	var physicalAddr *url.URL
	if lr.GetProxyThroughServiceUrl() {
		physicalAddr, err = ls.serviceNameResolver.ResolveHost()
		if err != nil {
			return nil, nil, err
		}
	} else {
		physicalAddr = logicalAddress
	}

	return logicalAddress, physicalAddr, nil
}

// Follow brokers redirect up to certain number of times
const lookupResultMaxRedirect = 20

func (ls *lookupService) Lookup(topic string) (*LookupResult, error) {
	ls.metrics.LookupRequestsCount.Inc()
	id := ls.rpcClient.NewRequestID()
	res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_LOOKUP, &pb.CommandLookupTopic{
		RequestId:              &id,
		Topic:                  &topic,
		Authoritative:          proto.Bool(false),
		AdvertisedListenerName: proto.String(ls.listenerName),
	})
	if err != nil {
		return nil, err
	}
	ls.log.Debugf("Got topic{%s} lookup response: %+v", topic, res)

	for i := 0; i < lookupResultMaxRedirect; i++ {
		lr := res.Response.LookupTopicResponse
		switch *lr.Response {

		case pb.CommandLookupTopicResponse_Redirect:
			logicalAddress, physicalAddr, err := ls.getBrokerAddress(lr)
			if err != nil {
				return nil, err
			}

			ls.log.Debugf("Follow topic{%s} redirect to broker. %v / %v - Use proxy: %v",
				topic, lr.BrokerServiceUrl, lr.BrokerServiceUrlTls, lr.ProxyThroughServiceUrl)

			id := ls.rpcClient.NewRequestID()
			res, err = ls.rpcClient.Request(logicalAddress, physicalAddr, id, pb.BaseCommand_LOOKUP, &pb.CommandLookupTopic{
				RequestId:              &id,
				Topic:                  &topic,
				Authoritative:          lr.Authoritative,
				AdvertisedListenerName: proto.String(ls.listenerName),
			})
			if err != nil {
				return nil, err
			}

			// Process the response at the top of the loop
			continue

		case pb.CommandLookupTopicResponse_Connect:
			ls.log.Debugf("Successfully looked up topic{%s} on broker. %s / %s - Use proxy: %t",
				topic, lr.GetBrokerServiceUrl(), lr.GetBrokerServiceUrlTls(), lr.GetProxyThroughServiceUrl())

			logicalAddress, physicalAddress, err := ls.getBrokerAddress(lr)
			if err != nil {
				return nil, err
			}

			return &LookupResult{
				LogicalAddr:  logicalAddress,
				PhysicalAddr: physicalAddress,
			}, nil

		case pb.CommandLookupTopicResponse_Failed:
			ls.log.WithFields(log.Fields{
				"topic":   topic,
				"error":   lr.GetError(),
				"message": lr.GetMessage(),
			}).Warn("Failed to lookup topic")
			return nil, errors.New(lr.GetError().String())
		}
	}

	return nil, errors.New("exceeded max number of redirection during topic lookup")
}

func (ls *lookupService) GetPartitionedTopicMetadata(topic string) (*PartitionedTopicMetadata,
	error) {
	ls.metrics.PartitionedTopicMetadataRequestsCount.Inc()
	topicName, err := ParseTopicName(topic)
	if err != nil {
		return nil, err
	}

	id := ls.rpcClient.NewRequestID()
	res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_PARTITIONED_METADATA,
		&pb.CommandPartitionedTopicMetadata{
			RequestId: &id,
			Topic:     &topicName.Name,
		})
	if err != nil {
		return nil, err
	}
	ls.log.Debugf("Got topic{%s} partitioned metadata response: %+v", topic, res)

	var partitionedTopicMetadata PartitionedTopicMetadata

	if res.Response.Error != nil {
		return nil, errors.New(res.Response.GetError().String())
	}

	if res.Response.PartitionMetadataResponse != nil {
		if res.Response.PartitionMetadataResponse.Error != nil {
			return nil, errors.New(res.Response.PartitionMetadataResponse.GetError().String())
		}

		partitionedTopicMetadata.Partitions = int(res.Response.PartitionMetadataResponse.GetPartitions())
	} else {
		return nil, fmt.Errorf("no partitioned metadata for topic{%s} in lookup response", topic)
	}

	return &partitionedTopicMetadata, nil
}

func (ls *lookupService) GetTopicsOfNamespace(namespace string, mode GetTopicsOfNamespaceMode) ([]string, error) {
	id := ls.rpcClient.NewRequestID()
	pbMode := pb.CommandGetTopicsOfNamespace_Mode(pb.CommandGetTopicsOfNamespace_Mode_value[string(mode)])
	req := &pb.CommandGetTopicsOfNamespace{
		RequestId: proto.Uint64(id),
		Namespace: proto.String(namespace),
		Mode:      &pbMode,
	}
	res, err := ls.rpcClient.RequestToAnyBroker(id, pb.BaseCommand_GET_TOPICS_OF_NAMESPACE, req)
	if err != nil {
		return nil, err
	}
	if res.Response.Error != nil {
		return []string{}, errors.New(res.Response.GetError().String())
	}

	return res.Response.GetTopicsOfNamespaceResponse.GetTopics(), nil
}

func (ls *lookupService) Close() {}

const HTTPLookupServiceBasePathV1 string = "/lookup/v2/destination/"
const HTTPLookupServiceBasePathV2 string = "/lookup/v2/topic/"
const HTTPAdminServiceV1Format string = "/admin/%s/partitions"
const HTTPAdminServiceV2Format string = "/admin/v2/%s/partitions"
const HTTPTopicUnderNamespaceV1 string = "/admin/namespaces/%s/destinations?mode=%s"
const HTTPTopicUnderNamespaceV2 string = "/admin/v2/namespaces/%s/topics?mode=%s"

type httpLookupData struct {
	BrokerURL    string `json:"brokerUrl"`
	BrokerURLTLS string `json:"brokerUrlTls"`
	HTTPURL      string `json:"httpUrl"`
	HTTPURLTLS   string `json:"httpUrlTls"`
}

type httpLookupService struct {
	httpClient          HTTPClient
	serviceNameResolver ServiceNameResolver
	tlsEnabled          bool
	log                 log.Logger
	metrics             *Metrics
}

func (h *httpLookupService) getBrokerAddress(ld *httpLookupData) (logicalAddress *url.URL,
	physicalAddress *url.URL, err error) {
	if h.tlsEnabled {
		logicalAddress, err = url.ParseRequestURI(ld.BrokerURLTLS)
	} else {
		logicalAddress, err = url.ParseRequestURI(ld.BrokerURL)
	}

	if err != nil {
		return nil, nil, err
	}

	return logicalAddress, logicalAddress, nil
}

func (h *httpLookupService) Lookup(topic string) (*LookupResult, error) {
	topicName, err := ParseTopicName(topic)
	if err != nil {
		return nil, err
	}

	basePath := HTTPLookupServiceBasePathV2
	if !IsV2TopicName(topicName) {
		basePath = HTTPLookupServiceBasePathV1
	}

	lookupData := &httpLookupData{}
	err = h.httpClient.Get(basePath+GetTopicRestPath(topicName), lookupData, nil)
	if err != nil {
		return nil, err
	}

	h.log.Debugf("Successfully looked up topic{%s} on http broker. %+v",
		topic, lookupData)

	logicalAddress, physicalAddress, err := h.getBrokerAddress(lookupData)
	if err != nil {
		return nil, err
	}

	return &LookupResult{
		LogicalAddr:  logicalAddress,
		PhysicalAddr: physicalAddress,
	}, nil

}

func (h *httpLookupService) GetPartitionedTopicMetadata(topic string) (*PartitionedTopicMetadata,
	error) {
	topicName, err := ParseTopicName(topic)
	if err != nil {
		return nil, err
	}

	format := HTTPAdminServiceV2Format
	if !IsV2TopicName(topicName) {
		format = HTTPAdminServiceV1Format
	}

	path := fmt.Sprintf(format, GetTopicRestPath(topicName))

	tMetadata := &PartitionedTopicMetadata{}

	err = h.httpClient.Get(path, tMetadata, map[string]string{"checkAllowAutoCreation": "true"})
	if err != nil {
		return nil, err
	}

	h.log.Debugf("Got topic{%s} partitioned metadata response: %+v", topic, tMetadata)

	return tMetadata, nil
}

func (h *httpLookupService) GetTopicsOfNamespace(namespace string, mode GetTopicsOfNamespaceMode) ([]string, error) {

	format := HTTPTopicUnderNamespaceV2
	if !IsV2Namespace(namespace) {
		format = HTTPTopicUnderNamespaceV1
	}

	path := fmt.Sprintf(format, namespace, string(mode))

	topics := []string{}

	err := h.httpClient.Get(path, &topics, nil)
	if err != nil {
		return nil, err
	}

	h.log.Debugf("Got namespace{%s} mode{%s} topics response: %+v", namespace, mode, topics)

	return topics, nil
}

func (h *httpLookupService) GetSchema(topic string, schemaVersion []byte) (schema *pb.Schema, err error) {
	return nil, errors.New("GetSchema is not supported by httpLookupService")
}
func (h *httpLookupService) Close() {
	h.httpClient.Close()
}

// NewHTTPLookupService init a http based lookup service struct and return an object of LookupService.
func NewHTTPLookupService(httpClient HTTPClient, serviceURL *url.URL, serviceNameResolver ServiceNameResolver,
	tlsEnabled bool, logger log.Logger, metrics *Metrics) LookupService {

	return &httpLookupService{
		httpClient:          httpClient,
		serviceNameResolver: serviceNameResolver,
		tlsEnabled:          tlsEnabled,
		log:                 logger.SubLogger(log.Fields{"serviceURL": serviceURL}),
		metrics:             metrics,
	}
}
