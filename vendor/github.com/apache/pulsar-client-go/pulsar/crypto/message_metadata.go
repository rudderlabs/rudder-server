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

package crypto

import (
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

// MessageMetadataSupplier wrapper implementation around message metadata
type MessageMetadataSupplier interface {
	// EncryptionKeys read all the encryption keys from the MessageMetadata
	EncryptionKeys() []EncryptionKeyInfo

	// UpsertEncryptionKey add new or update existing EncryptionKeys in to the MessageMetadata
	UpsertEncryptionKey(EncryptionKeyInfo)

	// EncryptionParam read the ecryption parameter from the MessageMetadata
	EncryptionParam() []byte

	// SetEncryptionParam set encryption parameter in to the MessageMetadata
	SetEncryptionParam([]byte)
}

type MessageMetadata struct {
	messageMetadata *pb.MessageMetadata
}

func NewMessageMetadataSupplier(messageMetadata *pb.MessageMetadata) MessageMetadataSupplier {
	return &MessageMetadata{
		messageMetadata: messageMetadata,
	}
}

func (m *MessageMetadata) EncryptionKeys() []EncryptionKeyInfo {
	if m.messageMetadata != nil {
		encInfo := []EncryptionKeyInfo{}
		for _, k := range m.messageMetadata.EncryptionKeys {
			key := NewEncryptionKeyInfo(k.GetKey(), k.GetValue(), getKeyMetaMap(k.GetMetadata()))
			encInfo = append(encInfo, *key)
		}
		return encInfo
	}
	return nil
}

func (m *MessageMetadata) UpsertEncryptionKey(keyInfo EncryptionKeyInfo) {
	if m.messageMetadata != nil {
		idx := m.encryptionKeyPresent(keyInfo)
		newKey := &pb.EncryptionKeys{
			Key:      &keyInfo.name,
			Value:    keyInfo.Key(),
			Metadata: getKeyMeta(keyInfo.Metadata()),
		}

		if idx >= 0 {
			m.messageMetadata.EncryptionKeys[idx] = newKey
		} else {
			m.messageMetadata.EncryptionKeys = append(m.messageMetadata.EncryptionKeys, newKey)
		}
	}
}

func (m *MessageMetadata) EncryptionParam() []byte {
	if m.messageMetadata != nil {
		return m.messageMetadata.EncryptionParam
	}
	return nil
}

func (m *MessageMetadata) SetEncryptionParam(param []byte) {
	if m.messageMetadata != nil {
		m.messageMetadata.EncryptionParam = param
	}
}

func (m *MessageMetadata) encryptionKeyPresent(keyInfo EncryptionKeyInfo) int {
	if len(m.messageMetadata.EncryptionKeys) > 0 {
		for idx, k := range m.messageMetadata.EncryptionKeys {
			if k.GetKey() == keyInfo.Name() {
				return idx
			}
		}
	}
	return -1
}

func getKeyMeta(metaMap map[string]string) []*pb.KeyValue {
	if len(metaMap) > 0 {
		var meta []*pb.KeyValue
		for k, v := range metaMap {
			meta = append(meta, &pb.KeyValue{Key: &k, Value: &v})
		}
		return meta
	}
	return nil
}

func getKeyMetaMap(keyValues []*pb.KeyValue) map[string]string {
	if keyValues != nil {
		meta := map[string]string{}
		for _, kv := range keyValues {
			meta[kv.GetKey()] = kv.GetValue()
		}
		return meta
	}
	return nil
}
