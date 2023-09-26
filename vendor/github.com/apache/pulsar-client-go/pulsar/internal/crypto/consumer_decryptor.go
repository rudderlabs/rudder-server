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
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar/crypto"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/apache/pulsar-client-go/pulsar/log"
)

type consumerDecryptor struct {
	keyReader     crypto.KeyReader
	messageCrypto crypto.MessageCrypto
	logger        log.Logger
}

func NewConsumerDecryptor(keyReader crypto.KeyReader,
	messageCrypto crypto.MessageCrypto,
	logger log.Logger) Decryptor {
	return &consumerDecryptor{
		keyReader:     keyReader,
		messageCrypto: messageCrypto,
		logger:        logger,
	}
}

func (d *consumerDecryptor) Decrypt(payload []byte,
	msgID *pb.MessageIdData,
	msgMetadata *pb.MessageMetadata) ([]byte, error) {
	// encryption keys are not present in message metadta, no need decrypt the payload
	if len(msgMetadata.GetEncryptionKeys()) == 0 {
		return payload, nil
	}

	// KeyReader interface is not implemented
	if d.keyReader == nil {
		return payload, fmt.Errorf("KeyReader interface is not implemented")
	}

	return d.messageCrypto.Decrypt(crypto.NewMessageMetadataSupplier(msgMetadata),
		payload,
		d.keyReader)
}
