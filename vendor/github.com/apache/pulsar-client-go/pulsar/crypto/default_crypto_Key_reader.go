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

import "os"

// FileKeyReader default implementation of KeyReader
type FileKeyReader struct {
	publicKeyPath  string
	privateKeyPath string
}

func NewFileKeyReader(publicKeyPath, privateKeyPath string) *FileKeyReader {
	return &FileKeyReader{
		publicKeyPath:  publicKeyPath,
		privateKeyPath: privateKeyPath,
	}
}

// PublicKey read public key from the given path
func (d *FileKeyReader) PublicKey(keyName string, keyMeta map[string]string) (*EncryptionKeyInfo, error) {
	return readKey(keyName, d.publicKeyPath, keyMeta)
}

// PrivateKey read private key from the given path
func (d *FileKeyReader) PrivateKey(keyName string, keyMeta map[string]string) (*EncryptionKeyInfo, error) {
	return readKey(keyName, d.privateKeyPath, keyMeta)
}

func readKey(keyName, path string, keyMeta map[string]string) (*EncryptionKeyInfo, error) {
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return NewEncryptionKeyInfo(keyName, key, keyMeta), nil
}
