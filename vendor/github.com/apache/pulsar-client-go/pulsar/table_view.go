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
	"reflect"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/log"
)

// TableViewOptions contains the options for creating a TableView
type TableViewOptions struct {
	// Topic specifies the topic this table view will subscribe on.
	// This argument is required when constructing the table view.
	Topic string

	// Set the interval of updating partitions. Default to 1 minute.
	AutoUpdatePartitionsInterval time.Duration

	// Schema represents the schema implementation.
	Schema Schema

	// SchemaValueType represents the type of values for the given schema.
	SchemaValueType reflect.Type

	// Configure the logger used by the TableView.
	// By default, a wrapped logrus.StandardLogger will be used, namely,
	// log.NewLoggerWithLogrus(logrus.StandardLogger())
	Logger log.Logger
}

// TableView provides a key-value map view of a compacted topic. Messages without keys will be ignored.
type TableView interface {
	// Size returns the number of key-value mappings in the TableView.
	Size() int

	// IsEmpty returns true if this TableView contains no key-value mappings.
	IsEmpty() bool

	// ContainsKey returns true if this TableView contains a mapping for the specified key.
	ContainsKey(key string) bool

	// Get returns the value to which the specified key is mapped, or nil if this map contains no mapping for the key.
	Get(key string) interface{}

	// Entries returns a map view of the mappings contained in this TableView.
	Entries() map[string]interface{}

	// Keys returns a slice of the keys contained in this TableView.
	Keys() []string

	// ForEach performs the give action for each entry in this map until all entries have been processed or the action
	// returns an error.
	ForEach(func(string, interface{}) error) error

	// ForEachAndListen performs the give action for each entry in this map until all entries have been processed or
	// the action returns an error.
	ForEachAndListen(func(string, interface{}) error) error

	// Close closes the table view and releases resources allocated.
	Close()
}
