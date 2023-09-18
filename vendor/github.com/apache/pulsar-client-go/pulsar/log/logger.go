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

// Package log defines the logger interfaces used by pulsar client.
// Users can leverage these interfaces to provide a customized logger
// implementation.
//
// The Logger and Entry interfaces defined here are inspired
// by sirupsen/logrus, both logrus and zap logging libraries
// are good resources to learn how to implement a effective
// logging library.
//
// Besides the interfaces, this log library also provides an
// implementation based on logrus, and a No-op one as well.
package log

// Fields type, used to pass to `WithFields`.
type Fields map[string]interface{}

// Logger describes the interface that must be implemeted by all loggers.
type Logger interface {
	SubLogger(fields Fields) Logger

	WithFields(fields Fields) Entry
	WithField(name string, value interface{}) Entry
	WithError(err error) Entry

	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})

	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// Entry describes the interface for the logger entry.
type Entry interface {
	WithFields(fields Fields) Entry
	WithField(name string, value interface{}) Entry

	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})

	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}
