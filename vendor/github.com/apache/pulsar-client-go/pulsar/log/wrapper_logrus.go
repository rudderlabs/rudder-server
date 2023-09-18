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

package log

import (
	"github.com/sirupsen/logrus"
)

// logrusWrapper implements Logger interface
// based on underlying logrus.FieldLogger
type logrusWrapper struct {
	l logrus.FieldLogger
}

// NewLoggerWithLogrus creates a new logger which wraps
// the given logrus.Logger
func NewLoggerWithLogrus(logger *logrus.Logger) Logger {
	return &logrusWrapper{
		l: logger,
	}
}

func (l *logrusWrapper) SubLogger(fs Fields) Logger {
	return &logrusWrapper{
		l: l.l.WithFields(logrus.Fields(fs)),
	}
}

func (l *logrusWrapper) WithFields(fs Fields) Entry {
	return logrusEntry{
		e: l.l.WithFields(logrus.Fields(fs)),
	}
}

func (l *logrusWrapper) WithField(name string, value interface{}) Entry {
	return logrusEntry{
		e: l.l.WithField(name, value),
	}
}

func (l *logrusWrapper) WithError(err error) Entry {
	return logrusEntry{
		e: l.l.WithError(err),
	}
}

func (l *logrusWrapper) Debug(args ...interface{}) {
	l.l.Debug(args...)
}

func (l *logrusWrapper) Info(args ...interface{}) {
	l.l.Info(args...)
}

func (l *logrusWrapper) Warn(args ...interface{}) {
	l.l.Warn(args...)
}

func (l *logrusWrapper) Error(args ...interface{}) {
	l.l.Error(args...)
}

func (l *logrusWrapper) Debugf(format string, args ...interface{}) {
	l.l.Debugf(format, args...)
}

func (l *logrusWrapper) Infof(format string, args ...interface{}) {
	l.l.Infof(format, args...)
}

func (l *logrusWrapper) Warnf(format string, args ...interface{}) {
	l.l.Warnf(format, args...)
}

func (l *logrusWrapper) Errorf(format string, args ...interface{}) {
	l.l.Errorf(format, args...)
}

type logrusEntry struct {
	e logrus.FieldLogger
}

func (l logrusEntry) WithFields(fs Fields) Entry {
	return logrusEntry{
		e: l.e.WithFields(logrus.Fields(fs)),
	}
}

func (l logrusEntry) WithField(name string, value interface{}) Entry {
	return logrusEntry{
		e: l.e.WithField(name, value),
	}
}

func (l logrusEntry) Debug(args ...interface{}) {
	l.e.Debug(args...)
}

func (l logrusEntry) Info(args ...interface{}) {
	l.e.Info(args...)
}

func (l logrusEntry) Warn(args ...interface{}) {
	l.e.Warn(args...)
}

func (l logrusEntry) Error(args ...interface{}) {
	l.e.Error(args...)
}

func (l logrusEntry) Debugf(format string, args ...interface{}) {
	l.e.Debugf(format, args...)
}

func (l logrusEntry) Infof(format string, args ...interface{}) {
	l.e.Infof(format, args...)
}

func (l logrusEntry) Warnf(format string, args ...interface{}) {
	l.e.Warnf(format, args...)
}

func (l logrusEntry) Errorf(format string, args ...interface{}) {
	l.e.Errorf(format, args...)
}
