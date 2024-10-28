// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/services/debugger/source (interfaces: SourceDebugger)
//
// Generated by this command:
//
//	mockgen -destination=./mocks/mock.go -package=mocks github.com/rudderlabs/rudder-server/services/debugger/source SourceDebugger
//

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockSourceDebugger is a mock of SourceDebugger interface.
type MockSourceDebugger struct {
	ctrl     *gomock.Controller
	recorder *MockSourceDebuggerMockRecorder
}

// MockSourceDebuggerMockRecorder is the mock recorder for MockSourceDebugger.
type MockSourceDebuggerMockRecorder struct {
	mock *MockSourceDebugger
}

// NewMockSourceDebugger creates a new mock instance.
func NewMockSourceDebugger(ctrl *gomock.Controller) *MockSourceDebugger {
	mock := &MockSourceDebugger{ctrl: ctrl}
	mock.recorder = &MockSourceDebuggerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSourceDebugger) EXPECT() *MockSourceDebuggerMockRecorder {
	return m.recorder
}

// RecordEvent mocks base method.
func (m *MockSourceDebugger) RecordEvent(arg0 string, arg1 []byte) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RecordEvent", arg0, arg1)
	ret0, _ := ret[0].(bool)
	return ret0
}

// RecordEvent indicates an expected call of RecordEvent.
func (mr *MockSourceDebuggerMockRecorder) RecordEvent(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordEvent", reflect.TypeOf((*MockSourceDebugger)(nil).RecordEvent), arg0, arg1)
}

// Stop mocks base method.
func (m *MockSourceDebugger) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockSourceDebuggerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockSourceDebugger)(nil).Stop))
}
