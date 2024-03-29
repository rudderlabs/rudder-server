// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/services/dedup (interfaces: Dedup)

// Package mock_dedup is a generated GoMock package.
package mock_dedup

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	dedup "github.com/rudderlabs/rudder-server/services/dedup"
)

// MockDedup is a mock of Dedup interface.
type MockDedup struct {
	ctrl     *gomock.Controller
	recorder *MockDedupMockRecorder
}

// MockDedupMockRecorder is the mock recorder for MockDedup.
type MockDedupMockRecorder struct {
	mock *MockDedup
}

// NewMockDedup creates a new mock instance.
func NewMockDedup(ctrl *gomock.Controller) *MockDedup {
	mock := &MockDedup{ctrl: ctrl}
	mock.recorder = &MockDedupMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDedup) EXPECT() *MockDedupMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockDedup) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockDedupMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockDedup)(nil).Close))
}

// Commit mocks base method.
func (m *MockDedup) Commit(arg0 []string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit.
func (mr *MockDedupMockRecorder) Commit(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockDedup)(nil).Commit), arg0)
}

// Set mocks base method.
func (m *MockDedup) Set(arg0 dedup.KeyValue) (bool, int64) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Set", arg0)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(int64)
	return ret0, ret1
}

// Set indicates an expected call of Set.
func (mr *MockDedupMockRecorder) Set(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Set", reflect.TypeOf((*MockDedup)(nil).Set), arg0)
}
