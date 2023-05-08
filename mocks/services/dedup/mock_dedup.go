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

// Get mocks base method.
func (m *MockDedup) Get(arg0 string) (int64, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", arg0)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockDedupMockRecorder) Get(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockDedup)(nil).Get), arg0)
}

// MarkProcessed mocks base method.
func (m *MockDedup) MarkProcessed(arg0 []dedup.Message) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MarkProcessed", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// MarkProcessed indicates an expected call of MarkProcessed.
func (mr *MockDedupMockRecorder) MarkProcessed(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MarkProcessed", reflect.TypeOf((*MockDedup)(nil).MarkProcessed), arg0)
}

// PrintHistogram mocks base method.
func (m *MockDedup) PrintHistogram() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "PrintHistogram")
}

// PrintHistogram indicates an expected call of PrintHistogram.
func (mr *MockDedupMockRecorder) PrintHistogram() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PrintHistogram", reflect.TypeOf((*MockDedup)(nil).PrintHistogram))
}
