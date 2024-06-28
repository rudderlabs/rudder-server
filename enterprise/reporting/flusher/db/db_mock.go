// Code generated by MockGen. DO NOT EDIT.
// Source: ./db.go

// Package db is a generated GoMock package.
package db

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	report "github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/report"
)

// MockDB is a mock of DB interface.
type MockDB struct {
	ctrl     *gomock.Controller
	recorder *MockDBMockRecorder
}

// MockDBMockRecorder is the mock recorder for MockDB.
type MockDBMockRecorder struct {
	mock *MockDB
}

// NewMockDB creates a new mock instance.
func NewMockDB(ctrl *gomock.Controller) *MockDB {
	mock := &MockDB{ctrl: ctrl}
	mock.recorder = &MockDBMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDB) EXPECT() *MockDBMockRecorder {
	return m.recorder
}

// CloseDB mocks base method.
func (m *MockDB) CloseDB() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseDB")
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseDB indicates an expected call of CloseDB.
func (mr *MockDBMockRecorder) CloseDB() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseDB", reflect.TypeOf((*MockDB)(nil).CloseDB))
}

// Delete mocks base method.
func (m *MockDB) Delete(ctx context.Context, table string, start, end time.Time) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", ctx, table, start, end)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockDBMockRecorder) Delete(ctx, table, start, end interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockDB)(nil).Delete), ctx, table, start, end)
}

// FetchBatch mocks base method.
func (m *MockDB) FetchBatch(ctx context.Context, table string, start, end time.Time, limit, offset int) ([]report.RawReport, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchBatch", ctx, table, start, end, limit, offset)
	ret0, _ := ret[0].([]report.RawReport)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FetchBatch indicates an expected call of FetchBatch.
func (mr *MockDBMockRecorder) FetchBatch(ctx, table, start, end, limit, offset interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchBatch", reflect.TypeOf((*MockDB)(nil).FetchBatch), ctx, table, start, end, limit, offset)
}

// GetStart mocks base method.
func (m *MockDB) GetStart(ctx context.Context, table string) (time.Time, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStart", ctx, table)
	ret0, _ := ret[0].(time.Time)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStart indicates an expected call of GetStart.
func (mr *MockDBMockRecorder) GetStart(ctx, table interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStart", reflect.TypeOf((*MockDB)(nil).GetStart), ctx, table)
}

// InitDB mocks base method.
func (m *MockDB) InitDB() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "InitDB")
	ret0, _ := ret[0].(error)
	return ret0
}

// InitDB indicates an expected call of InitDB.
func (mr *MockDBMockRecorder) InitDB() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "InitDB", reflect.TypeOf((*MockDB)(nil).InitDB))
}