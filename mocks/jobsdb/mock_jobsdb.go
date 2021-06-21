// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/jobsdb (interfaces: JobsDB)

// Package mocks_jobsdb is a generated GoMock package.
package mocks_jobsdb

import (
	sql "database/sql"
	gomock "github.com/golang/mock/gomock"
	jobsdb "github.com/rudderlabs/rudder-server/jobsdb"
	uuid "github.com/satori/go.uuid"
	reflect "reflect"
)

// MockJobsDB is a mock of JobsDB interface
type MockJobsDB struct {
	ctrl     *gomock.Controller
	recorder *MockJobsDBMockRecorder
}

// MockJobsDBMockRecorder is the mock recorder for MockJobsDB
type MockJobsDBMockRecorder struct {
	mock *MockJobsDB
}

// NewMockJobsDB creates a new mock instance
func NewMockJobsDB(ctrl *gomock.Controller) *MockJobsDB {
	mock := &MockJobsDB{ctrl: ctrl}
	mock.recorder = &MockJobsDBMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockJobsDB) EXPECT() *MockJobsDBMockRecorder {
	return m.recorder
}

// AcquireStoreLock mocks base method
func (m *MockJobsDB) AcquireStoreLock() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AcquireStoreLock")
}

// AcquireStoreLock indicates an expected call of AcquireStoreLock
func (mr *MockJobsDBMockRecorder) AcquireStoreLock() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AcquireStoreLock", reflect.TypeOf((*MockJobsDB)(nil).AcquireStoreLock))
}

// AcquireUpdateJobStatusLocks mocks base method
func (m *MockJobsDB) AcquireUpdateJobStatusLocks() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AcquireUpdateJobStatusLocks")
}

// AcquireUpdateJobStatusLocks indicates an expected call of AcquireUpdateJobStatusLocks
func (mr *MockJobsDBMockRecorder) AcquireUpdateJobStatusLocks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AcquireUpdateJobStatusLocks", reflect.TypeOf((*MockJobsDB)(nil).AcquireUpdateJobStatusLocks))
}

// BeginGlobalTransaction mocks base method
func (m *MockJobsDB) BeginGlobalTransaction() *sql.Tx {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BeginGlobalTransaction")
	ret0, _ := ret[0].(*sql.Tx)
	return ret0
}

// BeginGlobalTransaction indicates an expected call of BeginGlobalTransaction
func (mr *MockJobsDBMockRecorder) BeginGlobalTransaction() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BeginGlobalTransaction", reflect.TypeOf((*MockJobsDB)(nil).BeginGlobalTransaction))
}

// CheckPGHealth mocks base method
func (m *MockJobsDB) CheckPGHealth() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckPGHealth")
	ret0, _ := ret[0].(bool)
	return ret0
}

// CheckPGHealth indicates an expected call of CheckPGHealth
func (mr *MockJobsDBMockRecorder) CheckPGHealth() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckPGHealth", reflect.TypeOf((*MockJobsDB)(nil).CheckPGHealth))
}

// CommitTransaction mocks base method
func (m *MockJobsDB) CommitTransaction(arg0 *sql.Tx) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "CommitTransaction", arg0)
}

// CommitTransaction indicates an expected call of CommitTransaction
func (mr *MockJobsDBMockRecorder) CommitTransaction(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CommitTransaction", reflect.TypeOf((*MockJobsDB)(nil).CommitTransaction), arg0)
}

// DeleteExecuting mocks base method
func (m *MockJobsDB) DeleteExecuting(arg0 jobsdb.GetQueryParamsT) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "DeleteExecuting", arg0)
}

// DeleteExecuting indicates an expected call of DeleteExecuting
func (mr *MockJobsDBMockRecorder) DeleteExecuting(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteExecuting", reflect.TypeOf((*MockJobsDB)(nil).DeleteExecuting), arg0)
}

// GetExecuting mocks base method
func (m *MockJobsDB) GetExecuting(arg0 jobsdb.GetQueryParamsT) []*jobsdb.JobT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetExecuting", arg0)
	ret0, _ := ret[0].([]*jobsdb.JobT)
	return ret0
}

// GetExecuting indicates an expected call of GetExecuting
func (mr *MockJobsDBMockRecorder) GetExecuting(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetExecuting", reflect.TypeOf((*MockJobsDB)(nil).GetExecuting), arg0)
}

// GetIdentifier mocks base method
func (m *MockJobsDB) GetIdentifier() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetIdentifier")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetIdentifier indicates an expected call of GetIdentifier
func (mr *MockJobsDBMockRecorder) GetIdentifier() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetIdentifier", reflect.TypeOf((*MockJobsDB)(nil).GetIdentifier))
}

// GetProcessed mocks base method
func (m *MockJobsDB) GetProcessed(arg0 jobsdb.GetQueryParamsT) []*jobsdb.JobT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetProcessed", arg0)
	ret0, _ := ret[0].([]*jobsdb.JobT)
	return ret0
}

// GetProcessed indicates an expected call of GetProcessed
func (mr *MockJobsDBMockRecorder) GetProcessed(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetProcessed", reflect.TypeOf((*MockJobsDB)(nil).GetProcessed), arg0)
}

// GetThrottled mocks base method
func (m *MockJobsDB) GetThrottled(arg0 jobsdb.GetQueryParamsT) []*jobsdb.JobT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetThrottled", arg0)
	ret0, _ := ret[0].([]*jobsdb.JobT)
	return ret0
}

// GetThrottled indicates an expected call of GetThrottled
func (mr *MockJobsDBMockRecorder) GetThrottled(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetThrottled", reflect.TypeOf((*MockJobsDB)(nil).GetThrottled), arg0)
}

// GetToRetry mocks base method
func (m *MockJobsDB) GetToRetry(arg0 jobsdb.GetQueryParamsT) []*jobsdb.JobT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetToRetry", arg0)
	ret0, _ := ret[0].([]*jobsdb.JobT)
	return ret0
}

// GetToRetry indicates an expected call of GetToRetry
func (mr *MockJobsDBMockRecorder) GetToRetry(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetToRetry", reflect.TypeOf((*MockJobsDB)(nil).GetToRetry), arg0)
}

// GetUnprocessed mocks base method
func (m *MockJobsDB) GetUnprocessed(arg0 jobsdb.GetQueryParamsT) []*jobsdb.JobT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUnprocessed", arg0)
	ret0, _ := ret[0].([]*jobsdb.JobT)
	return ret0
}

// GetUnprocessed indicates an expected call of GetUnprocessed
func (mr *MockJobsDBMockRecorder) GetUnprocessed(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUnprocessed", reflect.TypeOf((*MockJobsDB)(nil).GetUnprocessed), arg0)
}

// GetWaiting mocks base method
func (m *MockJobsDB) GetWaiting(arg0 jobsdb.GetQueryParamsT) []*jobsdb.JobT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWaiting", arg0)
	ret0, _ := ret[0].([]*jobsdb.JobT)
	return ret0
}

// GetWaiting indicates an expected call of GetWaiting
func (mr *MockJobsDBMockRecorder) GetWaiting(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWaiting", reflect.TypeOf((*MockJobsDB)(nil).GetWaiting), arg0)
}

// ReleaseStoreLock mocks base method
func (m *MockJobsDB) ReleaseStoreLock() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ReleaseStoreLock")
}

// ReleaseStoreLock indicates an expected call of ReleaseStoreLock
func (mr *MockJobsDBMockRecorder) ReleaseStoreLock() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReleaseStoreLock", reflect.TypeOf((*MockJobsDB)(nil).ReleaseStoreLock))
}

// ReleaseUpdateJobStatusLocks mocks base method
func (m *MockJobsDB) ReleaseUpdateJobStatusLocks() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ReleaseUpdateJobStatusLocks")
}

// ReleaseUpdateJobStatusLocks indicates an expected call of ReleaseUpdateJobStatusLocks
func (mr *MockJobsDBMockRecorder) ReleaseUpdateJobStatusLocks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReleaseUpdateJobStatusLocks", reflect.TypeOf((*MockJobsDB)(nil).ReleaseUpdateJobStatusLocks))
}

// Status mocks base method
func (m *MockJobsDB) Status() interface{} {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Status")
	ret0, _ := ret[0].(interface{})
	return ret0
}

// Status indicates an expected call of Status
func (mr *MockJobsDBMockRecorder) Status() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Status", reflect.TypeOf((*MockJobsDB)(nil).Status))
}

// Store mocks base method
func (m *MockJobsDB) Store(arg0 []*jobsdb.JobT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Store", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Store indicates an expected call of Store
func (mr *MockJobsDBMockRecorder) Store(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Store", reflect.TypeOf((*MockJobsDB)(nil).Store), arg0)
}

// StoreWithRetryEach mocks base method
func (m *MockJobsDB) StoreWithRetryEach(arg0 []*jobsdb.JobT) map[uuid.UUID]string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoreWithRetryEach", arg0)
	ret0, _ := ret[0].(map[uuid.UUID]string)
	return ret0
}

// StoreWithRetryEach indicates an expected call of StoreWithRetryEach
func (mr *MockJobsDBMockRecorder) StoreWithRetryEach(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoreWithRetryEach", reflect.TypeOf((*MockJobsDB)(nil).StoreWithRetryEach), arg0)
}

// UpdateJobStatus mocks base method
func (m *MockJobsDB) UpdateJobStatus(arg0 []*jobsdb.JobStatusT, arg1 []string, arg2 []jobsdb.ParameterFilterT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJobStatus", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateJobStatus indicates an expected call of UpdateJobStatus
func (mr *MockJobsDBMockRecorder) UpdateJobStatus(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJobStatus", reflect.TypeOf((*MockJobsDB)(nil).UpdateJobStatus), arg0, arg1, arg2)
}

// UpdateJobStatusInTxn mocks base method
func (m *MockJobsDB) UpdateJobStatusInTxn(arg0 *sql.Tx, arg1 []*jobsdb.JobStatusT, arg2 []string, arg3 []jobsdb.ParameterFilterT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJobStatusInTxn", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateJobStatusInTxn indicates an expected call of UpdateJobStatusInTxn
func (mr *MockJobsDBMockRecorder) UpdateJobStatusInTxn(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJobStatusInTxn", reflect.TypeOf((*MockJobsDB)(nil).UpdateJobStatusInTxn), arg0, arg1, arg2, arg3)
}
