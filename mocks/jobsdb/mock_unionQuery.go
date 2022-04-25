// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/jobsdb (interfaces: MultiTenantJobsDB)

// Package mocks_jobsdb is a generated GoMock package.
package mocks_jobsdb

import (
	sql "database/sql"
	json "encoding/json"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	jobsdb "github.com/rudderlabs/rudder-server/jobsdb"
)

// MockMultiTenantJobsDB is a mock of MultiTenantJobsDB interface.
type MockMultiTenantJobsDB struct {
	ctrl     *gomock.Controller
	recorder *MockMultiTenantJobsDBMockRecorder
}

// MockMultiTenantJobsDBMockRecorder is the mock recorder for MockMultiTenantJobsDB.
type MockMultiTenantJobsDBMockRecorder struct {
	mock *MockMultiTenantJobsDB
}

// NewMockMultiTenantJobsDB creates a new mock instance.
func NewMockMultiTenantJobsDB(ctrl *gomock.Controller) *MockMultiTenantJobsDB {
	mock := &MockMultiTenantJobsDB{ctrl: ctrl}
	mock.recorder = &MockMultiTenantJobsDBMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMultiTenantJobsDB) EXPECT() *MockMultiTenantJobsDBMockRecorder {
	return m.recorder
}

// AcquireUpdateJobStatusLocks mocks base method.
func (m *MockMultiTenantJobsDB) AcquireUpdateJobStatusLocks() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AcquireUpdateJobStatusLocks")
}

// AcquireUpdateJobStatusLocks indicates an expected call of AcquireUpdateJobStatusLocks.
func (mr *MockMultiTenantJobsDBMockRecorder) AcquireUpdateJobStatusLocks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AcquireUpdateJobStatusLocks", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).AcquireUpdateJobStatusLocks))
}

// BeginGlobalTransaction mocks base method.
func (m *MockMultiTenantJobsDB) BeginGlobalTransaction() *sql.Tx {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BeginGlobalTransaction")
	ret0, _ := ret[0].(*sql.Tx)
	return ret0
}

// BeginGlobalTransaction indicates an expected call of BeginGlobalTransaction.
func (mr *MockMultiTenantJobsDBMockRecorder) BeginGlobalTransaction() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BeginGlobalTransaction", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).BeginGlobalTransaction))
}

// CommitTransaction mocks base method.
func (m *MockMultiTenantJobsDB) CommitTransaction(arg0 *sql.Tx) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "CommitTransaction", arg0)
}

// CommitTransaction indicates an expected call of CommitTransaction.
func (mr *MockMultiTenantJobsDBMockRecorder) CommitTransaction(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CommitTransaction", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).CommitTransaction), arg0)
}

// DeleteExecuting mocks base method.
func (m *MockMultiTenantJobsDB) DeleteExecuting(arg0 *jobsdb.GetQueryParamsT) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "DeleteExecuting", arg0)
}

// DeleteExecuting indicates an expected call of DeleteExecuting.
func (mr *MockMultiTenantJobsDBMockRecorder) DeleteExecuting(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteExecuting", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).DeleteExecuting), arg0)
}

// GetAllJobs mocks base method.
func (m *MockMultiTenantJobsDB) GetAllJobs(arg0 map[string]int, arg1 *jobsdb.GetQueryParamsT, arg2 int) []*jobsdb.JobT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllJobs", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*jobsdb.JobT)
	return ret0
}

// GetAllJobs indicates an expected call of GetAllJobs.
func (mr *MockMultiTenantJobsDBMockRecorder) GetAllJobs(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllJobs", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).GetAllJobs), arg0, arg1, arg2)
}

// GetJournalEntries mocks base method.
func (m *MockMultiTenantJobsDB) GetJournalEntries(arg0 string) []jobsdb.JournalEntryT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetJournalEntries", arg0)
	ret0, _ := ret[0].([]jobsdb.JournalEntryT)
	return ret0
}

// GetJournalEntries indicates an expected call of GetJournalEntries.
func (mr *MockMultiTenantJobsDBMockRecorder) GetJournalEntries(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetJournalEntries", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).GetJournalEntries), arg0)
}

// GetPileUpCounts mocks base method.
func (m *MockMultiTenantJobsDB) GetPileUpCounts(arg0 map[string]map[string]int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "GetPileUpCounts", arg0)
}

// GetPileUpCounts indicates an expected call of GetPileUpCounts.
func (mr *MockMultiTenantJobsDBMockRecorder) GetPileUpCounts(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPileUpCounts", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).GetPileUpCounts), arg0)
}

// JournalDeleteEntry mocks base method.
func (m *MockMultiTenantJobsDB) JournalDeleteEntry(arg0 int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "JournalDeleteEntry", arg0)
}

// JournalDeleteEntry indicates an expected call of JournalDeleteEntry.
func (mr *MockMultiTenantJobsDBMockRecorder) JournalDeleteEntry(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "JournalDeleteEntry", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).JournalDeleteEntry), arg0)
}

// JournalMarkStart mocks base method.
func (m *MockMultiTenantJobsDB) JournalMarkStart(arg0 string, arg1 json.RawMessage) int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "JournalMarkStart", arg0, arg1)
	ret0, _ := ret[0].(int64)
	return ret0
}

// JournalMarkStart indicates an expected call of JournalMarkStart.
func (mr *MockMultiTenantJobsDBMockRecorder) JournalMarkStart(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "JournalMarkStart", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).JournalMarkStart), arg0, arg1)
}

// ReleaseUpdateJobStatusLocks mocks base method.
func (m *MockMultiTenantJobsDB) ReleaseUpdateJobStatusLocks() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ReleaseUpdateJobStatusLocks")
}

// ReleaseUpdateJobStatusLocks indicates an expected call of ReleaseUpdateJobStatusLocks.
func (mr *MockMultiTenantJobsDBMockRecorder) ReleaseUpdateJobStatusLocks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReleaseUpdateJobStatusLocks", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).ReleaseUpdateJobStatusLocks))
}

// UpdateJobStatus mocks base method.
func (m *MockMultiTenantJobsDB) UpdateJobStatus(arg0 []*jobsdb.JobStatusT, arg1 []string, arg2 []jobsdb.ParameterFilterT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJobStatus", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateJobStatus indicates an expected call of UpdateJobStatus.
func (mr *MockMultiTenantJobsDBMockRecorder) UpdateJobStatus(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJobStatus", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).UpdateJobStatus), arg0, arg1, arg2)
}

// UpdateJobStatusInTxn mocks base method.
func (m *MockMultiTenantJobsDB) UpdateJobStatusInTxn(arg0 *sql.Tx, arg1 []*jobsdb.JobStatusT, arg2 []string, arg3 []jobsdb.ParameterFilterT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJobStatusInTxn", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateJobStatusInTxn indicates an expected call of UpdateJobStatusInTxn.
func (mr *MockMultiTenantJobsDBMockRecorder) UpdateJobStatusInTxn(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJobStatusInTxn", reflect.TypeOf((*MockMultiTenantJobsDB)(nil).UpdateJobStatusInTxn), arg0, arg1, arg2, arg3)
}
