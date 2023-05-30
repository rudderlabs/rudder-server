// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/jobsdb (interfaces: JobsDB)

// Package mocks_jobsdb is a generated GoMock package.
package mocks_jobsdb

import (
	context "context"
	json "encoding/json"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	uuid "github.com/google/uuid"
	jobsdb "github.com/rudderlabs/rudder-server/jobsdb"
)

// MockJobsDB is a mock of JobsDB interface.
type MockJobsDB struct {
	ctrl     *gomock.Controller
	recorder *MockJobsDBMockRecorder
}

// MockJobsDBMockRecorder is the mock recorder for MockJobsDB.
type MockJobsDBMockRecorder struct {
	mock *MockJobsDB
}

// NewMockJobsDB creates a new mock instance.
func NewMockJobsDB(ctrl *gomock.Controller) *MockJobsDB {
	mock := &MockJobsDB{ctrl: ctrl}
	mock.recorder = &MockJobsDBMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockJobsDB) EXPECT() *MockJobsDBMockRecorder {
	return m.recorder
}

// DeleteExecuting mocks base method.
func (m *MockJobsDB) DeleteExecuting() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "DeleteExecuting")
}

// DeleteExecuting indicates an expected call of DeleteExecuting.
func (mr *MockJobsDBMockRecorder) DeleteExecuting() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteExecuting", reflect.TypeOf((*MockJobsDB)(nil).DeleteExecuting))
}

// FailExecuting mocks base method.
func (m *MockJobsDB) FailExecuting() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "FailExecuting")
}

// FailExecuting indicates an expected call of FailExecuting.
func (mr *MockJobsDBMockRecorder) FailExecuting() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FailExecuting", reflect.TypeOf((*MockJobsDB)(nil).FailExecuting))
}

// GetActiveWorkspaces mocks base method.
func (m *MockJobsDB) GetActiveWorkspaces(arg0 context.Context, arg1 string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetActiveWorkspaces", arg0, arg1)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetActiveWorkspaces indicates an expected call of GetActiveWorkspaces.
func (mr *MockJobsDBMockRecorder) GetActiveWorkspaces(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetActiveWorkspaces", reflect.TypeOf((*MockJobsDB)(nil).GetActiveWorkspaces), arg0, arg1)
}

// GetDistinctParameterValues mocks base method.
func (m *MockJobsDB) GetDistinctParameterValues(arg0 context.Context, arg1 string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDistinctParameterValues", arg0, arg1)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDistinctParameterValues indicates an expected call of GetDistinctParameterValues.
func (mr *MockJobsDBMockRecorder) GetDistinctParameterValues(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDistinctParameterValues", reflect.TypeOf((*MockJobsDB)(nil).GetDistinctParameterValues), arg0, arg1)
}

// GetExecuting mocks base method.
func (m *MockJobsDB) GetExecuting(arg0 context.Context, arg1 jobsdb.GetQueryParamsT) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetExecuting", arg0, arg1)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetExecuting indicates an expected call of GetExecuting.
func (mr *MockJobsDBMockRecorder) GetExecuting(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetExecuting", reflect.TypeOf((*MockJobsDB)(nil).GetExecuting), arg0, arg1)
}

// GetImporting mocks base method.
func (m *MockJobsDB) GetImporting(arg0 context.Context, arg1 jobsdb.GetQueryParamsT) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetImporting", arg0, arg1)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetImporting indicates an expected call of GetImporting.
func (mr *MockJobsDBMockRecorder) GetImporting(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetImporting", reflect.TypeOf((*MockJobsDB)(nil).GetImporting), arg0, arg1)
}

// GetJournalEntries mocks base method.
func (m *MockJobsDB) GetJournalEntries(arg0 string) []jobsdb.JournalEntryT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetJournalEntries", arg0)
	ret0, _ := ret[0].([]jobsdb.JournalEntryT)
	return ret0
}

// GetJournalEntries indicates an expected call of GetJournalEntries.
func (mr *MockJobsDBMockRecorder) GetJournalEntries(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetJournalEntries", reflect.TypeOf((*MockJobsDB)(nil).GetJournalEntries), arg0)
}

// GetPileUpCounts mocks base method.
func (m *MockJobsDB) GetPileUpCounts(arg0 context.Context) (map[string]map[string]int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPileUpCounts", arg0)
	ret0, _ := ret[0].(map[string]map[string]int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPileUpCounts indicates an expected call of GetPileUpCounts.
func (mr *MockJobsDBMockRecorder) GetPileUpCounts(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPileUpCounts", reflect.TypeOf((*MockJobsDB)(nil).GetPileUpCounts), arg0)
}

// GetProcessed mocks base method.
func (m *MockJobsDB) GetProcessed(arg0 context.Context, arg1 jobsdb.GetQueryParamsT) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetProcessed", arg0, arg1)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetProcessed indicates an expected call of GetProcessed.
func (mr *MockJobsDBMockRecorder) GetProcessed(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetProcessed", reflect.TypeOf((*MockJobsDB)(nil).GetProcessed), arg0, arg1)
}

// GetToRetry mocks base method.
func (m *MockJobsDB) GetToRetry(arg0 context.Context, arg1 jobsdb.GetQueryParamsT) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetToRetry", arg0, arg1)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetToRetry indicates an expected call of GetToRetry.
func (mr *MockJobsDBMockRecorder) GetToRetry(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetToRetry", reflect.TypeOf((*MockJobsDB)(nil).GetToRetry), arg0, arg1)
}

// GetUnprocessed mocks base method.
func (m *MockJobsDB) GetUnprocessed(arg0 context.Context, arg1 jobsdb.GetQueryParamsT) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUnprocessed", arg0, arg1)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetUnprocessed indicates an expected call of GetUnprocessed.
func (mr *MockJobsDBMockRecorder) GetUnprocessed(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUnprocessed", reflect.TypeOf((*MockJobsDB)(nil).GetUnprocessed), arg0, arg1)
}

// GetWaiting mocks base method.
func (m *MockJobsDB) GetWaiting(arg0 context.Context, arg1 jobsdb.GetQueryParamsT) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWaiting", arg0, arg1)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWaiting indicates an expected call of GetWaiting.
func (mr *MockJobsDBMockRecorder) GetWaiting(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWaiting", reflect.TypeOf((*MockJobsDB)(nil).GetWaiting), arg0, arg1)
}

// Identifier mocks base method.
func (m *MockJobsDB) Identifier() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Identifier")
	ret0, _ := ret[0].(string)
	return ret0
}

// Identifier indicates an expected call of Identifier.
func (mr *MockJobsDBMockRecorder) Identifier() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Identifier", reflect.TypeOf((*MockJobsDB)(nil).Identifier))
}

// JournalDeleteEntry mocks base method.
func (m *MockJobsDB) JournalDeleteEntry(arg0 int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "JournalDeleteEntry", arg0)
}

// JournalDeleteEntry indicates an expected call of JournalDeleteEntry.
func (mr *MockJobsDBMockRecorder) JournalDeleteEntry(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "JournalDeleteEntry", reflect.TypeOf((*MockJobsDB)(nil).JournalDeleteEntry), arg0)
}

// JournalMarkStart mocks base method.
func (m *MockJobsDB) JournalMarkStart(arg0 string, arg1 json.RawMessage) int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "JournalMarkStart", arg0, arg1)
	ret0, _ := ret[0].(int64)
	return ret0
}

// JournalMarkStart indicates an expected call of JournalMarkStart.
func (mr *MockJobsDBMockRecorder) JournalMarkStart(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "JournalMarkStart", reflect.TypeOf((*MockJobsDB)(nil).JournalMarkStart), arg0, arg1)
}

// Ping mocks base method.
func (m *MockJobsDB) Ping() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ping")
	ret0, _ := ret[0].(error)
	return ret0
}

// Ping indicates an expected call of Ping.
func (mr *MockJobsDBMockRecorder) Ping() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ping", reflect.TypeOf((*MockJobsDB)(nil).Ping))
}

// Store mocks base method.
func (m *MockJobsDB) Store(arg0 context.Context, arg1 []*jobsdb.JobT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Store", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Store indicates an expected call of Store.
func (mr *MockJobsDBMockRecorder) Store(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Store", reflect.TypeOf((*MockJobsDB)(nil).Store), arg0, arg1)
}

// StoreEachBatchRetry mocks base method.
func (m *MockJobsDB) StoreEachBatchRetry(arg0 context.Context, arg1 [][]*jobsdb.JobT) map[uuid.UUID]string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoreEachBatchRetry", arg0, arg1)
	ret0, _ := ret[0].(map[uuid.UUID]string)
	return ret0
}

// StoreEachBatchRetry indicates an expected call of StoreEachBatchRetry.
func (mr *MockJobsDBMockRecorder) StoreEachBatchRetry(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoreEachBatchRetry", reflect.TypeOf((*MockJobsDB)(nil).StoreEachBatchRetry), arg0, arg1)
}

// StoreEachBatchRetryInTx mocks base method.
func (m *MockJobsDB) StoreEachBatchRetryInTx(arg0 context.Context, arg1 jobsdb.StoreSafeTx, arg2 [][]*jobsdb.JobT) (map[uuid.UUID]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoreEachBatchRetryInTx", arg0, arg1, arg2)
	ret0, _ := ret[0].(map[uuid.UUID]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StoreEachBatchRetryInTx indicates an expected call of StoreEachBatchRetryInTx.
func (mr *MockJobsDBMockRecorder) StoreEachBatchRetryInTx(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoreEachBatchRetryInTx", reflect.TypeOf((*MockJobsDB)(nil).StoreEachBatchRetryInTx), arg0, arg1, arg2)
}

// StoreInTx mocks base method.
func (m *MockJobsDB) StoreInTx(arg0 context.Context, arg1 jobsdb.StoreSafeTx, arg2 []*jobsdb.JobT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoreInTx", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// StoreInTx indicates an expected call of StoreInTx.
func (mr *MockJobsDBMockRecorder) StoreInTx(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoreInTx", reflect.TypeOf((*MockJobsDB)(nil).StoreInTx), arg0, arg1, arg2)
}

// UpdateJobStatus mocks base method.
func (m *MockJobsDB) UpdateJobStatus(arg0 context.Context, arg1 []*jobsdb.JobStatusT, arg2 []string, arg3 []jobsdb.ParameterFilterT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJobStatus", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateJobStatus indicates an expected call of UpdateJobStatus.
func (mr *MockJobsDBMockRecorder) UpdateJobStatus(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJobStatus", reflect.TypeOf((*MockJobsDB)(nil).UpdateJobStatus), arg0, arg1, arg2, arg3)
}

// UpdateJobStatusInTx mocks base method.
func (m *MockJobsDB) UpdateJobStatusInTx(arg0 context.Context, arg1 jobsdb.UpdateSafeTx, arg2 []*jobsdb.JobStatusT, arg3 []string, arg4 []jobsdb.ParameterFilterT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJobStatusInTx", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateJobStatusInTx indicates an expected call of UpdateJobStatusInTx.
func (mr *MockJobsDBMockRecorder) UpdateJobStatusInTx(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJobStatusInTx", reflect.TypeOf((*MockJobsDB)(nil).UpdateJobStatusInTx), arg0, arg1, arg2, arg3, arg4)
}

// WithStoreSafeTx mocks base method.
func (m *MockJobsDB) WithStoreSafeTx(arg0 context.Context, arg1 func(jobsdb.StoreSafeTx) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithStoreSafeTx", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithStoreSafeTx indicates an expected call of WithStoreSafeTx.
func (mr *MockJobsDBMockRecorder) WithStoreSafeTx(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithStoreSafeTx", reflect.TypeOf((*MockJobsDB)(nil).WithStoreSafeTx), arg0, arg1)
}

// WithTx mocks base method.
func (m *MockJobsDB) WithTx(arg0 func(*jobsdb.Tx) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithTx", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithTx indicates an expected call of WithTx.
func (mr *MockJobsDBMockRecorder) WithTx(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithTx", reflect.TypeOf((*MockJobsDB)(nil).WithTx), arg0)
}

// WithUpdateSafeTx mocks base method.
func (m *MockJobsDB) WithUpdateSafeTx(arg0 context.Context, arg1 func(jobsdb.UpdateSafeTx) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithUpdateSafeTx", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithUpdateSafeTx indicates an expected call of WithUpdateSafeTx.
func (mr *MockJobsDBMockRecorder) WithUpdateSafeTx(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithUpdateSafeTx", reflect.TypeOf((*MockJobsDB)(nil).WithUpdateSafeTx), arg0, arg1)
}
