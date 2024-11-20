// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/jobsdb (interfaces: JobsDB)
//
// Generated by this command:
//
//	mockgen -destination=../mocks/jobsdb/mock_jobsdb.go -package=mocks_jobsdb github.com/rudderlabs/rudder-server/jobsdb JobsDB
//

// Package mocks_jobsdb is a generated GoMock package.
package mocks_jobsdb

import (
	context "context"
	json "encoding/json"
	reflect "reflect"

	uuid "github.com/google/uuid"
	jobsdb "github.com/rudderlabs/rudder-server/jobsdb"
	tx "github.com/rudderlabs/rudder-server/utils/tx"
	gomock "go.uber.org/mock/gomock"
)

// MockJobsDB is a mock of JobsDB interface.
type MockJobsDB struct {
	ctrl     *gomock.Controller
	recorder *MockJobsDBMockRecorder
	isgomock struct{}
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

// GetAborted mocks base method.
func (m *MockJobsDB) GetAborted(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAborted", ctx, params)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAborted indicates an expected call of GetAborted.
func (mr *MockJobsDBMockRecorder) GetAborted(ctx, params any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAborted", reflect.TypeOf((*MockJobsDB)(nil).GetAborted), ctx, params)
}

// GetActiveWorkspaces mocks base method.
func (m *MockJobsDB) GetActiveWorkspaces(ctx context.Context, customVal string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetActiveWorkspaces", ctx, customVal)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetActiveWorkspaces indicates an expected call of GetActiveWorkspaces.
func (mr *MockJobsDBMockRecorder) GetActiveWorkspaces(ctx, customVal any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetActiveWorkspaces", reflect.TypeOf((*MockJobsDB)(nil).GetActiveWorkspaces), ctx, customVal)
}

// GetDistinctParameterValues mocks base method.
func (m *MockJobsDB) GetDistinctParameterValues(ctx context.Context, parameterName string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDistinctParameterValues", ctx, parameterName)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDistinctParameterValues indicates an expected call of GetDistinctParameterValues.
func (mr *MockJobsDBMockRecorder) GetDistinctParameterValues(ctx, parameterName any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDistinctParameterValues", reflect.TypeOf((*MockJobsDB)(nil).GetDistinctParameterValues), ctx, parameterName)
}

// GetFailed mocks base method.
func (m *MockJobsDB) GetFailed(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetFailed", ctx, params)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetFailed indicates an expected call of GetFailed.
func (mr *MockJobsDBMockRecorder) GetFailed(ctx, params any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetFailed", reflect.TypeOf((*MockJobsDB)(nil).GetFailed), ctx, params)
}

// GetImporting mocks base method.
func (m *MockJobsDB) GetImporting(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetImporting", ctx, params)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetImporting indicates an expected call of GetImporting.
func (mr *MockJobsDBMockRecorder) GetImporting(ctx, params any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetImporting", reflect.TypeOf((*MockJobsDB)(nil).GetImporting), ctx, params)
}

// GetJobs mocks base method.
func (m *MockJobsDB) GetJobs(ctx context.Context, states []string, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetJobs", ctx, states, params)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetJobs indicates an expected call of GetJobs.
func (mr *MockJobsDBMockRecorder) GetJobs(ctx, states, params any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetJobs", reflect.TypeOf((*MockJobsDB)(nil).GetJobs), ctx, states, params)
}

// GetJournalEntries mocks base method.
func (m *MockJobsDB) GetJournalEntries(opType string) []jobsdb.JournalEntryT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetJournalEntries", opType)
	ret0, _ := ret[0].([]jobsdb.JournalEntryT)
	return ret0
}

// GetJournalEntries indicates an expected call of GetJournalEntries.
func (mr *MockJobsDBMockRecorder) GetJournalEntries(opType any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetJournalEntries", reflect.TypeOf((*MockJobsDB)(nil).GetJournalEntries), opType)
}

// GetPileUpCounts mocks base method.
func (m *MockJobsDB) GetPileUpCounts(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPileUpCounts", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// GetPileUpCounts indicates an expected call of GetPileUpCounts.
func (mr *MockJobsDBMockRecorder) GetPileUpCounts(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPileUpCounts", reflect.TypeOf((*MockJobsDB)(nil).GetPileUpCounts), ctx)
}

// GetSucceeded mocks base method.
func (m *MockJobsDB) GetSucceeded(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSucceeded", ctx, params)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSucceeded indicates an expected call of GetSucceeded.
func (mr *MockJobsDBMockRecorder) GetSucceeded(ctx, params any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSucceeded", reflect.TypeOf((*MockJobsDB)(nil).GetSucceeded), ctx, params)
}

// GetToProcess mocks base method.
func (m *MockJobsDB) GetToProcess(ctx context.Context, params jobsdb.GetQueryParams, more jobsdb.MoreToken) (*jobsdb.MoreJobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetToProcess", ctx, params, more)
	ret0, _ := ret[0].(*jobsdb.MoreJobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetToProcess indicates an expected call of GetToProcess.
func (mr *MockJobsDBMockRecorder) GetToProcess(ctx, params, more any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetToProcess", reflect.TypeOf((*MockJobsDB)(nil).GetToProcess), ctx, params, more)
}

// GetUnprocessed mocks base method.
func (m *MockJobsDB) GetUnprocessed(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUnprocessed", ctx, params)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetUnprocessed indicates an expected call of GetUnprocessed.
func (mr *MockJobsDBMockRecorder) GetUnprocessed(ctx, params any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUnprocessed", reflect.TypeOf((*MockJobsDB)(nil).GetUnprocessed), ctx, params)
}

// GetWaiting mocks base method.
func (m *MockJobsDB) GetWaiting(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWaiting", ctx, params)
	ret0, _ := ret[0].(jobsdb.JobsResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWaiting indicates an expected call of GetWaiting.
func (mr *MockJobsDBMockRecorder) GetWaiting(ctx, params any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWaiting", reflect.TypeOf((*MockJobsDB)(nil).GetWaiting), ctx, params)
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

// IsMasterBackupEnabled mocks base method.
func (m *MockJobsDB) IsMasterBackupEnabled() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsMasterBackupEnabled")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsMasterBackupEnabled indicates an expected call of IsMasterBackupEnabled.
func (mr *MockJobsDBMockRecorder) IsMasterBackupEnabled() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsMasterBackupEnabled", reflect.TypeOf((*MockJobsDB)(nil).IsMasterBackupEnabled))
}

// JournalDeleteEntry mocks base method.
func (m *MockJobsDB) JournalDeleteEntry(opID int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "JournalDeleteEntry", opID)
}

// JournalDeleteEntry indicates an expected call of JournalDeleteEntry.
func (mr *MockJobsDBMockRecorder) JournalDeleteEntry(opID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "JournalDeleteEntry", reflect.TypeOf((*MockJobsDB)(nil).JournalDeleteEntry), opID)
}

// JournalMarkDone mocks base method.
func (m *MockJobsDB) JournalMarkDone(opID int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "JournalMarkDone", opID)
	ret0, _ := ret[0].(error)
	return ret0
}

// JournalMarkDone indicates an expected call of JournalMarkDone.
func (mr *MockJobsDBMockRecorder) JournalMarkDone(opID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "JournalMarkDone", reflect.TypeOf((*MockJobsDB)(nil).JournalMarkDone), opID)
}

// JournalMarkStart mocks base method.
func (m *MockJobsDB) JournalMarkStart(opType string, opPayload json.RawMessage) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "JournalMarkStart", opType, opPayload)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// JournalMarkStart indicates an expected call of JournalMarkStart.
func (mr *MockJobsDBMockRecorder) JournalMarkStart(opType, opPayload any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "JournalMarkStart", reflect.TypeOf((*MockJobsDB)(nil).JournalMarkStart), opType, opPayload)
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
func (m *MockJobsDB) Store(ctx context.Context, jobList []*jobsdb.JobT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Store", ctx, jobList)
	ret0, _ := ret[0].(error)
	return ret0
}

// Store indicates an expected call of Store.
func (mr *MockJobsDBMockRecorder) Store(ctx, jobList any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Store", reflect.TypeOf((*MockJobsDB)(nil).Store), ctx, jobList)
}

// StoreEachBatchRetry mocks base method.
func (m *MockJobsDB) StoreEachBatchRetry(ctx context.Context, jobBatches [][]*jobsdb.JobT) map[uuid.UUID]string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoreEachBatchRetry", ctx, jobBatches)
	ret0, _ := ret[0].(map[uuid.UUID]string)
	return ret0
}

// StoreEachBatchRetry indicates an expected call of StoreEachBatchRetry.
func (mr *MockJobsDBMockRecorder) StoreEachBatchRetry(ctx, jobBatches any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoreEachBatchRetry", reflect.TypeOf((*MockJobsDB)(nil).StoreEachBatchRetry), ctx, jobBatches)
}

// StoreEachBatchRetryInTx mocks base method.
func (m *MockJobsDB) StoreEachBatchRetryInTx(ctx context.Context, tx jobsdb.StoreSafeTx, jobBatches [][]*jobsdb.JobT) (map[uuid.UUID]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoreEachBatchRetryInTx", ctx, tx, jobBatches)
	ret0, _ := ret[0].(map[uuid.UUID]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StoreEachBatchRetryInTx indicates an expected call of StoreEachBatchRetryInTx.
func (mr *MockJobsDBMockRecorder) StoreEachBatchRetryInTx(ctx, tx, jobBatches any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoreEachBatchRetryInTx", reflect.TypeOf((*MockJobsDB)(nil).StoreEachBatchRetryInTx), ctx, tx, jobBatches)
}

// StoreInTx mocks base method.
func (m *MockJobsDB) StoreInTx(ctx context.Context, tx jobsdb.StoreSafeTx, jobList []*jobsdb.JobT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoreInTx", ctx, tx, jobList)
	ret0, _ := ret[0].(error)
	return ret0
}

// StoreInTx indicates an expected call of StoreInTx.
func (mr *MockJobsDBMockRecorder) StoreInTx(ctx, tx, jobList any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoreInTx", reflect.TypeOf((*MockJobsDB)(nil).StoreInTx), ctx, tx, jobList)
}

// UpdateJobStatus mocks base method.
func (m *MockJobsDB) UpdateJobStatus(ctx context.Context, statusList []*jobsdb.JobStatusT, customValFilters []string, parameterFilters []jobsdb.ParameterFilterT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJobStatus", ctx, statusList, customValFilters, parameterFilters)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateJobStatus indicates an expected call of UpdateJobStatus.
func (mr *MockJobsDBMockRecorder) UpdateJobStatus(ctx, statusList, customValFilters, parameterFilters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJobStatus", reflect.TypeOf((*MockJobsDB)(nil).UpdateJobStatus), ctx, statusList, customValFilters, parameterFilters)
}

// UpdateJobStatusInTx mocks base method.
func (m *MockJobsDB) UpdateJobStatusInTx(ctx context.Context, tx jobsdb.UpdateSafeTx, statusList []*jobsdb.JobStatusT, customValFilters []string, parameterFilters []jobsdb.ParameterFilterT) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJobStatusInTx", ctx, tx, statusList, customValFilters, parameterFilters)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateJobStatusInTx indicates an expected call of UpdateJobStatusInTx.
func (mr *MockJobsDBMockRecorder) UpdateJobStatusInTx(ctx, tx, statusList, customValFilters, parameterFilters any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJobStatusInTx", reflect.TypeOf((*MockJobsDB)(nil).UpdateJobStatusInTx), ctx, tx, statusList, customValFilters, parameterFilters)
}

// WithStoreSafeTx mocks base method.
func (m *MockJobsDB) WithStoreSafeTx(arg0 context.Context, arg1 func(jobsdb.StoreSafeTx) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithStoreSafeTx", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithStoreSafeTx indicates an expected call of WithStoreSafeTx.
func (mr *MockJobsDBMockRecorder) WithStoreSafeTx(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithStoreSafeTx", reflect.TypeOf((*MockJobsDB)(nil).WithStoreSafeTx), arg0, arg1)
}

// WithStoreSafeTxFromTx mocks base method.
func (m *MockJobsDB) WithStoreSafeTxFromTx(ctx context.Context, tx *tx.Tx, f func(jobsdb.StoreSafeTx) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithStoreSafeTxFromTx", ctx, tx, f)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithStoreSafeTxFromTx indicates an expected call of WithStoreSafeTxFromTx.
func (mr *MockJobsDBMockRecorder) WithStoreSafeTxFromTx(ctx, tx, f any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithStoreSafeTxFromTx", reflect.TypeOf((*MockJobsDB)(nil).WithStoreSafeTxFromTx), ctx, tx, f)
}

// WithTx mocks base method.
func (m *MockJobsDB) WithTx(arg0 func(*tx.Tx) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithTx", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithTx indicates an expected call of WithTx.
func (mr *MockJobsDBMockRecorder) WithTx(arg0 any) *gomock.Call {
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
func (mr *MockJobsDBMockRecorder) WithUpdateSafeTx(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithUpdateSafeTx", reflect.TypeOf((*MockJobsDB)(nil).WithUpdateSafeTx), arg0, arg1)
}
