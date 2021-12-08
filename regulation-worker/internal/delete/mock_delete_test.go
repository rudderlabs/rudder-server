// Code generated by MockGen. DO NOT EDIT.
// Source: delete.go

// Package delete is a generated GoMock package.
package delete

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	model "github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

// MockdeleteManager is a mock of deleteManager interface.
type MockdeleteManager struct {
	ctrl     *gomock.Controller
	recorder *MockdeleteManagerMockRecorder
}

// MockdeleteManagerMockRecorder is the mock recorder for MockdeleteManager.
type MockdeleteManagerMockRecorder struct {
	mock *MockdeleteManager
}

// NewMockdeleteManager creates a new mock instance.
func NewMockdeleteManager(ctrl *gomock.Controller) *MockdeleteManager {
	mock := &MockdeleteManager{ctrl: ctrl}
	mock.recorder = &MockdeleteManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockdeleteManager) EXPECT() *MockdeleteManagerMockRecorder {
	return m.recorder
}

// Delete mocks base method.
func (m *MockdeleteManager) Delete(ctx context.Context, job model.Job, destConfig map[string]interface{}, destName string) model.JobStatus {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", ctx, job, destConfig, destName)
	ret0, _ := ret[0].(model.JobStatus)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockdeleteManagerMockRecorder) Delete(ctx, job, destConfig, destName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockdeleteManager)(nil).Delete), ctx, job, destConfig, destName)
}

// GetSupportedDestination mocks base method.
func (m *MockdeleteManager) GetSupportedDestination() []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSupportedDestination")
	ret0, _ := ret[0].([]string)
	return ret0
}

// GetSupportedDestination indicates an expected call of GetSupportedDestination.
func (mr *MockdeleteManagerMockRecorder) GetSupportedDestination() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSupportedDestination", reflect.TypeOf((*MockdeleteManager)(nil).GetSupportedDestination))
}
