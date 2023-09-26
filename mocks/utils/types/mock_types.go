// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/utils/types (interfaces: UserSuppression,Reporting)

// Package mock_types is a generated GoMock package.
package mock_types

import (
	sql "database/sql"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	model "github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	types "github.com/rudderlabs/rudder-server/utils/types"
)

// MockUserSuppression is a mock of UserSuppression interface.
type MockUserSuppression struct {
	ctrl     *gomock.Controller
	recorder *MockUserSuppressionMockRecorder
}

// MockUserSuppressionMockRecorder is the mock recorder for MockUserSuppression.
type MockUserSuppressionMockRecorder struct {
	mock *MockUserSuppression
}

// NewMockUserSuppression creates a new mock instance.
func NewMockUserSuppression(ctrl *gomock.Controller) *MockUserSuppression {
	mock := &MockUserSuppression{ctrl: ctrl}
	mock.recorder = &MockUserSuppressionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockUserSuppression) EXPECT() *MockUserSuppressionMockRecorder {
	return m.recorder
}

// GetSuppressedUser mocks base method.
func (m *MockUserSuppression) GetSuppressedUser(arg0, arg1, arg2 string) *model.Metadata {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSuppressedUser", arg0, arg1, arg2)
	ret0, _ := ret[0].(*model.Metadata)
	return ret0
}

// GetSuppressedUser indicates an expected call of GetSuppressedUser.
func (mr *MockUserSuppressionMockRecorder) GetSuppressedUser(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSuppressedUser", reflect.TypeOf((*MockUserSuppression)(nil).GetSuppressedUser), arg0, arg1, arg2)
}

// MockReporting is a mock of Reporting interface.
type MockReporting struct {
	ctrl     *gomock.Controller
	recorder *MockReportingMockRecorder
}

// MockReportingMockRecorder is the mock recorder for MockReporting.
type MockReportingMockRecorder struct {
	mock *MockReporting
}

// NewMockReporting creates a new mock instance.
func NewMockReporting(ctrl *gomock.Controller) *MockReporting {
	mock := &MockReporting{ctrl: ctrl}
	mock.recorder = &MockReportingMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReporting) EXPECT() *MockReportingMockRecorder {
	return m.recorder
}

// DatabaseSyncer mocks base method.
func (m *MockReporting) DatabaseSyncer(arg0 types.SyncerConfig) types.ReportingSyncer {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DatabaseSyncer", arg0)
	ret0, _ := ret[0].(types.ReportingSyncer)
	return ret0
}

// DatabaseSyncer indicates an expected call of DatabaseSyncer.
func (mr *MockReportingMockRecorder) DatabaseSyncer(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DatabaseSyncer", reflect.TypeOf((*MockReporting)(nil).DatabaseSyncer), arg0)
}

// Report mocks base method.
func (m *MockReporting) Report(arg0 []*types.PUReportedMetric, arg1 *sql.Tx) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Report", arg0, arg1)
}

// Report indicates an expected call of Report.
func (mr *MockReportingMockRecorder) Report(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Report", reflect.TypeOf((*MockReporting)(nil).Report), arg0, arg1)
}
