// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/utils/types (interfaces: ReportingTypesI)

// Package mock_types is a generated GoMock package.
package mock_types

import (
	json "encoding/json"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	types "github.com/rudderlabs/rudder-server/utils/types"
)

// MockReportingTypesI is a mock of ReportingTypesI interface.
type MockReportingTypesI struct {
	ctrl     *gomock.Controller
	recorder *MockReportingTypesIMockRecorder
}

// MockReportingTypesIMockRecorder is the mock recorder for MockReportingTypesI.
type MockReportingTypesIMockRecorder struct {
	mock *MockReportingTypesI
}

// NewMockReportingTypesI creates a new mock instance.
func NewMockReportingTypesI(ctrl *gomock.Controller) *MockReportingTypesI {
	mock := &MockReportingTypesI{ctrl: ctrl}
	mock.recorder = &MockReportingTypesIMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReportingTypesI) EXPECT() *MockReportingTypesIMockRecorder {
	return m.recorder
}

// AssertSameKeys mocks base method.
func (m *MockReportingTypesI) AssertSameKeys(arg0 map[string]*types.ConnectionDetails, arg1 map[string]*types.StatusDetail) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AssertSameKeys", arg0, arg1)
}

// AssertSameKeys indicates an expected call of AssertSameKeys.
func (mr *MockReportingTypesIMockRecorder) AssertSameKeys(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AssertSameKeys", reflect.TypeOf((*MockReportingTypesI)(nil).AssertSameKeys), arg0, arg1)
}

// CreateConnectionDetail mocks base method.
func (m *MockReportingTypesI) CreateConnectionDetail(arg0, arg1, arg2, arg3, arg4, arg5, arg6 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "CreateConnectionDetail", arg0, arg1, arg2, arg3, arg4, arg5, arg6)
}

// CreateConnectionDetail indicates an expected call of CreateConnectionDetail.
func (mr *MockReportingTypesIMockRecorder) CreateConnectionDetail(arg0, arg1, arg2, arg3, arg4, arg5, arg6 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateConnectionDetail", reflect.TypeOf((*MockReportingTypesI)(nil).CreateConnectionDetail), arg0, arg1, arg2, arg3, arg4, arg5, arg6)
}

// CreatePUDetails mocks base method.
func (m *MockReportingTypesI) CreatePUDetails(arg0, arg1 string, arg2, arg3 bool) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "CreatePUDetails", arg0, arg1, arg2, arg3)
}

// CreatePUDetails indicates an expected call of CreatePUDetails.
func (mr *MockReportingTypesIMockRecorder) CreatePUDetails(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreatePUDetails", reflect.TypeOf((*MockReportingTypesI)(nil).CreatePUDetails), arg0, arg1, arg2, arg3)
}

// CreateStatusDetail mocks base method.
func (m *MockReportingTypesI) CreateStatusDetail(arg0 string, arg1 int64, arg2 int, arg3 string, arg4 json.RawMessage) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "CreateStatusDetail", arg0, arg1, arg2, arg3, arg4)
}

// CreateStatusDetail indicates an expected call of CreateStatusDetail.
func (mr *MockReportingTypesIMockRecorder) CreateStatusDetail(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateStatusDetail", reflect.TypeOf((*MockReportingTypesI)(nil).CreateStatusDetail), arg0, arg1, arg2, arg3, arg4)
}
