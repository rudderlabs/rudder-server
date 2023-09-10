// Code generated by MockGen. DO NOT EDIT.
// Source: /Users/sudippaul/workspace/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua/types.go

// Package mock_bulkservice is a generated GoMock package.
package mock_bulkservice

import (
	http "net/http"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	eloqua "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua"
)

// MockEloqua is a mock of Eloqua interface.
type MockEloqua struct {
	ctrl     *gomock.Controller
	recorder *MockEloquaMockRecorder
}

// MockEloquaMockRecorder is the mock recorder for MockEloqua.
type MockEloquaMockRecorder struct {
	mock *MockEloqua
}

// NewMockEloqua creates a new mock instance.
func NewMockEloqua(ctrl *gomock.Controller) *MockEloqua {
	mock := &MockEloqua{ctrl: ctrl}
	mock.recorder = &MockEloquaMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEloqua) EXPECT() *MockEloquaMockRecorder {
	return m.recorder
}

// CheckRejectedData mocks base method.
func (m *MockEloqua) CheckRejectedData(arg0 *eloqua.HttpRequestData) (*eloqua.RejectResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckRejectedData", arg0)
	ret0, _ := ret[0].(*eloqua.RejectResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckRejectedData indicates an expected call of CheckRejectedData.
func (mr *MockEloquaMockRecorder) CheckRejectedData(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckRejectedData", reflect.TypeOf((*MockEloqua)(nil).CheckRejectedData), arg0)
}

// CheckSyncStatus mocks base method.
func (m *MockEloqua) CheckSyncStatus(arg0 *eloqua.HttpRequestData) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckSyncStatus", arg0)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckSyncStatus indicates an expected call of CheckSyncStatus.
func (mr *MockEloquaMockRecorder) CheckSyncStatus(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckSyncStatus", reflect.TypeOf((*MockEloqua)(nil).CheckSyncStatus), arg0)
}

// CreateImportDefinition mocks base method.
func (m *MockEloqua) CreateImportDefinition(arg0 *eloqua.HttpRequestData, arg1 string) (*eloqua.ImportDefinition, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateImportDefinition", arg0, arg1)
	ret0, _ := ret[0].(*eloqua.ImportDefinition)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateImportDefinition indicates an expected call of CreateImportDefinition.
func (mr *MockEloquaMockRecorder) CreateImportDefinition(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateImportDefinition", reflect.TypeOf((*MockEloqua)(nil).CreateImportDefinition), arg0, arg1)
}

// DeleteImportDefinition mocks base method.
func (m *MockEloqua) DeleteImportDefinition(arg0 *eloqua.HttpRequestData) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteImportDefinition", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteImportDefinition indicates an expected call of DeleteImportDefinition.
func (mr *MockEloquaMockRecorder) DeleteImportDefinition(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteImportDefinition", reflect.TypeOf((*MockEloqua)(nil).DeleteImportDefinition), arg0)
}

// FetchFields mocks base method.
func (m *MockEloqua) FetchFields(arg0 *eloqua.HttpRequestData) (*eloqua.Fields, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchFields", arg0)
	ret0, _ := ret[0].(*eloqua.Fields)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FetchFields indicates an expected call of FetchFields.
func (mr *MockEloquaMockRecorder) FetchFields(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchFields", reflect.TypeOf((*MockEloqua)(nil).FetchFields), arg0)
}

// GetBaseEndpoint mocks base method.
func (m *MockEloqua) GetBaseEndpoint(arg0 *eloqua.HttpRequestData) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBaseEndpoint", arg0)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBaseEndpoint indicates an expected call of GetBaseEndpoint.
func (mr *MockEloquaMockRecorder) GetBaseEndpoint(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBaseEndpoint", reflect.TypeOf((*MockEloqua)(nil).GetBaseEndpoint), arg0)
}

// RunSync mocks base method.
func (m *MockEloqua) RunSync(arg0 *eloqua.HttpRequestData) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RunSync", arg0)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RunSync indicates an expected call of RunSync.
func (mr *MockEloquaMockRecorder) RunSync(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RunSync", reflect.TypeOf((*MockEloqua)(nil).RunSync), arg0)
}

// UploadData mocks base method.
func (m *MockEloqua) UploadData(arg0 *eloqua.HttpRequestData, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UploadData", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UploadData indicates an expected call of UploadData.
func (mr *MockEloquaMockRecorder) UploadData(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UploadData", reflect.TypeOf((*MockEloqua)(nil).UploadData), arg0, arg1)
}

// MockHttpClient is a mock of HttpClient interface.
type MockHttpClient struct {
	ctrl     *gomock.Controller
	recorder *MockHttpClientMockRecorder
}

// MockHttpClientMockRecorder is the mock recorder for MockHttpClient.
type MockHttpClientMockRecorder struct {
	mock *MockHttpClient
}

// NewMockHttpClient creates a new mock instance.
func NewMockHttpClient(ctrl *gomock.Controller) *MockHttpClient {
	mock := &MockHttpClient{ctrl: ctrl}
	mock.recorder = &MockHttpClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHttpClient) EXPECT() *MockHttpClientMockRecorder {
	return m.recorder
}

// Do mocks base method.
func (m *MockHttpClient) Do(arg0 *http.Request) (*http.Response, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Do", arg0)
	ret0, _ := ret[0].(*http.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Do indicates an expected call of Do.
func (mr *MockHttpClientMockRecorder) Do(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Do", reflect.TypeOf((*MockHttpClient)(nil).Do), arg0)
}
