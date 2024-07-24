// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/utils/sysUtils (interfaces: HTTPClientI)
//
// Generated by this command:
//
//	mockgen -destination=../../mocks/utils/sysUtils/mock_httpclient.go -package mock_sysUtils github.com/rudderlabs/rudder-server/utils/sysUtils HTTPClientI
//

// Package mock_sysUtils is a generated GoMock package.
package mock_sysUtils

import (
	http "net/http"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockHTTPClientI is a mock of HTTPClientI interface.
type MockHTTPClientI struct {
	ctrl     *gomock.Controller
	recorder *MockHTTPClientIMockRecorder
}

// MockHTTPClientIMockRecorder is the mock recorder for MockHTTPClientI.
type MockHTTPClientIMockRecorder struct {
	mock *MockHTTPClientI
}

// NewMockHTTPClientI creates a new mock instance.
func NewMockHTTPClientI(ctrl *gomock.Controller) *MockHTTPClientI {
	mock := &MockHTTPClientI{ctrl: ctrl}
	mock.recorder = &MockHTTPClientIMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHTTPClientI) EXPECT() *MockHTTPClientIMockRecorder {
	return m.recorder
}

// Do mocks base method.
func (m *MockHTTPClientI) Do(arg0 *http.Request) (*http.Response, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Do", arg0)
	ret0, _ := ret[0].(*http.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Do indicates an expected call of Do.
func (mr *MockHTTPClientIMockRecorder) Do(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Do", reflect.TypeOf((*MockHTTPClientI)(nil).Do), arg0)
}
