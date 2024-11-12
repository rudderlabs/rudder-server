// Code generated by MockGen. DO NOT EDIT.
// Source: net/http (interfaces: RoundTripper)
//
// Generated by this command:
//
//	mockgen -destination=../../../../mocks/services/oauthV2/mock_roundtripper.go -package=mock_oauthV2 net/http RoundTripper
//

// Package mock_oauthV2 is a generated GoMock package.
package mock_oauthV2

import (
	http "net/http"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockRoundTripper is a mock of RoundTripper interface.
type MockRoundTripper struct {
	ctrl     *gomock.Controller
	recorder *MockRoundTripperMockRecorder
	isgomock struct{}
}

// MockRoundTripperMockRecorder is the mock recorder for MockRoundTripper.
type MockRoundTripperMockRecorder struct {
	mock *MockRoundTripper
}

// NewMockRoundTripper creates a new mock instance.
func NewMockRoundTripper(ctrl *gomock.Controller) *MockRoundTripper {
	mock := &MockRoundTripper{ctrl: ctrl}
	mock.recorder = &MockRoundTripperMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRoundTripper) EXPECT() *MockRoundTripperMockRecorder {
	return m.recorder
}

// RoundTrip mocks base method.
func (m *MockRoundTripper) RoundTrip(arg0 *http.Request) (*http.Response, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RoundTrip", arg0)
	ret0, _ := ret[0].(*http.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RoundTrip indicates an expected call of RoundTrip.
func (mr *MockRoundTripperMockRecorder) RoundTrip(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RoundTrip", reflect.TypeOf((*MockRoundTripper)(nil).RoundTrip), arg0)
}
