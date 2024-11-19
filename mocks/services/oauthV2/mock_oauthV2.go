// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/services/oauth/v2 (interfaces: TokenProvider)
//
// Generated by this command:
//
//	mockgen -destination=../../../mocks/services/oauthV2/mock_oauthV2.go -package=mock_oauthV2 github.com/rudderlabs/rudder-server/services/oauth/v2 TokenProvider
//

// Package mock_oauthV2 is a generated GoMock package.
package mock_oauthV2

import (
	reflect "reflect"

	identity "github.com/rudderlabs/rudder-server/services/controlplane/identity"
	gomock "go.uber.org/mock/gomock"
)

// MockTokenProvider is a mock of TokenProvider interface.
type MockTokenProvider struct {
	ctrl     *gomock.Controller
	recorder *MockTokenProviderMockRecorder
	isgomock struct{}
}

// MockTokenProviderMockRecorder is the mock recorder for MockTokenProvider.
type MockTokenProviderMockRecorder struct {
	mock *MockTokenProvider
}

// NewMockTokenProvider creates a new mock instance.
func NewMockTokenProvider(ctrl *gomock.Controller) *MockTokenProvider {
	mock := &MockTokenProvider{ctrl: ctrl}
	mock.recorder = &MockTokenProviderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTokenProvider) EXPECT() *MockTokenProviderMockRecorder {
	return m.recorder
}

// Identity mocks base method.
func (m *MockTokenProvider) Identity() identity.Identifier {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Identity")
	ret0, _ := ret[0].(identity.Identifier)
	return ret0
}

// Identity indicates an expected call of Identity.
func (mr *MockTokenProviderMockRecorder) Identity() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Identity", reflect.TypeOf((*MockTokenProvider)(nil).Identity))
}
