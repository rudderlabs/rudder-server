// Code generated by MockGen. DO NOT EDIT.
// Source: ./backend-config.go
//
// Generated by this command:
//
//	mockgen -destination=./mock_workspaceconfig.go -package=backendconfig -source=./backend-config.go workspaceConfig
//

// Package backendconfig is a generated GoMock package.
package backendconfig

import (
	context "context"
	reflect "reflect"

	identity "github.com/rudderlabs/rudder-server/services/controlplane/identity"
	pubsub "github.com/rudderlabs/rudder-server/utils/pubsub"
	gomock "go.uber.org/mock/gomock"
)

// MockworkspaceConfig is a mock of workspaceConfig interface.
type MockworkspaceConfig struct {
	ctrl     *gomock.Controller
	recorder *MockworkspaceConfigMockRecorder
	isgomock struct{}
}

// MockworkspaceConfigMockRecorder is the mock recorder for MockworkspaceConfig.
type MockworkspaceConfigMockRecorder struct {
	mock *MockworkspaceConfig
}

// NewMockworkspaceConfig creates a new mock instance.
func NewMockworkspaceConfig(ctrl *gomock.Controller) *MockworkspaceConfig {
	mock := &MockworkspaceConfig{ctrl: ctrl}
	mock.recorder = &MockworkspaceConfigMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockworkspaceConfig) EXPECT() *MockworkspaceConfigMockRecorder {
	return m.recorder
}

// AccessToken mocks base method.
func (m *MockworkspaceConfig) AccessToken() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AccessToken")
	ret0, _ := ret[0].(string)
	return ret0
}

// AccessToken indicates an expected call of AccessToken.
func (mr *MockworkspaceConfigMockRecorder) AccessToken() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AccessToken", reflect.TypeOf((*MockworkspaceConfig)(nil).AccessToken))
}

// Get mocks base method.
func (m *MockworkspaceConfig) Get(arg0 context.Context) (map[string]ConfigT, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", arg0)
	ret0, _ := ret[0].(map[string]ConfigT)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockworkspaceConfigMockRecorder) Get(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockworkspaceConfig)(nil).Get), arg0)
}

// Identity mocks base method.
func (m *MockworkspaceConfig) Identity() identity.Identifier {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Identity")
	ret0, _ := ret[0].(identity.Identifier)
	return ret0
}

// Identity indicates an expected call of Identity.
func (mr *MockworkspaceConfigMockRecorder) Identity() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Identity", reflect.TypeOf((*MockworkspaceConfig)(nil).Identity))
}

// SetUp mocks base method.
func (m *MockworkspaceConfig) SetUp() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetUp")
	ret0, _ := ret[0].(error)
	return ret0
}

// SetUp indicates an expected call of SetUp.
func (mr *MockworkspaceConfigMockRecorder) SetUp() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetUp", reflect.TypeOf((*MockworkspaceConfig)(nil).SetUp))
}

// MockBackendConfig is a mock of BackendConfig interface.
type MockBackendConfig struct {
	ctrl     *gomock.Controller
	recorder *MockBackendConfigMockRecorder
	isgomock struct{}
}

// MockBackendConfigMockRecorder is the mock recorder for MockBackendConfig.
type MockBackendConfigMockRecorder struct {
	mock *MockBackendConfig
}

// NewMockBackendConfig creates a new mock instance.
func NewMockBackendConfig(ctrl *gomock.Controller) *MockBackendConfig {
	mock := &MockBackendConfig{ctrl: ctrl}
	mock.recorder = &MockBackendConfigMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBackendConfig) EXPECT() *MockBackendConfigMockRecorder {
	return m.recorder
}

// AccessToken mocks base method.
func (m *MockBackendConfig) AccessToken() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AccessToken")
	ret0, _ := ret[0].(string)
	return ret0
}

// AccessToken indicates an expected call of AccessToken.
func (mr *MockBackendConfigMockRecorder) AccessToken() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AccessToken", reflect.TypeOf((*MockBackendConfig)(nil).AccessToken))
}

// Get mocks base method.
func (m *MockBackendConfig) Get(arg0 context.Context) (map[string]ConfigT, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", arg0)
	ret0, _ := ret[0].(map[string]ConfigT)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockBackendConfigMockRecorder) Get(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockBackendConfig)(nil).Get), arg0)
}

// Identity mocks base method.
func (m *MockBackendConfig) Identity() identity.Identifier {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Identity")
	ret0, _ := ret[0].(identity.Identifier)
	return ret0
}

// Identity indicates an expected call of Identity.
func (mr *MockBackendConfigMockRecorder) Identity() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Identity", reflect.TypeOf((*MockBackendConfig)(nil).Identity))
}

// SetUp mocks base method.
func (m *MockBackendConfig) SetUp() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetUp")
	ret0, _ := ret[0].(error)
	return ret0
}

// SetUp indicates an expected call of SetUp.
func (mr *MockBackendConfigMockRecorder) SetUp() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetUp", reflect.TypeOf((*MockBackendConfig)(nil).SetUp))
}

// StartWithIDs mocks base method.
func (m *MockBackendConfig) StartWithIDs(ctx context.Context, workspaces string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "StartWithIDs", ctx, workspaces)
}

// StartWithIDs indicates an expected call of StartWithIDs.
func (mr *MockBackendConfigMockRecorder) StartWithIDs(ctx, workspaces any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartWithIDs", reflect.TypeOf((*MockBackendConfig)(nil).StartWithIDs), ctx, workspaces)
}

// Stop mocks base method.
func (m *MockBackendConfig) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockBackendConfigMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockBackendConfig)(nil).Stop))
}

// Subscribe mocks base method.
func (m *MockBackendConfig) Subscribe(ctx context.Context, topic Topic) pubsub.DataChannel {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Subscribe", ctx, topic)
	ret0, _ := ret[0].(pubsub.DataChannel)
	return ret0
}

// Subscribe indicates an expected call of Subscribe.
func (mr *MockBackendConfigMockRecorder) Subscribe(ctx, topic any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Subscribe", reflect.TypeOf((*MockBackendConfig)(nil).Subscribe), ctx, topic)
}

// WaitForConfig mocks base method.
func (m *MockBackendConfig) WaitForConfig(ctx context.Context) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "WaitForConfig", ctx)
}

// WaitForConfig indicates an expected call of WaitForConfig.
func (mr *MockBackendConfigMockRecorder) WaitForConfig(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WaitForConfig", reflect.TypeOf((*MockBackendConfig)(nil).WaitForConfig), ctx)
}
