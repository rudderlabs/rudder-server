// Code generated by MockGen. DO NOT EDIT.
// Source: ./backend-config.go

// Package mock_backendconfig is a generated GoMock package.
package mock_backendconfig

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	pubsub "github.com/rudderlabs/rudder-server/utils/pubsub"
)

// MockworkspaceConfig is a mock of workspaceConfig interface.
type MockworkspaceConfig struct {
	ctrl     *gomock.Controller
	recorder *MockworkspaceConfigMockRecorder
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
func (m *MockworkspaceConfig) Get(arg0 context.Context, arg1 string) (backendconfig.ConfigT, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", arg0, arg1)
	ret0, _ := ret[0].(backendconfig.ConfigT)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockworkspaceConfigMockRecorder) Get(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockworkspaceConfig)(nil).Get), arg0, arg1)
}

// GetWorkspaceIDForSourceID mocks base method.
func (m *MockworkspaceConfig) GetWorkspaceIDForSourceID(arg0 string) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkspaceIDForSourceID", arg0)
	ret0, _ := ret[0].(string)
	return ret0
}

// GetWorkspaceIDForSourceID indicates an expected call of GetWorkspaceIDForSourceID.
func (mr *MockworkspaceConfigMockRecorder) GetWorkspaceIDForSourceID(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkspaceIDForSourceID", reflect.TypeOf((*MockworkspaceConfig)(nil).GetWorkspaceIDForSourceID), arg0)
}

// GetWorkspaceIDForWriteKey mocks base method.
func (m *MockworkspaceConfig) GetWorkspaceIDForWriteKey(arg0 string) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkspaceIDForWriteKey", arg0)
	ret0, _ := ret[0].(string)
	return ret0
}

// GetWorkspaceIDForWriteKey indicates an expected call of GetWorkspaceIDForWriteKey.
func (mr *MockworkspaceConfigMockRecorder) GetWorkspaceIDForWriteKey(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkspaceIDForWriteKey", reflect.TypeOf((*MockworkspaceConfig)(nil).GetWorkspaceIDForWriteKey), arg0)
}

// GetWorkspaceLibrariesForWorkspaceID mocks base method.
func (m *MockworkspaceConfig) GetWorkspaceLibrariesForWorkspaceID(arg0 string) backendconfig.LibrariesT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkspaceLibrariesForWorkspaceID", arg0)
	ret0, _ := ret[0].(backendconfig.LibrariesT)
	return ret0
}

// GetWorkspaceLibrariesForWorkspaceID indicates an expected call of GetWorkspaceLibrariesForWorkspaceID.
func (mr *MockworkspaceConfigMockRecorder) GetWorkspaceLibrariesForWorkspaceID(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkspaceLibrariesForWorkspaceID", reflect.TypeOf((*MockworkspaceConfig)(nil).GetWorkspaceLibrariesForWorkspaceID), arg0)
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
func (m *MockBackendConfig) Get(arg0 context.Context, arg1 string) (backendconfig.ConfigT, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", arg0, arg1)
	ret0, _ := ret[0].(backendconfig.ConfigT)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockBackendConfigMockRecorder) Get(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockBackendConfig)(nil).Get), arg0, arg1)
}

// GetConfig mocks base method.
func (m *MockBackendConfig) GetConfig() backendconfig.ConfigT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetConfig")
	ret0, _ := ret[0].(backendconfig.ConfigT)
	return ret0
}

// GetConfig indicates an expected call of GetConfig.
func (mr *MockBackendConfigMockRecorder) GetConfig() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetConfig", reflect.TypeOf((*MockBackendConfig)(nil).GetConfig))
}

// GetWorkspaceIDForSourceID mocks base method.
func (m *MockBackendConfig) GetWorkspaceIDForSourceID(arg0 string) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkspaceIDForSourceID", arg0)
	ret0, _ := ret[0].(string)
	return ret0
}

// GetWorkspaceIDForSourceID indicates an expected call of GetWorkspaceIDForSourceID.
func (mr *MockBackendConfigMockRecorder) GetWorkspaceIDForSourceID(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkspaceIDForSourceID", reflect.TypeOf((*MockBackendConfig)(nil).GetWorkspaceIDForSourceID), arg0)
}

// GetWorkspaceIDForWriteKey mocks base method.
func (m *MockBackendConfig) GetWorkspaceIDForWriteKey(arg0 string) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkspaceIDForWriteKey", arg0)
	ret0, _ := ret[0].(string)
	return ret0
}

// GetWorkspaceIDForWriteKey indicates an expected call of GetWorkspaceIDForWriteKey.
func (mr *MockBackendConfigMockRecorder) GetWorkspaceIDForWriteKey(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkspaceIDForWriteKey", reflect.TypeOf((*MockBackendConfig)(nil).GetWorkspaceIDForWriteKey), arg0)
}

// GetWorkspaceLibrariesForWorkspaceID mocks base method.
func (m *MockBackendConfig) GetWorkspaceLibrariesForWorkspaceID(arg0 string) backendconfig.LibrariesT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkspaceLibrariesForWorkspaceID", arg0)
	ret0, _ := ret[0].(backendconfig.LibrariesT)
	return ret0
}

// GetWorkspaceLibrariesForWorkspaceID indicates an expected call of GetWorkspaceLibrariesForWorkspaceID.
func (mr *MockBackendConfigMockRecorder) GetWorkspaceLibrariesForWorkspaceID(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkspaceLibrariesForWorkspaceID", reflect.TypeOf((*MockBackendConfig)(nil).GetWorkspaceLibrariesForWorkspaceID), arg0)
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
func (mr *MockBackendConfigMockRecorder) StartWithIDs(ctx, workspaces interface{}) *gomock.Call {
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
func (m *MockBackendConfig) Subscribe(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Subscribe", ctx, topic)
	ret0, _ := ret[0].(pubsub.DataChannel)
	return ret0
}

// Subscribe indicates an expected call of Subscribe.
func (mr *MockBackendConfigMockRecorder) Subscribe(ctx, topic interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Subscribe", reflect.TypeOf((*MockBackendConfig)(nil).Subscribe), ctx, topic)
}

// WaitForConfig mocks base method.
func (m *MockBackendConfig) WaitForConfig(ctx context.Context) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "WaitForConfig", ctx)
}

// WaitForConfig indicates an expected call of WaitForConfig.
func (mr *MockBackendConfigMockRecorder) WaitForConfig(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WaitForConfig", reflect.TypeOf((*MockBackendConfig)(nil).WaitForConfig), ctx)
}
