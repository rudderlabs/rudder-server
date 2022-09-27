// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/app (interfaces: App)

// Package mock_app is a generated GoMock package.
package mock_app

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	app "github.com/rudderlabs/rudder-server/app"
)

// MockApp is a mock of App interface.
type MockApp struct {
	ctrl     *gomock.Controller
	recorder *MockAppMockRecorder
}

// MockAppMockRecorder is the mock recorder for MockApp.
type MockAppMockRecorder struct {
	mock *MockApp
}

// NewMockApp creates a new mock instance.
func NewMockApp(ctrl *gomock.Controller) *MockApp {
	mock := &MockApp{ctrl: ctrl}
	mock.recorder = &MockAppMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockApp) EXPECT() *MockAppMockRecorder {
	return m.recorder
}

// Features mocks base method.
func (m *MockApp) Features() *app.Features {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Features")
	ret0, _ := ret[0].(*app.Features)
	return ret0
}

// Features indicates an expected call of Features.
func (mr *MockAppMockRecorder) Features() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Features", reflect.TypeOf((*MockApp)(nil).Features))
}

// Options mocks base method.
func (m *MockApp) Options() *app.Options {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Options")
	ret0, _ := ret[0].(*app.Options)
	return ret0
}

// Options indicates an expected call of Options.
func (mr *MockAppMockRecorder) Options() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Options", reflect.TypeOf((*MockApp)(nil).Options))
}

// Setup mocks base method.
func (m *MockApp) Setup() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Setup")
}

// Setup indicates an expected call of Setup.
func (mr *MockAppMockRecorder) Setup() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Setup", reflect.TypeOf((*MockApp)(nil).Setup))
}

// Stop mocks base method.
func (m *MockApp) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockAppMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockApp)(nil).Stop))
}
