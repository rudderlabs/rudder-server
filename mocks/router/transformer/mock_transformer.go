// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/router/transformer (interfaces: Transformer)

// Package mocks_transformer is a generated GoMock package.
package mocks_transformer

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	integrations "github.com/rudderlabs/rudder-server/processor/integrations"
	types "github.com/rudderlabs/rudder-server/router/types"
)

// MockTransformer is a mock of Transformer interface.
type MockTransformer struct {
	ctrl     *gomock.Controller
	recorder *MockTransformerMockRecorder
}

// MockTransformerMockRecorder is the mock recorder for MockTransformer.
type MockTransformerMockRecorder struct {
	mock *MockTransformer
}

// NewMockTransformer creates a new mock instance.
func NewMockTransformer(ctrl *gomock.Controller) *MockTransformer {
	mock := &MockTransformer{ctrl: ctrl}
	mock.recorder = &MockTransformerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTransformer) EXPECT() *MockTransformerMockRecorder {
	return m.recorder
}

// ProxyRequest mocks base method.
func (m *MockTransformer) ProxyRequest(arg0 context.Context, arg1 integrations.PostParametersT, arg2 string) (int, string) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProxyRequest", arg0, arg1, arg2)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(string)
	return ret0, ret1
}

// ProxyRequest indicates an expected call of ProxyRequest.
func (mr *MockTransformerMockRecorder) ProxyRequest(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProxyRequest", reflect.TypeOf((*MockTransformer)(nil).ProxyRequest), arg0, arg1, arg2)
}

// Setup mocks base method.
func (m *MockTransformer) Setup() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Setup")
}

// Setup indicates an expected call of Setup.
func (mr *MockTransformerMockRecorder) Setup() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Setup", reflect.TypeOf((*MockTransformer)(nil).Setup))
}

// Transform mocks base method.
func (m *MockTransformer) Transform(arg0 string, arg1 *types.TransformMessageT) []types.DestinationJobT {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Transform", arg0, arg1)
	ret0, _ := ret[0].([]types.DestinationJobT)
	return ret0
}

// Transform indicates an expected call of Transform.
func (mr *MockTransformerMockRecorder) Transform(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Transform", reflect.TypeOf((*MockTransformer)(nil).Transform), arg0, arg1)
}
