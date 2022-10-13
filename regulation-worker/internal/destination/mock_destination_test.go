// Code generated by MockGen. DO NOT EDIT.
// Source: destination.go

// Package destination is a generated GoMock package.
package destination

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
)

// MockdestinationMiddleware is a mock of destinationMiddleware interface.
type MockdestinationMiddleware struct {
	ctrl     *gomock.Controller
	recorder *MockdestinationMiddlewareMockRecorder
}

// MockdestinationMiddlewareMockRecorder is the mock recorder for MockdestinationMiddleware.
type MockdestinationMiddlewareMockRecorder struct {
	mock *MockdestinationMiddleware
}

// NewMockdestinationMiddleware creates a new mock instance.
func NewMockdestinationMiddleware(ctrl *gomock.Controller) *MockdestinationMiddleware {
	mock := &MockdestinationMiddleware{ctrl: ctrl}
	mock.recorder = &MockdestinationMiddlewareMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockdestinationMiddleware) EXPECT() *MockdestinationMiddlewareMockRecorder {
	return m.recorder
}

// Get mocks base method.
func (m *MockdestinationMiddleware) Get(ctx context.Context, workspace string) (map[string]backendconfig.ConfigT, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", ctx, workspace)
	ret0, _ := ret[0].(map[string]backendconfig.ConfigT)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockdestinationMiddlewareMockRecorder) Get(ctx, workspace interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockdestinationMiddleware)(nil).Get), ctx, workspace)
}
