// Code generated by MockGen. DO NOT EDIT.
// Source: destination.go

// Package destination is a generated GoMock package.
package destination

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	identity "github.com/rudderlabs/rudder-server/services/controlplane/identity"
	pubsub "github.com/rudderlabs/rudder-server/utils/pubsub"
)

// MockdestMiddleware is a mock of destMiddleware interface.
type MockdestMiddleware struct {
	ctrl     *gomock.Controller
	recorder *MockdestMiddlewareMockRecorder
}

// MockdestMiddlewareMockRecorder is the mock recorder for MockdestMiddleware.
type MockdestMiddlewareMockRecorder struct {
	mock *MockdestMiddleware
}

// NewMockdestMiddleware creates a new mock instance.
func NewMockdestMiddleware(ctrl *gomock.Controller) *MockdestMiddleware {
	mock := &MockdestMiddleware{ctrl: ctrl}
	mock.recorder = &MockdestMiddlewareMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockdestMiddleware) EXPECT() *MockdestMiddlewareMockRecorder {
	return m.recorder
}

// Identity mocks base method.
func (m *MockdestMiddleware) Identity() identity.Identifier {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Identity")
	ret0, _ := ret[0].(identity.Identifier)
	return ret0
}

// Identity indicates an expected call of Identity.
func (mr *MockdestMiddlewareMockRecorder) Identity() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Identity", reflect.TypeOf((*MockdestMiddleware)(nil).Identity))
}

// StartWithIDs mocks base method.
func (m *MockdestMiddleware) StartWithIDs(ctx context.Context, workspaces string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "StartWithIDs", ctx, workspaces)
}

// StartWithIDs indicates an expected call of StartWithIDs.
func (mr *MockdestMiddlewareMockRecorder) StartWithIDs(ctx, workspaces interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartWithIDs", reflect.TypeOf((*MockdestMiddleware)(nil).StartWithIDs), ctx, workspaces)
}

// Subscribe mocks base method.
func (m *MockdestMiddleware) Subscribe(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Subscribe", ctx, topic)
	ret0, _ := ret[0].(pubsub.DataChannel)
	return ret0
}

// Subscribe indicates an expected call of Subscribe.
func (mr *MockdestMiddlewareMockRecorder) Subscribe(ctx, topic interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Subscribe", reflect.TypeOf((*MockdestMiddleware)(nil).Subscribe), ctx, topic)
}
