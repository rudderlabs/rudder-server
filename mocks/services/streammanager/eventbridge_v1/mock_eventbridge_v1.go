// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/services/streammanager/eventbridge (interfaces: EventBridgeClientV1)
//
// Generated by this command:
//
//	mockgen -destination=../../../mocks/services/streammanager/eventbridge_v1/mock_eventbridge_v1.go -package mock_eventbridge_v1 github.com/rudderlabs/rudder-server/services/streammanager/eventbridge EventBridgeClientV1
//

// Package mock_eventbridge_v1 is a generated GoMock package.
package mock_eventbridge_v1

import (
	reflect "reflect"

	eventbridge "github.com/aws/aws-sdk-go/service/eventbridge"
	gomock "go.uber.org/mock/gomock"
)

// MockEventBridgeClientV1 is a mock of EventBridgeClientV1 interface.
type MockEventBridgeClientV1 struct {
	ctrl     *gomock.Controller
	recorder *MockEventBridgeClientV1MockRecorder
	isgomock struct{}
}

// MockEventBridgeClientV1MockRecorder is the mock recorder for MockEventBridgeClientV1.
type MockEventBridgeClientV1MockRecorder struct {
	mock *MockEventBridgeClientV1
}

// NewMockEventBridgeClientV1 creates a new mock instance.
func NewMockEventBridgeClientV1(ctrl *gomock.Controller) *MockEventBridgeClientV1 {
	mock := &MockEventBridgeClientV1{ctrl: ctrl}
	mock.recorder = &MockEventBridgeClientV1MockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEventBridgeClientV1) EXPECT() *MockEventBridgeClientV1MockRecorder {
	return m.recorder
}

// PutEvents mocks base method.
func (m *MockEventBridgeClientV1) PutEvents(input *eventbridge.PutEventsInput) (*eventbridge.PutEventsOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutEvents", input)
	ret0, _ := ret[0].(*eventbridge.PutEventsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PutEvents indicates an expected call of PutEvents.
func (mr *MockEventBridgeClientV1MockRecorder) PutEvents(input any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutEvents", reflect.TypeOf((*MockEventBridgeClientV1)(nil).PutEvents), input)
}
