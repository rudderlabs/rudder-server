// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/services/streammanager/personalize (interfaces: PersonalizeClient)
//
// Generated by this command:
//
//	mockgen -destination=../../../mocks/services/streammanager/personalize/mock_personalize.go -package mock_personalize github.com/rudderlabs/rudder-server/services/streammanager/personalize PersonalizeClient
//

// Package mock_personalize is a generated GoMock package.
package mock_personalize

import (
	reflect "reflect"

	personalizeevents "github.com/aws/aws-sdk-go/service/personalizeevents"
	gomock "go.uber.org/mock/gomock"
)

// MockPersonalizeClient is a mock of PersonalizeClient interface.
type MockPersonalizeClient struct {
	ctrl     *gomock.Controller
	recorder *MockPersonalizeClientMockRecorder
	isgomock struct{}
}

// MockPersonalizeClientMockRecorder is the mock recorder for MockPersonalizeClient.
type MockPersonalizeClientMockRecorder struct {
	mock *MockPersonalizeClient
}

// NewMockPersonalizeClient creates a new mock instance.
func NewMockPersonalizeClient(ctrl *gomock.Controller) *MockPersonalizeClient {
	mock := &MockPersonalizeClient{ctrl: ctrl}
	mock.recorder = &MockPersonalizeClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPersonalizeClient) EXPECT() *MockPersonalizeClientMockRecorder {
	return m.recorder
}

// PutEvents mocks base method.
func (m *MockPersonalizeClient) PutEvents(input *personalizeevents.PutEventsInput) (*personalizeevents.PutEventsOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutEvents", input)
	ret0, _ := ret[0].(*personalizeevents.PutEventsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PutEvents indicates an expected call of PutEvents.
func (mr *MockPersonalizeClientMockRecorder) PutEvents(input any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutEvents", reflect.TypeOf((*MockPersonalizeClient)(nil).PutEvents), input)
}

// PutItems mocks base method.
func (m *MockPersonalizeClient) PutItems(input *personalizeevents.PutItemsInput) (*personalizeevents.PutItemsOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutItems", input)
	ret0, _ := ret[0].(*personalizeevents.PutItemsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PutItems indicates an expected call of PutItems.
func (mr *MockPersonalizeClientMockRecorder) PutItems(input any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutItems", reflect.TypeOf((*MockPersonalizeClient)(nil).PutItems), input)
}

// PutUsers mocks base method.
func (m *MockPersonalizeClient) PutUsers(input *personalizeevents.PutUsersInput) (*personalizeevents.PutUsersOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutUsers", input)
	ret0, _ := ret[0].(*personalizeevents.PutUsersOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PutUsers indicates an expected call of PutUsers.
func (mr *MockPersonalizeClientMockRecorder) PutUsers(input any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutUsers", reflect.TypeOf((*MockPersonalizeClient)(nil).PutUsers), input)
}
