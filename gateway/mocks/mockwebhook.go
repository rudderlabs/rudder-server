// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/gateway/webhook (interfaces: Gateway)

// Package mockwebhook is a generated GoMock package.
package mockwebhook

import (
	http "net/http"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	stats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	types "github.com/rudderlabs/rudder-server/gateway/internal/types"
	model "github.com/rudderlabs/rudder-server/gateway/webhook/model"
)

// MockGateway is a mock of Gateway interface.
type MockGateway struct {
	ctrl     *gomock.Controller
	recorder *MockGatewayMockRecorder
}

// MockGatewayMockRecorder is the mock recorder for MockGateway.
type MockGatewayMockRecorder struct {
	mock *MockGateway
}

// NewMockGateway creates a new mock instance.
func NewMockGateway(ctrl *gomock.Controller) *MockGateway {
	mock := &MockGateway{ctrl: ctrl}
	mock.recorder = &MockGatewayMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockGateway) EXPECT() *MockGatewayMockRecorder {
	return m.recorder
}

// GetSource mocks base method.
func (m *MockGateway) GetSource(arg0 string) (*backendconfig.SourceT, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSource", arg0)
	ret0, _ := ret[0].(*backendconfig.SourceT)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSource indicates an expected call of GetSource.
func (mr *MockGatewayMockRecorder) GetSource(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSource", reflect.TypeOf((*MockGateway)(nil).GetSource), arg0)
}

// NewSourceStat mocks base method.
func (m *MockGateway) NewSourceStat(arg0 *types.AuthRequestContext, arg1 string) *stats.SourceStat {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewSourceStat", arg0, arg1)
	ret0, _ := ret[0].(*stats.SourceStat)
	return ret0
}

// NewSourceStat indicates an expected call of NewSourceStat.
func (mr *MockGatewayMockRecorder) NewSourceStat(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewSourceStat", reflect.TypeOf((*MockGateway)(nil).NewSourceStat), arg0, arg1)
}

// ProcessWebRequest mocks base method.
func (m *MockGateway) ProcessWebRequest(arg0 *http.ResponseWriter, arg1 *http.Request, arg2 string, arg3 []byte, arg4 *types.AuthRequestContext) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProcessWebRequest", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(string)
	return ret0
}

// ProcessWebRequest indicates an expected call of ProcessWebRequest.
func (mr *MockGatewayMockRecorder) ProcessWebRequest(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProcessWebRequest", reflect.TypeOf((*MockGateway)(nil).ProcessWebRequest), arg0, arg1, arg2, arg3, arg4)
}

// SaveWebhookFailures mocks base method.
func (m *MockGateway) SaveWebhookFailures(arg0 []*model.FailedWebhookPayload) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SaveWebhookFailures", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SaveWebhookFailures indicates an expected call of SaveWebhookFailures.
func (mr *MockGatewayMockRecorder) SaveWebhookFailures(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SaveWebhookFailures", reflect.TypeOf((*MockGateway)(nil).SaveWebhookFailures), arg0)
}

// TrackRequestMetrics mocks base method.
func (m *MockGateway) TrackRequestMetrics(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "TrackRequestMetrics", arg0)
}

// TrackRequestMetrics indicates an expected call of TrackRequestMetrics.
func (mr *MockGatewayMockRecorder) TrackRequestMetrics(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TrackRequestMetrics", reflect.TypeOf((*MockGateway)(nil).TrackRequestMetrics), arg0)
}
