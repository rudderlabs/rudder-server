// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rudderlabs/rudder-server/utils/sysUtils (interfaces: ZipI)
//
// Generated by this command:
//
//	mockgen -destination=../../mocks/utils/sysUtils/mock_archive.go -package mock_sysUtils github.com/rudderlabs/rudder-server/utils/sysUtils ZipI
//

// Package mock_sysUtils is a generated GoMock package.
package mock_sysUtils

import (
	zip "archive/zip"
	io "io"
	fs "io/fs"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockZipI is a mock of ZipI interface.
type MockZipI struct {
	ctrl     *gomock.Controller
	recorder *MockZipIMockRecorder
	isgomock struct{}
}

// MockZipIMockRecorder is the mock recorder for MockZipI.
type MockZipIMockRecorder struct {
	mock *MockZipI
}

// NewMockZipI creates a new mock instance.
func NewMockZipI(ctrl *gomock.Controller) *MockZipI {
	mock := &MockZipI{ctrl: ctrl}
	mock.recorder = &MockZipIMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockZipI) EXPECT() *MockZipIMockRecorder {
	return m.recorder
}

// FileInfoHeader mocks base method.
func (m *MockZipI) FileInfoHeader(fi fs.FileInfo) (*zip.FileHeader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FileInfoHeader", fi)
	ret0, _ := ret[0].(*zip.FileHeader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FileInfoHeader indicates an expected call of FileInfoHeader.
func (mr *MockZipIMockRecorder) FileInfoHeader(fi any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FileInfoHeader", reflect.TypeOf((*MockZipI)(nil).FileInfoHeader), fi)
}

// NewWriter mocks base method.
func (m *MockZipI) NewWriter(w io.Writer) *zip.Writer {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewWriter", w)
	ret0, _ := ret[0].(*zip.Writer)
	return ret0
}

// NewWriter indicates an expected call of NewWriter.
func (mr *MockZipIMockRecorder) NewWriter(w any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewWriter", reflect.TypeOf((*MockZipI)(nil).NewWriter), w)
}

// OpenReader mocks base method.
func (m *MockZipI) OpenReader(name string) (*zip.ReadCloser, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OpenReader", name)
	ret0, _ := ret[0].(*zip.ReadCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OpenReader indicates an expected call of OpenReader.
func (mr *MockZipIMockRecorder) OpenReader(name any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OpenReader", reflect.TypeOf((*MockZipI)(nil).OpenReader), name)
}
