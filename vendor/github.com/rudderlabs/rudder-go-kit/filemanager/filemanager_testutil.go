package filemanager

// MockListSession is a mock implementation of ListSession that always returns the given file objects and error
func MockListSession(fileObjects []*FileInfo, err error) ListSession {
	return mockListSession{
		fileObjects: fileObjects,
		err:         err,
	}
}

type mockListSession struct {
	fileObjects []*FileInfo
	err         error
}

func (m mockListSession) Next() (fileObjects []*FileInfo, err error) {
	return m.fileObjects, m.err
}
