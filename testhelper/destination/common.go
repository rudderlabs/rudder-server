package destination

type Logger interface {
	Log(...interface{})
}

type Cleaner interface {
	Cleanup(func())
	Logger
}

type NOPLogger struct{}

func (*NOPLogger) Log(...interface{}) {}
