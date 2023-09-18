package resource

type logger interface {
	Log(...interface{})
}

type cleaner interface {
	Cleanup(func())
	logger
}
