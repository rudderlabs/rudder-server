package helper

type Debugger interface {
	Send(input, output any, meta MetaInfo)
	Shutdown()
}
