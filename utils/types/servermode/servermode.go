package servermode

type Mode string

const (
	NormalMode    Mode = "NORMAL"
	DegradedMode  Mode = "DEGRADED"
)

type ModeAck struct {
	ack  func()
	mode Mode
}

func (m ModeAck) Ack() {
	m.ack()
}

func (m ModeAck) Mode() Mode {
	return m.mode
}

func WithACK(mode Mode, ack func()) ModeAck {
	return ModeAck{
		mode: mode,
		ack:  ack,
	}
}
