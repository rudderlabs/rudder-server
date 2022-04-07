package servermode

type Mode string

const (
	NormalMode   Mode = "NORMAL"
	DegradedMode Mode = "DEGRADED"
)

type ModeRequest struct {
	err  error
	ack  func() error
	mode Mode
}

func (m ModeRequest) Ack() error {
	return m.ack()
}

func (m ModeRequest) Mode() Mode {
	return m.mode
}

func (m ModeRequest) Err() error {
	return m.err
}

func NewModeRequest(mode Mode, ack func() error) ModeRequest {
	return ModeRequest{
		mode: mode,
		ack:  ack,
	}
}

func ModeError(err error) ModeRequest {
	return ModeRequest{
		err: err,
	}
}

func (mode Mode) Valid() bool {
	if mode == NormalMode || mode == DegradedMode {
		return true
	}
	return false
}
