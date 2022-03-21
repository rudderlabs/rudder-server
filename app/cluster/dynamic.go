package cluster

import "context"

type ModeChange interface {
	Value() string
	Ack() error
}

type modeProvider interface {
	ServerMode() <-chan ModeChange
}

type Dynamic struct {
	mode     string
	Provider modeProvider
}

func (d *Dynamic) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case newMode := <-d.Provider.ServerMode():
			err := d.handleModeChange(newMode.Value())
			if err != nil {

			}
			newMode.Ack()
		}
	}
}

func (d *Dynamic) handleModeChange(newMode string) error {
	return nil
}
