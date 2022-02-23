package processor

import (
	"context"
)

func (p *HandleT) Run(ctx context.Context) error {
	return nil
}

func (p *HandleT) StartNew() {
	p.Start(context.Background())
}

func (p *HandleT) Stop() {
	p.Shutdown()
	// maybe use context here and don't use the existing stop method.
}
