package httputil

import (
	"context"
	"time"
)

type server interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

func GracefulListenAndServe(ctx context.Context, server server, shutdownTimeout ...time.Duration) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		var (
			shutdownCtx context.Context
			cancel      context.CancelFunc
		)
		if len(shutdownTimeout) > 0 {
			shutdownCtx, cancel = context.WithTimeout(context.Background(), shutdownTimeout[0])
		} else {
			shutdownCtx, cancel = context.WithCancel(context.Background())
		}
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
	}
	return nil
}
