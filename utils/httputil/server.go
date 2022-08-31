package httputil

import (
	"context"
	"net"
	"net/http"
	"time"
)

type grace interface {
	Shutdown(context.Context) error
}

func GracefulListenAndServe(ctx context.Context, server *http.Server, shutdownTimeout ...time.Duration) error {
	return gracefulFunc(ctx, server, server.ListenAndServe, shutdownTimeout...)
}

func GracefulServe(ctx context.Context, server *http.Server, l net.Listener, shutdownTimeout ...time.Duration) error {
	fn := func() error {
		return server.Serve(l)
	}
	return gracefulFunc(ctx, server, fn, shutdownTimeout...)
}

func gracefulFunc(ctx context.Context, g grace, fn func() error, shutdownTimeout ...time.Duration) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- fn()
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

		if err := g.Shutdown(shutdownCtx); err != nil {
			return err
		}
	}
	return nil
}
