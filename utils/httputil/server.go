package httputil

import (
	"context"
	"net"
	"net/http"
	"time"
)

func ListenAndServe(ctx context.Context, server *http.Server, shutdownTimeout ...time.Duration) error {
	return gracefulFunc(ctx, server, server.ListenAndServe, shutdownTimeout...)
}

func Serve(ctx context.Context, server *http.Server, l net.Listener, shutdownTimeout ...time.Duration) error {
	fn := func() error {
		return server.Serve(l)
	}
	return gracefulFunc(ctx, server, fn, shutdownTimeout...)
}

func gracefulFunc(ctx context.Context, server *http.Server, fn func() error, shutdownTimeout ...time.Duration) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- fn()
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		switch {
		case len(shutdownTimeout) == 0:
			return server.Shutdown(context.Background())
		case shutdownTimeout[0] == 0:
			return server.Close()
		default:
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout[0])
			defer cancel()

			return server.Shutdown(ctx)
		}
	}
}
