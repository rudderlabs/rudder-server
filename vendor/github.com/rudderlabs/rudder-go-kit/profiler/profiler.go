package profiler

import (
	"context"
	"expvar"
	"fmt"
	"net/http"
	pprof "net/http/pprof"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/httputil"
)

// StartServer starts a HTTP server that serves profiling information. The function
// returns when the server is shut down or when the context is canceled.
func StartServer(ctx context.Context, port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = "/debug/pprof/"
		http.Redirect(w, r, r.URL.String(), http.StatusMovedPermanently)
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		first := true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\n")
		expvar.Do(func(kv expvar.KeyValue) {
			if !first {
				fmt.Fprintf(w, ",\n")
			}
			first = false
			fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
		})
		fmt.Fprintf(w, "\n}\n")
	})

	srv := &http.Server{
		Handler: mux,
		Addr:    ":" + strconv.Itoa(port),
	}
	if err := httputil.ListenAndServe(ctx, srv, 3*time.Second); err != nil && ctx.Err() != nil {
		return fmt.Errorf("profiler server: %w", err)
	}
	return nil
}
