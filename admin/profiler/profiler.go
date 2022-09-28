package profiler

import (
	"context"
	"expvar"
	"fmt"
	"net/http"
	pprof "net/http/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	defaultProfilePort = 7777
)

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("admin")
}

type Profiler struct {
	once      sync.Once
	pkgLogger logger.Logger
	port      int
	enabled   bool
}

func (p *Profiler) init() {
	p.once.Do(func() {
		if p.pkgLogger != nil {
			p.pkgLogger = logger.NewLogger().Child("admin")
		}
		if p.port == 0 {
			p.port = config.GetInt("Profiler.Port", defaultProfilePort)
		}
		p.enabled = config.GetBool("Profiler.Enabled", true)
	})
}

func (p *Profiler) StartServer(ctx context.Context) error {
	p.init()
	if !p.enabled {
		pkgLogger.Infof("Profiler disabled: no pprof HTTP server")
	}

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
		Addr:    ":" + strconv.Itoa(p.port),
	}

	pkgLogger.Infof("Starting server on port %d", p.port)
	if err := httputil.ListenAndServe(ctx, srv, 3*time.Second); err != nil {
		return fmt.Errorf("debug server: %w", err)
	}

	return nil
}
