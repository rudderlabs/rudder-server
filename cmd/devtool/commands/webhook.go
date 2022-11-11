package commands

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/urfave/cli/v2"
)

func init() {
	DefaultList = append(DefaultList, WEBHOOK())
}

func WEBHOOK() *cli.Command {
	c := &cli.Command{
		Name:  "webhook",
		Usage: "spin up a webhook destination server",
		Subcommands: []*cli.Command{
			{
				Name:   "run",
				Usage:  "switch between normal and degraded mode of rudder-server",
				Action: WebhookRun,
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:  "port",
						Usage: "specify the port to listen on",
						Value: 8083,
					},
					&cli.BoolFlag{
						Name:    "verbose",
						Aliases: []string{"v"},
						Usage:   "print more",
						Value:   false,
					},
				},
			},
		},
	}

	return c
}

func WebhookRun(c *cli.Context) error {
	port := c.Int("port")

	fmt.Printf("listening on: http://localhost:%d \n", port)
	httpWebServer := &http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &webhook{
			Verbose: c.Bool("verbose"),
		},
		ReadTimeout:       0 * time.Second,
		ReadHeaderTimeout: 0 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       720 * time.Second,
		MaxHeaderBytes:    524288,
	}
	return httpWebServer.ListenAndServe()
}

type webhook struct {
	Verbose bool
}

type payload struct {
	SentAt string
}

func (*webhook) computeTime(b []byte) {
	p := payload{}
	err := json.Unmarshal(b, &p)
	if err != nil {
		return
	}
	sentAt, err := time.Parse(time.RFC3339, p.SentAt)
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("got event after:", time.Since(sentAt))
}

func (wh *webhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	wh.computeTime(b)

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
