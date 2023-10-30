package commands

import (
	"bytes"
	"embed"
	"fmt"
	"io"
	"net/http"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/urfave/cli/v2"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

func init() {
	DefaultList = append(DefaultList, EVENT())
}

//go:embed payloads/*
var payloads embed.FS

func EVENT() *cli.Command {
	c := &cli.Command{
		Name:  "event",
		Usage: "collection of commands to help with event testing",
		Subcommands: []*cli.Command{
			{
				Name:   "send",
				Usage:  "send event to rudder-server",
				Action: EventSend,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "endpoint",
						Usage:   "HTTP endpoint for rudder-server",
						Value:   "http://localhost:8080",
						Aliases: []string{"e"},
					},
					&cli.StringFlag{
						Name:     "write-key",
						Usage:    "source write key",
						Required: true,
						Aliases:  []string{"w"},
					},
					&cli.IntFlag{
						Name:    "count",
						Usage:   "number of events to send",
						Value:   1,
						Aliases: []string{"c"},
					},
				},
				ArgsUsage: "",
			},
		},
	}

	return c
}

func EventSend(c *cli.Context) error {
	client := &http.Client{}

	url := fmt.Sprintf("%s/v1/batch", c.String("endpoint"))
	t, err := template.New("batch.json").ParseFS(payloads, "payloads/batch.json")
	if err != nil {
		return err
	}

	for i := 0; i < c.Int("count"); i++ {
		if err := func() error {
			anonymousId := uuid.New().String()
			buf := bytes.NewBuffer(nil)
			err = t.Execute(buf, map[string]string{
				"AnonymousId": anonymousId,
				"Timestamp":   time.Now().Format(time.RFC3339),
			})
			if err != nil {
				return err
			}

			req, err := http.NewRequestWithContext(c.Context, "POST", url, buf)
			if err != nil {
				return err
			}

			req.SetBasicAuth(c.String("write-key"), "")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("AnonymousId", anonymousId)

			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer func() { httputil.CloseResponse(resp) }()
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			if resp.StatusCode != http.StatusOK {
				fmt.Printf("%s\n%s\n", resp.Status, b)
				return fmt.Errorf("status code: %d", resp.StatusCode)
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	return nil
}
