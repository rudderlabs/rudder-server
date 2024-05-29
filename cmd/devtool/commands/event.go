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
				"Content":     string([]byte{212, 109, 92, 117, 48, 48, 49, 99, 92, 117, 48, 48, 51, 99, 92, 117, 48, 48, 49, 52, 69, 108, 101, 109, 101, 110, 116, 58, 92, 110, 32, 32, 45, 32, 84, 73, 77, 69, 83, 84, 65, 77, 80, 58, 32, 49, 55, 49, 54, 52, 55, 54, 53, 53, 48, 92, 110, 32, 32, 45, 32, 84, 65, 71, 58, 32, 83, 72, 82, 79, 79, 76, 89, 95, 77, 65, 73, 78, 92, 110, 32, 32, 45, 32, 69, 83, 80, 95, 76, 79, 71, 95, 76, 69, 86, 69, 76, 58, 32, 52, 92, 110, 32, 32, 45, 32, 77, 69, 83, 83, 65, 71, 69, 58, 32, 77, 97, 105, 110, 32, 108, 111, 111, 112, 32, 114, 117, 110, 115, 46, 92, 110, 69, 108, 101, 109, 101, 110, 116, 58, 92, 110, 32, 32, 45, 32, 84, 73, 77, 69, 83, 84, 65, 77, 80, 58, 32, 49, 55, 49, 54, 52, 55, 54, 53, 53, 50, 92, 110, 32, 32, 45, 32, 84, 65, 71, 58, 32, 83, 72, 82, 79, 79, 76, 89, 95, 77, 65, 73, 78, 92, 110, 32, 32, 45, 32, 69, 83, 80, 95, 76, 79, 71, 95, 76, 69, 86, 69, 76, 58, 32, 52, 92, 110, 32, 32, 45, 32, 77, 69, 83, 83, 65, 71, 69, 58, 32, 77, 97, 105, 110, 32, 108, 111, 111, 112, 32, 114, 117, 110, 115, 46, 92, 110, 69, 108, 101, 109, 101, 110, 116, 58, 92, 110, 32, 32, 45, 32, 84, 73, 77, 69, 83, 84, 65, 77, 80, 58, 32, 49, 55, 49, 54, 52, 55, 54, 53, 53, 52, 92, 110, 32, 32, 45, 32, 84, 65, 71, 58, 32, 83, 72, 82, 79, 79, 76, 89, 95, 77, 65, 73, 78, 92, 110, 32, 32, 45, 32, 69, 83, 80, 95, 76, 79, 71, 95, 76, 69, 86, 69, 76, 58, 32, 52, 92, 110, 32, 32, 45, 32, 77, 69, 83, 83, 65, 71, 69, 58, 32, 77, 97, 105, 110, 32, 108, 111, 111, 112, 32, 114, 117, 110, 115, 46, 92, 110, 69, 108, 101, 109, 101, 110, 116, 58, 92, 110, 32, 32, 45, 32, 84, 73, 77, 69, 83, 84, 65, 77, 80, 58, 32, 49, 55, 49, 54, 52, 55, 54, 53, 53, 54, 92, 110, 32, 32, 45, 32, 84, 65, 71, 58, 32, 83, 72, 82, 79, 79, 76, 89, 95, 77, 65, 73, 78, 92, 110, 32, 32, 45, 32, 69, 83, 80, 95, 76, 79, 71, 95, 76, 69, 86, 69, 76, 58, 32, 52, 92, 110, 32, 32, 45, 32, 77, 69, 83, 83, 65, 71, 69, 58, 32, 77, 97, 105, 110, 32, 108, 111, 111, 112, 32, 114, 117, 110, 115, 46, 92, 110, 69, 108, 101, 109, 101, 110, 116, 58, 92, 110, 32, 32, 45, 32, 84, 73, 77, 69, 83, 84, 65, 77, 80, 58, 32, 49, 55, 49, 54, 52, 55, 54, 53, 53, 56, 92, 110, 32, 32, 45, 32, 84, 65, 71, 58, 32, 83, 72, 82, 79, 79, 76, 89, 95, 77, 65, 73, 78, 92, 110, 32, 32, 45, 32, 69, 83, 80, 95, 76, 79, 71, 95, 76, 69, 86, 69, 76, 58, 32, 52, 92, 110, 32, 32, 45, 32, 77, 69, 83, 83, 65, 71, 69, 58, 32, 77, 97, 105, 110, 32, 108, 111, 111, 112, 32, 114, 117, 110, 115, 46, 92, 110, 69, 108, 101, 109, 101, 110, 116, 58, 92, 110, 32, 32, 45, 32, 84, 73, 77, 69, 83, 84, 65, 77, 80, 58, 32, 49, 55, 49, 54, 52, 55, 54, 53, 54, 48, 92, 110, 32, 32, 45, 32, 84, 65, 71, 58, 32, 83, 72, 82, 79, 79, 76, 89, 95, 77, 65, 73, 78, 92, 110, 32, 32, 45, 32, 69, 83, 80, 95, 76, 79, 71, 95, 76, 69, 86, 69, 76, 58, 32, 52, 92, 110, 32, 32, 45, 32, 77, 69, 83, 83, 65, 71, 69, 58, 32, 77, 97, 105, 110, 32, 108, 111, 111, 112, 32, 114, 117, 110, 115, 46, 92, 110}),
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
			// curl --location 'https://rudderstacthlq.dataplane.dev.rudderlabs.com/beacon/v1/batch?writeKey=216CD6a1KgBvYpie7mKPVlOH1a7' \\n--header 'Content-Type: application/json' \\n--data '{"userId":"anon-id-webhook-46864452-541d-4569-8bed-3daed87c39f5","anonymousId":"anon-id-webhook-46864452-541d-4569-8bed-3daed87c39f5","context":{"traits":{"timeout":"false","trait-1-25":"value-1-25","trait-2-25":"value-2-25"},"ip":"42.42.42.42","library":{"name":"http"}},"timestamp":"2023-06-12T01:04:04.781660426Z"}'
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
