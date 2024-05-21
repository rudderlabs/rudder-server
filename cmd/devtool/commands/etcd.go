package commands

import (
	"fmt"
	"strings"

	"github.com/alexeyco/simpletable"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/servermode"
)

var DefaultList []*cli.Command

func init() {
	DefaultList = append(DefaultList, ETCD())
}

func ETCD() *cli.Command {
	c := &cli.Command{
		Name:  "etcd",
		Usage: "interact with etcd",
		Subcommands: []*cli.Command{
			{
				Name:   "mode",
				Usage:  "switch between normal and degraded mode of rudder-server",
				Action: Mode,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "no-wait",
						Usage: "do not wait for the mode change to be acknowledged",
					},
				},
				ArgsUsage: "[normal|degraded]",
			},
			{
				Name:   "list",
				Usage:  "list all key-values in etcd",
				Action: List,
			},
		},
	}

	return c
}

func Mode(c *cli.Context) error {
	if c.Args().Len() == 0 {
		return fmt.Errorf("need to specify mode: normal or degraded")
	}

	mode := servermode.Mode(strings.ToUpper(c.Args().Get(0)))
	if !mode.Valid() {
		return fmt.Errorf("invalid mode: %s", mode)
	}

	endpoints := strings.Split(config.GetString("ETCD_HOSTS", "127.0.0.1:2379"), `,`)
	etcdClient, err := etcd.New(etcd.Config{
		Endpoints: endpoints,
		Context:   c.Context,
	})
	if err != nil {
		return err
	}

	releaseName := config.GetReleaseName()
	serverIndex := misc.GetInstanceID()

	ackKey := fmt.Sprintf("ack-devtool/%s", uuid.New().String())
	ackCh := etcdClient.Watch(c.Context, ackKey)

	modeRequestKey := fmt.Sprintf("/%s/SERVER/%s/MODE", releaseName, serverIndex)
	payload := fmt.Sprintf(`{"mode": %q, "ack_key": %q}`, mode, ackKey)
	_, err = etcdClient.Put(c.Context, modeRequestKey, payload)
	if err != nil {
		return err
	}
	fmt.Printf("mode request sent: %s -> %s \n", modeRequestKey, payload)

	if c.Bool("no-wait") {
		return nil
	}

	fmt.Print("waiting for ack: ")

	resp := <-ackCh

	if resp.Err() != nil {
		return resp.Err()
	}

	fmt.Printf("%s\n", resp.Events[0].Kv.Value)

	return nil
}

func List(c *cli.Context) error {
	endpoints := strings.Split(config.GetString("ETCD_HOSTS", "127.0.0.1:2379"), `,`)
	etcdClient, err := etcd.New(etcd.Config{
		Endpoints: endpoints,
		Context:   c.Context,
	})
	if err != nil {
		return err
	}

	resp, err := etcdClient.Get(c.Context, "", etcd.WithFromKey(), etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
	if err != nil {
		return err
	}

	table := simpletable.New()
	table.Header = &simpletable.Header{
		Cells: []*simpletable.Cell{
			{Align: simpletable.AlignCenter, Text: "Key"},
			{Align: simpletable.AlignCenter, Text: "Value"},
		},
	}

	for _, kv := range resp.Kvs {
		r := []*simpletable.Cell{
			{Align: simpletable.AlignLeft, Text: fmt.Sprintf("%q", kv.Key)},
			{Align: simpletable.AlignLeft, Text: string(kv.Value)},
		}

		table.Body.Cells = append(table.Body.Cells, r)
	}

	table.SetStyle(simpletable.StyleCompactLite)
	fmt.Println(table.String())

	return nil
}
