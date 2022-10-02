package commands

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"

	"github.com/joho/godotenv"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
)

func init() {
	DefaultList = append(DefaultList, RUN())
}

var (
	Red     = Color("\033[1;31m%s\033[0m")
	Green   = Color("\033[1;32m%s\033[0m")
	Yellow  = Color("\033[1;33m%s\033[0m")
	Purple  = Color("\033[1;34m%s\033[0m")
	Magenta = Color("\033[1;35m%s\033[0m")
	Teal    = Color("\033[1;36m%s\033[0m")
	White   = Color("\033[1;37m%s\033[0m")
)

type Color string

func (color Color) Apply(text string) string {
	return fmt.Sprintf(string(color), text)
}

func RUN() *cli.Command {
	c := &cli.Command{
		Name:  "run",
		Usage: "run all process of rudder-server depending on deployment mode",
		Subcommands: []*cli.Command{
			{
				Name:   "all",
				Usage:  "runs all server and warehouse components as separate processes",
				Action: RunAll,
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:  "slave-count",
						Usage: "number of slave warehouse processes to run",
						Value: 1,
					},
				},
			},
		},
	}

	return c
}

func RunAll(c *cli.Context) error {
	warehousePort, err := testhelper.GetFreePort()
	if err != nil {
		return err
	}

	setup := &setup{
		GlobalEnvs: []string{
			"RSERVER_WAREHOUSE_WEB_PORT=" + strconv.Itoa(warehousePort),
		},
		Processes: []rudderProcess{
			{
				name:           "gateway   ",
				appType:        app.GATEWAY,
				warehouseMode:  config.OffMode,
				logPrefixColor: Green,
			},
			{
				name:           "router    ",
				appType:        app.PROCESSOR,
				logPrefixColor: Yellow,
				warehouseMode:  config.OffMode,
			},
			{
				name:           "WH-master ",
				warehouseMode:  config.MasterMode,
				logPrefixColor: Purple,
			},
		},
	}

	for i := 0; i < c.Int("slave-count"); i++ {
		setup.Processes = append(setup.Processes, rudderProcess{
			name:           "WH-slave-" + strconv.Itoa(i),
			warehouseMode:  config.SlaveMode,
			logPrefixColor: Magenta,
		})
	}

	return setup.Run(c.Context)
}

type rudderProcess struct {
	name           string
	appType        string
	warehouseMode  string
	logPrefixColor Color
}

type setup struct {
	GlobalEnvs []string
	Processes  []rudderProcess

	once    sync.Once
	onceErr error
}

func (s *setup) init() error {
	s.once.Do(func() {
		if s.onceErr = godotenv.Load(".env"); s.onceErr != nil {
			return
		}

		s.GlobalEnvs = append(os.Environ(), s.GlobalEnvs...)
	})
	return s.onceErr
}

func (s *setup) Run(ctx context.Context) error {
	if err := s.init(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	for _, p := range s.Processes {
		p := p
		g.Go(func() error {
			defer cancel()

			return s.runAppType(ctx, p.name, p.appType, p.warehouseMode, p.logPrefixColor)
		})
	}
	return g.Wait()
}

func (s *setup) runAppType(ctx context.Context, name string, appType string, warehouseMode string, c Color) error {

	cmd := exec.Command("go", "run", "main.go")

	debugPort, err := testhelper.GetFreePort()
	if err != nil {
		return err
	}

	cmd.Env = append(cmd.Env, s.GlobalEnvs...)

	cmd.Env = append(cmd.Env,
		"RSERVER_DB_APPLICATION_NAME=run-rudder-server"+fmt.Sprintf("run-rudder-server-%s-%s", appType, warehouseMode),
		"APP_TYPE="+appType,
		"RSERVER_WAREHOUSE_MODE="+warehouseMode,
		"RSERVER_PROFILER_PORT="+strconv.Itoa(debugPort),
	)

	if warehouseMode == config.SlaveMode {
		warehousePort, err := testhelper.GetFreePort()
		if err != nil {
			return err
		}
		cmd.Env = append(cmd.Env, "RSERVER_WAREHOUSE_WEB_PORT="+strconv.Itoa(warehousePort))
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer stdout.Close()
		return prefixColorPipe(name, c, stdout, os.Stdout)
	})

	g.Go(func() error {
		defer stderr.Close()
		return prefixColorPipe(name, c, stderr, os.Stderr)
	})

	g.Go(func() error {
		return ctx.Err()
	})

	g.Go(cmd.Wait)

	if err := cmd.Start(); err != nil {
		return err
	}

	<-ctx.Done()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		return err
	}

	return g.Wait()

}

func prefixColorPipe(prefix string, c Color, r io.Reader, w io.Writer) error {
	fileScanner := bufio.NewScanner(r)
	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		fmt.Fprintf(w, "%s  %s\n", c.Apply(prefix), fileScanner.Text())
	}

	return fileScanner.Err()
}
