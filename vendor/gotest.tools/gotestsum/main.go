package main

import (
	"os"

	"gotest.tools/gotestsum/cmd"
	"gotest.tools/gotestsum/cmd/tool"
	"gotest.tools/gotestsum/internal/log"
)

func main() {
	err := route(os.Args)
	switch {
	case err == nil:
		return
	case cmd.IsExitCoder(err):
		// go test should already report the error to stderr, exit with
		// the same status code
		os.Exit(cmd.ExitCodeWithDefault(err))
	default:
		log.Error(err.Error())
		os.Exit(3)
	}
}

func route(args []string) error {
	name := args[0]
	next, rest := cmd.Next(args[1:])
	switch next {
	case "help", "?":
		return cmd.Run(name, []string{"--help"})
	case "tool":
		return tool.Run(name+" "+next, rest)
	default:
		return cmd.Run(name, args[1:])
	}
}
