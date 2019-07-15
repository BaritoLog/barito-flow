package main

import (
	"fmt"
	"os"

	"github.com/BaritoLog/barito-flow/cmds"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	Name    = "barito-flow"
	Version = "0.12.1"
)

var (
	Commit string = "N/A"
	Build  string = "MANUAL"
)

func init() {
	log.SetLevel(log.WarnLevel)
}

func main() {
	app := cli.App{
		Name:    Name,
		Usage:   "Provide kafka producer or consumer for Barito project",
		Version: fmt.Sprintf("%s-%s-%s", Version, Build, Commit),
		Commands: []cli.Command{
			{
				Name:      "producer",
				ShortName: "p",
				Usage:     "start barito-flow as producer",
				Action:    cmds.ActionBaritoProducerService,
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "verbose, V",
						Usage: "Enable verbose mode",
					},
				},
			},
			{
				Name:      "consumer",
				ShortName: "c",
				Usage:     "start barito-flow as consumer",
				Action:    cmds.ActionBaritoConsumerService,
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "verbose, V",
						Usage: "Enable verbose mode",
					},
				},
			},
		},
		UsageText: "barito-flow [commands]",
		Before: func(c *cli.Context) error {
			fmt.Fprintf(os.Stderr, "%s Started. Version: %s Build: %s Commit: %s\n", Name, Version, Build, Commit)
			return nil
		},
		CommandNotFound: func(c *cli.Context, command string) {
			fmt.Fprintf(os.Stderr, "Command not found: '%s'. Please use '%s -h' to view usage\n", command, Name)
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf("Some error occurred: %s", err.Error())
	}
}
