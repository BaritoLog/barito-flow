package main

import (
	"fmt"
	"log"
	"os"

	"github.com/BaritoLog/barito-flow/cmds"
	"github.com/BaritoLog/barito-flow/receiver"
	"github.com/BaritoLog/go-boilerplate/app"
	"github.com/urfave/cli"
)

const (
	Name    = "barito-agent"
	Version = "0.1.0"
)

func main() {
	app := cli.App{
		Name:    Name,
		Usage:   "Provide kafka reciever or log forwarder for Barito project",
		Version: Version,
		Commands: []cli.Command{
			{
				Name:      "receiver",
				ShortName: "r",
				Usage:     "Kafka Receiver",
				Action:    startReceiver,
			},
			{
				Name:      "forwarder",
				ShortName: "f",
				Usage:     "start Log Forwarder",
				Action:    cmds.Forwarder,
			},
			{
				Name:      "start",
				ShortName: "s",
				Usage:     "start barito flow",
				Action:    cmds.Start,
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(fmt.Sprintf("Some error occurred: %s", err.Error()))
	}
}

func startReceiver(c *cli.Context) (err error) {
	runner := app.NewRunner(
		receiver.NewContext(),
		receiver.NewConfigurationManager(),
	)

	err = runner.Run()
	return
}
