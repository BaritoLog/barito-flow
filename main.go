package main

import (
	"fmt"
	"log"
	"os"

	"github.com/BaritoLog/barito-flow/common"
	"github.com/BaritoLog/barito-flow/forwarder"
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
				Usage:     "Log Forwarder",
				Action:    startForwarder,
			},
			{
				Name:      "start",
				ShortName: "s",
				Usage:     "start barito flow",
				Action:    start,
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

func startForwarder(c *cli.Context) (err error) {
	runner := app.NewRunner(
		forwarder.NewContext(),
		forwarder.NewConfigurationManager(),
	)

	err = runner.Run()

	return
}

func start(c *cli.Context) (err error) {

	from := common.NewConsoleUpstream(os.Stdin)
	to := common.NewConsoleDownstream(os.Stdout)

	raft := common.NewRaft(from, to)
	err = raft.Start()

	return

}
