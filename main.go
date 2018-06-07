package main

import (
	"fmt"
	"log"
	"os"

	"github.com/BaritoLog/barito-flow/cmds"
	"github.com/urfave/cli"
)

const (
	Name    = "barito-agent"
	Version = "0.3.0"
)

func main() {
	app := cli.App{
		Name:    Name,
		Usage:   "Provide kafka reciever or log forwarder for Barito project",
		Version: Version,
		Commands: []cli.Command{
			{
				Name:      "producer",
				ShortName: "p",
				Usage:     "start barito-flow as producer",
				Action:    cmds.Producer,
			},
			{
				Name:      "consumer",
				ShortName: "c",
				Usage:     "start barito-flow as consumer",
				Action:    cmds.Consumer,
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(fmt.Sprintf("Some error occurred: %s", err.Error()))
	}
}
