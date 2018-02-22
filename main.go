package main

import (
	"fmt"
	"log"
	"logstore/forwarder"
	"logstore/receiver"
	"os"

	"github.com/urfave/cli"
)

const (
	NAME = "logstore"
)

func main() {
	app := cli.NewApp()
	app.Name = NAME
	app.Commands = []cli.Command{
		{
			Name:        "receiver",
			Description: fmt.Sprintf("Start Receiver v%s", receiver.Version),
			Aliases:     []string{"r"},
			Action:      receiver.Start,
		},
		{
			Name:        "forwarder",
			Description: fmt.Sprintf("Start Forwarder v%s", forwarder.Version),
			Aliases:     []string{"f"},
			Action:      forwarder.Start,
		},
	}

	log.Fatal(app.Run(os.Args))

}
