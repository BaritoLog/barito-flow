package receiver

import (
	"fmt"

	"github.com/urfave/cli"
)

const (
	Version = "0.1"
)

func Start(c *cli.Context) (err error) {
	fmt.Printf("Reciever v%s\n", Version)
	fmt.Printf("%+v", c)

	return
}
