package cmds

import (
	"os"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	log "github.com/sirupsen/logrus"
)

func TestProducer(t *testing.T) {
	log.SetLevel(log.ErrorLevel)
	os.Setenv(EnvKafkaBrokers, "wronghost:2349")
	defer os.Clearenv()

	err := Producer(nil)
	FatalIfWrongError(t, err, "kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")
}
