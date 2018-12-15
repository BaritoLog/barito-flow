package cmds

import (
	"fmt"
	"os"
	"testing"

	"github.com/BaritoLog/go-boilerplate/saramatestkit"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.ErrorLevel)
}

func TestProducer_KafkaError(t *testing.T) {
	patch := saramatestkit.PatchNewSyncProducer(nil, fmt.Errorf("some-error"))
	defer patch.Unpatch()

	os.Setenv(EnvKafkaBrokers, "wronghost:2349")
	defer os.Clearenv()
}
