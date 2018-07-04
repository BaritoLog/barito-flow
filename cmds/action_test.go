package cmds

import (
	"fmt"
	"os"
	"testing"

	"github.com/BaritoLog/go-boilerplate/saramatestkit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.ErrorLevel)
}

func TestProducer(t *testing.T) {
	kafkaProducer := mocks.NewSyncProducer(nil, sarama.NewConfig())

	patch := saramatestkit.PatchNewSyncProducer(kafkaProducer, nil)
	defer patch.Unpatch()

	os.Setenv(EnvProducerAddress, "asdf")
	defer os.Clearenv()

	err := ActionBaritoProducerService(nil)
	FatalIfWrongError(t, err, "listen tcp: address asdf: missing port in address")

}

func TestProducer_KafkaError(t *testing.T) {
	patch := saramatestkit.PatchNewSyncProducer(nil, fmt.Errorf("some-error"))
	defer patch.Unpatch()

	os.Setenv(EnvKafkaBrokers, "wronghost:2349")
	defer os.Clearenv()

	err := ActionBaritoProducerService(nil)
	FatalIfWrongError(t, err, "some-error")
}
