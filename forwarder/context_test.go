package forwarder

import (
	"testing"

	"github.com/BaritoLog/go-boilerplate/testkit"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.FatalLevel)
}


func TestNewContext(t *testing.T) {
	ctx := NewContext()
	testkit.FatalIf(
		t,
		ctx == nil,
		"ctx should be initiate",
	)
}

func TestContext_KafkaConfig(t *testing.T) {
	ctx := context{}
	config := ctx.kafkaConfig()
	testkit.FatalIf(t, config == nil, "kafka config should be set")
}