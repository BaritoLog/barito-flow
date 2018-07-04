package cmds

// import (
// 	"os"
// 	"testing"
//
// 	. "github.com/BaritoLog/go-boilerplate/testkit"
// 	"github.com/BaritoLog/instru"
// )

// func TestConsumer(t *testing.T) {
// 	log.SetLevel(log.ErrorLevel)
// 	os.Setenv(EnvKafkaBrokers, "wronghost:2349")
// 	defer os.Clearenv()
//
// 	err := ConsumerAction(nil)
//
// 	FatalIfWrongError(t, err, "kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")
// }

// func TestCallbackInstrumentation(t *testing.T) {
//
// 	FatalIf(t, callbackInstrumentation(), "it should be return false")
//
// 	os.Setenv(EnvPushMetricToken, "some-token")
// 	os.Setenv(EnvPushMetricUrl, "http://some-url")
// 	FatalIf(t, !callbackInstrumentation(), "it should be return true")
// 	FatalIf(t, instru.DefaultCallback == nil, "it should be set callback")
// }
