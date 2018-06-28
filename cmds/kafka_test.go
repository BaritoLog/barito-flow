package cmds

import (
	"fmt"
	"testing"

	"github.com/BaritoLog/go-boilerplate/saramatestkit"
	"github.com/BaritoLog/go-boilerplate/slicekit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestKafkaTopics_NewClientError(t *testing.T) {
	patch := saramatestkit.PatchNewClient(nil, fmt.Errorf("some-error"))
	defer patch.Unpatch()

	_, err := kafkaTopics([]string{})
	FatalIfWrongError(t, err, "some-error")
}

func TestKafkaTopcis(t *testing.T) {

	client := saramatestkit.NewClient()
	client.TopicsFunc = func() ([]string, error) {
		return []string{"t1", "t2"}, fmt.Errorf("some-error")
	}
	patch := saramatestkit.PatchNewClient(client, nil)
	defer patch.Unpatch()

	topics, err := kafkaTopics([]string{})
	FatalIfWrongError(t, err, "some-error")
	FatalIf(t, !slicekit.StringSliceEqual(topics, []string{"t1", "t2"}), "wrong topics")

}
