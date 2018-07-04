package flow

import (
	"fmt"
	"testing"

	"github.com/BaritoLog/go-boilerplate/saramatestkit"
	"github.com/BaritoLog/go-boilerplate/slicekit"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/Shopify/sarama"
)

func TestKafkaAdmin_New_CreateClientError(t *testing.T) {
	patch := saramatestkit.PatchNewClient(nil, fmt.Errorf("some-error"))
	defer patch.Unpatch()

	_, err := NewKafkaAdmin([]string{}, sarama.NewConfig())
	FatalIfWrongError(t, err, "some-error")
}

func TestKafkaAdmin_RefreshTopics_ReturnError(t *testing.T) {
	client := saramatestkit.NewClient()
	client.TopicsFunc = func() ([]string, error) {
		return nil, fmt.Errorf("topics-error")
	}

	patch := saramatestkit.PatchNewClient(client, nil)
	defer patch.Unpatch()

	admin, err := NewKafkaAdmin([]string{}, sarama.NewConfig())
	FatalIfError(t, err)
	defer admin.Close()

	err = admin.RefreshTopics()
	FatalIfWrongError(t, err, "topics-error")
}

func TestKafkaAdmin_Topics(t *testing.T) {
	topics := []string{"topic01", "topic02_logs", "topic03_logs"}

	client := saramatestkit.NewClient()
	client.TopicsFunc = func() ([]string, error) {
		return topics, nil
	}

	patch := saramatestkit.PatchNewClient(client, nil)
	defer patch.Unpatch()

	admin, err := NewKafkaAdmin([]string{}, sarama.NewConfig())
	FatalIfError(t, err)
	defer admin.Close()

	FatalIf(t, !slicekit.StringSliceEqual(admin.Topics(), topics), "wrong admin.Topics()")
	FatalIf(t, !slicekit.StringSliceEqual(admin.TopicsWithSuffix("_logs"), []string{"topic02_logs", "topic03_logs"}), "wrong admin.Topics()")

}
