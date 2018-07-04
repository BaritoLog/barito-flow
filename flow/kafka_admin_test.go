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

func TestKafkaAdmin_Exist(t *testing.T) {

	topics := []string{"topic01", "new-topic"}

	client := saramatestkit.NewClient()
	client.TopicsFunc = func() ([]string, error) {
		return topics, nil
	}

	patch := saramatestkit.PatchNewClient(client, nil)
	defer patch.Unpatch()

	admin, err := NewKafkaAdmin([]string{}, sarama.NewConfig())
	FatalIfError(t, err)
	defer admin.Close()

	// assume admin already cache for its topics
	admin.SetTopics([]string{"topic01"})

	FatalIf(t, !admin.Exist("topic01"), "topic01 is exist without refresh")
	FatalIf(t, !admin.Exist("new-topic"), "new-topic is exist after refresh")
	FatalIf(t, admin.Exist("no-topic"), "no-topic is really not exist")
}

func TestKafkaAdmin_CreateTopicIfNotExist(t *testing.T) {

	topics := []string{"topic01", "topic02"}

	client := saramatestkit.NewClient()
	client.TopicsFunc = func() ([]string, error) {
		return topics, nil
	}

	patch := saramatestkit.PatchNewClient(client, nil)
	defer patch.Unpatch()

	admin, err := NewKafkaAdmin([]string{}, sarama.NewConfig())
	FatalIfError(t, err)
	defer admin.Close()

	creatingTopic, err := admin.CreateTopicIfNotExist("topic02", 3, 1)
	FatalIfError(t, err)
	FatalIf(t, creatingTopic, "topic02 already exist")
}
