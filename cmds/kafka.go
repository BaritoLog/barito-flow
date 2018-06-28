package cmds

import "github.com/Shopify/sarama"

func kafkaTopics(brokers []string) (topics []string, err error) {

	config := sarama.NewConfig()
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return
	}
	defer client.Close()

	topics, err = client.Topics()
	return
}
