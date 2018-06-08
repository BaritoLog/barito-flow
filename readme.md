# Barito Flow 
![alt](https://travis-ci.org/BaritoLog/barito-flow.svg?branch=master)

Building flow of Barito river with provide kafka reciever or log forwarder 

## Setup Development

```sh
cd $GOPATH/src
git clone git@github.com:BaritoLog/barito-flow.git 

cd barito-flow
go build
```

## Producer

Responsible to:
- expose a http end point
- produce message to kafka broker

Run
```sh
./barito-agent producer

#or
./barito-agent p
```

Environment Variables



log.Infof("ProducerMaxTps: %d", producerMaxTps)


| Name| Description | ENV | Default Value  |
| ---|---|----|
| KafkaBrokers | Kafka broker addresses (CSV). Get from env is not available in consul| BARITO_KAFKA_BROKERS| localhost:9092 |
| | | BARITO_CONSUL_URL | |
| | | BARITO_CONSUL_KAFKA_NAME | kafka |
| ProducerAddress | Http Server Address | BARITO_PRODUCER_ADDRESS| :8080 |
|KafkaProducerTopic| kafka topic| BARITO_KAFKA_PRODUCER_TOPIC | producer-topic |
|ProducerMaxRetry| set kafka setting max retry| BARITO_PRODUCER_MAX_RETRY | 10 |
|ProducerMaxTps| producer rate limit trx per second| BARITO_PRODUCER_MAX_TPS | 100 |


## Consumer

Responsible to:
- consume message from kafka
- commit message to elasticsearch

Run
```sh
./barito-agent Consumer

# or
./barito-agent c
```

Environment Variables

| Name| Description | ENV | Default Value  |
| ---|---|----|
| KafkaBrokers | Kafka broker addresses (CSV). Get from env is not available in consul| BARITO_KAFKA_BROKERS| localhost:9092 |
| | | BARITO_CONSUL_URL | |
| | | BARITO_CONSUL_KAFKA_NAME | kafka |
| KafkaGroupID | kafka group id | BARITO_KAFKA_GROUP_ID | barito-group |
| KafkaConsumerTopics | kafka consumer topics (CSV) | BARITO_KAFKA_CONSUMER_TOPICS| consumer-topic |
| ElasticsearchUrl | elastisearch url | BARITO_ELASTICSEARCH_URL| http://localhost:9200 |
| PushMetricUrl | push metric api url | BARITO_PUSH_METRIC_URL| http://localhost:3000/api/increase_log_count |
| PushMetricToken | push metric api token| BARITO_PUSH_METRIC_TOKEN |  |
| PushMetricInterval | push metric interval| BARITO_PUSH_METRIC_INTERVAL | 30s |


## Changes Log

#### 0.5.0 
- Rate limit trx per second (by default 100)
