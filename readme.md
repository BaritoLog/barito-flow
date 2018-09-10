# Barito Flow 
![alt](https://travis-ci.org/BaritoLog/barito-flow.svg?branch=master)

Building flow of Barito river with provide kafka reciever or log forwarder 

## Setup 

Setup the project
```sh
cd $GOPATH/src
git clone git@github.com:BaritoLog/barito-flow.git 

cd barito-flow
go build
```

Generate mock class
```sh
mockgen -source=flow/leaky_bucket.go -destination=mock/leaky_bucket.go -package=mock
mockgen -source=flow/kafka_admin.go -destination=mock/kafka_admin.go -package=mock
mockgen -source=flow/Vendor/github.com/sarama/sync_producer.go -destination=mock/sync_producer.go -package=mock
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
| ---|---|---|---|
| KafkaBrokers | Kafka broker addresses (CSV). Get from env is not available in consul| BARITO_KAFKA_BROKERS| localhost:9092 |
|   |   | BARITO_CONSUL_URL | |
|   |   | BARITO_CONSUL_KAFKA_NAME | kafka |
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
| ---|---|----|----|
| KafkaBrokers | Kafka broker addresses (CSV). Get from env is not available in consul| BARITO_KAFKA_BROKERS| localhost:9092 |
|   |   | BARITO_CONSUL_URL | |
|   |   | BARITO_CONSUL_KAFKA_NAME | kafka |
| KafkaGroupID | kafka group id | BARITO_KAFKA_GROUP_ID | barito-group |
| KafkaConsumerTopics | kafka consumer topics (CSV) | BARITO_KAFKA_CONSUMER_TOPICS| consumer-topic |
| ElasticsearchUrl | elastisearch url | BARITO_ELASTICSEARCH_URL| http://localhost:9200 |
|   |   | BARITO_CONSUL_URL | |
|   |   | BARITO_CONSUL_ELASTICSEARCH_NAME | elasticsearch |
| PushMetricUrl | push metric api url | BARITO_PUSH_METRIC_URL| http://localhost:3000/api/increase_log_count |
| PushMetricToken | push metric api token| BARITO_PUSH_METRIC_TOKEN |  |
| PushMetricInterval | push metric interval| BARITO_PUSH_METRIC_INTERVAL | 30s |

## Running Test Stack using Docker Compose

First you'd need to Install [Docker for Windows](https://www.docker.com/docker-windows) or [Docker
for Mac](https://www.docker.com/docker-mac) if you're using Windows or Linux.

Run `docker-compose`:

```sh
$ docker-compose -f docker/docker-compose.yml up -d
```

This will pull Elastic Search, Kafka, and build image for produces as well as consumer. The ports
are mapped as if they are running on local machine.

## Changes Log

#### 0.8.3
- Also send application secret from context when sending metrics

#### 0.8.2
- Send metrics to market

#### 0.8.1
- Fix bug when checking for new events

#### 0.8.0
- Support for multiple topics and indexes

#### 0.7.0 
- Graceful Shutdown

#### 0.6.0 
- Using consul to get kafka brokers, if consul not available then using environment variable
- Using consul to get elasticsearch url, if url not available then using environment variable

#### 0.5.0 
- Rate limit trx per second (by default 100)

#### 0.4.0 
- Rename receiver to producer
- Rename forwarder to consumer
