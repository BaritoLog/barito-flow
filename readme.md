# Barito Flow 
![alt](https://travis-ci.org/BaritoLog/barito-flow.svg?branch=master)

Building flow of Barito river with provide kafka reciever or log forwarder 

## Setup 

Setup the project
```sh
git clone https://github.com/BaritoLog/barito-flow

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

| Name| Description | ENV | Default Value  |
| ---|---|---|---|
| ConsulUrl | Consul URL | BARITO_CONSUL_URL | |
| ConsulKafkaName  | Kafka service name in consul | BARITO_CONSUL_KAFKA_NAME | kafka |
| KafkaBrokers | Kafka broker addresses (CSV). Get from env is not available in consul | BARITO_KAFKA_BROKERS | localhost:9092 |
| KafkaMaxRetry | Number of retry to connect to kafka during startup | BARITO_KAFKA_MAX_RETRY | 0 (unlimited) |
| KafkaRetryInterval | Interval between retry connecting to kafka (in seconds) | BARITO_KAFKA_RETRY_INTERVAL | 10 |
| ProducerAddress | Http Server Address | BARITO_PRODUCER_ADDRESS| :8080 |
| ProducerMaxRetry | Set kafka setting max retry | BARITO_PRODUCER_MAX_RETRY | 10 |
| ProducerMaxTps | Producer rate limit trx per second | BARITO_PRODUCER_MAX_TPS | 100 |
| ProducerRateLimitResetInterval | Producer rate limit reset interval (in seconds) | BARITO_PRODUCER_RATE_LIMIT_RESET_INTERVAL | 10 |

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
| ConsulUrl | Consul URL | BARITO_CONSUL_URL | |
| ConsulKafkaName  | Kafka service name in consul | BARITO_CONSUL_KAFKA_NAME | kafka |
| ConsulElasticsearchName | Elasticsearch service name in consul | BARITO_CONSUL_ELASTICSEARCH_NAME | elasticsearch |
| KafkaBrokers | Kafka broker addresses (CSV). Get from env is not available in consul | BARITO_KAFKA_BROKERS| localhost:9092 |
| KafkaGroupID | kafka consumer group id | BARITO_KAFKA_GROUP_ID | barito-group |
| KafkaMaxRetry | Number of retry to connect to kafka during startup | BARITO_KAFKA_MAX_RETRY | 0 (unlimited) |
| KafkaRetryInterval | Interval between retry connecting to kafka (in seconds) | BARITO_KAFKA_RETRY_INTERVAL | 10 |
| ElasticsearchUrl | Elastisearch url | BARITO_ELASTICSEARCH_URL | http://localhost:9200 |
| EsIndexMethod | BulkProcessor / SingleInsert | BARITO_ELASTICSEARCH_INDEX_METHOD | BulkProcessor |
| EsBulkSize | BulkProcessor bulk size | BARITO_ELASTICSEARCH_BULK_SIZE | 100 |
| EsFlushIntervalMs | BulkProcessor flush interval (ms) | BARITO_ELASTICSEARCH_FLUSH_INTERVAL_MS | 500 |
| PrintTPS | print estimated consumed every second | BARITO_PRINT_TPS | off |
| PushMetricUrl | push metric api url | BARITO_PUSH_METRIC_URL|   |
| PushMetricInterval | push metric interval | BARITO_PUSH_METRIC_INTERVAL | 30s |

**NOTE** 
The following will not be used if BARITO_ELASTICSEARCH_INDEX_METHOD is set to "SingleInsert"

- BARITO_ELASTICSEARCH_BULK_SIZE. 
- BARITO_ELASTICSEARCH_FLUSH_INTERVAL_MS

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

#### 0.11.8
- Retry connecting to kafka during startup if kafka cluster is not available (configurable)

#### 0.11.7
- Enable toggle-able verbose mode

#### 0.11.6
- Set round robin strategy as default for consumer rebalancing, also allow it to be configured

#### 0.11.5
- Recreate cluster admin whenever barito-flow needs to create a new topic

#### 0.11.4
- Update rate limiter behaviour for produce batch, now it counts rate per line in batch instead of per batch request

#### 0.11.3
- Lower metadata refresh frequency

#### 0.11.2
- Bugfix: Refresh metadata before creating new topic (see [here](https://github.com/Shopify/sarama/issues/1162))

#### 0.11.1
- Bugfix: Rate limiter on produce batch should count per request, not per log

#### 0.11
- New feature: Ability to process batch logs

#### 0.10.1
- Use updated instru that avoid race condition problem on counter

#### 0.10.0
- Implement Elasticsearch backoff functionality

#### 0.9.0
- Upgrade sarama to 1.19.0
- Use sarama ClusterAdmin for creating topic

#### 0.8.5
- Produce logs if flow has to return 5xx errors

#### 0.8.4
- Use updated instru that avoid race condition problem

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
