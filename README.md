# Barito Flow

![Build Status](https://travis-ci.org/BaritoLog/barito-flow.svg?branch=master)

Component for handling flow of logs within a cluster. Support 2 modes:
- **Producer**, for receiving logs and forwarding it to Kafka
- **Consumer**, for consuming from kafka and forwarding it to Elasticsearch

Compatible with both gRPC and REST API. The use of REST API is optional and is implemented by using
[gRPC-gateway](https://github.com/grpc-ecosystem/grpc-gateway) which work like a reverse-proxy server
to translate RESTful HTTP API into gRPC. Barito flow infrastructure consists of producer and consumer.

Barito flow producer will turn on gRPC server and optionally REST gateway reverse proxy server.
It will automatically create Kafka topic for the log if not exist yet.
gRPC messages and services are declared in [barito-proto](https://github.com/bentol/barito-proto) repository.

Barito flow consumer will firstly create a topic event and generates the workers. Then based on the logs send,
each topic inside this event topic will be created a cluster consumer separately. This cluster consumer will
store the logs to Elasticsearch by calling a single store or bulk store Elasticsearch API. If the process has
failed, Elasticsearch will halt all the workers and retry again after some backoff period. The halted workers
only continue when the failed process success on the retry attempt.

## Development Setup

If running on local machine with ARM (e.g. Apple M1, Apple M2) Chipset, run below command.
```sh
go env -w GOARCH=amd64
```

Fetch and build the project.
```sh
git clone https://github.com/BaritoLog/barito-flow
cd barito-flow
go build
```

Generate mock classes.
```sh
mockgen -source=flow/leaky_bucket.go -destination=mock/leaky_bucket.go -package=mock
mockgen -source=flow/kafka_admin.go -destination=mock/kafka_admin.go -package=mock
mockgen -source=flow/Vendor/github.com/sarama/sync_producer.go -destination=mock/sync_producer.go -package=mock
```

### Running Test Stack using Docker Compose

First, you need to install Docker on your local machine. Then you can run `docker-compose`:

```sh
$ docker-compose -f docker/docker-compose.yml up -d
```

This will pull Elasticsearch, Kafka, and build producer and consumer image. The ports
are mapped as if they are running on local machine.

### Run Unit Tests

```sh
make test
```

### Check Vulnerability

```sh
make vuln
```

### Check Deadcode

```sh
make deadcode
```

## Producer Mode

Responsible for:
- Receive logs by exposing an HTTP endpoint
- Produce message to kafka cluster

After the project is built, run:
```sh
./barito-flow producer

# or
./barito-flow p
```

Endpoints using REST gateway:

POST /produce
```json
  {
    "context": {
        "kafka_topic": "kafka_topic",
        "kafka_partition": 1,
        "kafka_replication_factor": 1,
        "es_index_prefix": "es_index_prefix",
        "es_document_type": "es_document_type",
        "app_max_tps": 100,
        "app_secret": "app_secret"
    },
    "timestamp": "optional timestamp here",
    "content": {
        "hello": "world",
        "key": "value",
        "num": 100
    },
  }
```

POST /produce_batch
```json
  {
    "context": {
        "kafka_topic": "kafka_topic",
        "kafka_partition": 1,
        "kafka_replication_factor": 1,
        "es_index_prefix": "es_index_prefix",
        "es_document_type": "es_document_type",
        "app_max_tps": 100,
        "app_secret": "app_secret"
    },
    "items": [
        {
            "content": {
                "timber_num": 1
            }
        },
        {
            "content": {
                "timber_num": 2
            }
        }
    ]
  }
```

These environment variables can be modified to customize its behaviour.

| Name| Description | ENV | Default Value  |
| ---|---|---|---|
| ConsulUrl | Consul URL | BARITO_CONSUL_URL | |
| ConsulKafkaName | Kafka service name in consul | BARITO_CONSUL_KAFKA_NAME | kafka |
| KafkaBrokers | Kafka broker addresses (CSV). Get from env is not available in consul | BARITO_KAFKA_BROKERS | localhost:9092 |
| KafkaMaxRetry | Number of retry to connect to kafka during startup | BARITO_KAFKA_MAX_RETRY | 0 (unlimited) |
| KafkaRetryInterval | Interval between retry connecting to kafka (in seconds) | BARITO_KAFKA_RETRY_INTERVAL | 10 |
| ServeRestApi | Toggle for REST gateway api | BARITO_PRODUCER_REST_API | true |
| ProducerAddressGrpc | gRPC Server Address | BARITO_PRODUCER_GRPC| :8082 |
| ProducerAddressRest | REST Server Address | BARITO_PRODUCER_REST| :8080 |
| ProducerMaxRetry | Set kafka setting max retry | BARITO_PRODUCER_MAX_RETRY | 10 |
| ProducerMaxTps | Producer rate limit trx per second | BARITO_PRODUCER_MAX_TPS | 100 |
| ProducerRateLimitResetInterval | Producer rate limit reset interval (in seconds) | BARITO_PRODUCER_RATE_LIMIT_RESET_INTERVAL | 10 |

## Consumer Mode

Responsible for:
- Consume logs from kafka
- Commit logs to elasticsearch

After the project is built, run:
```sh
./barito-flow Consumer

# or
./barito-flow c
```

These environment variables can be modified to customize its behaviour.

| Name| Description | ENV | Default Value  |
| ---|---|----|----|
| ConsulUrl | Consul URL | BARITO_CONSUL_URL | |
| ConsulKafkaName  | Kafka service name in consul | BARITO_CONSUL_KAFKA_NAME | kafka |
| ConsulElasticsearchName | Elasticsearch service name in consul | BARITO_CONSUL_ELASTICSEARCH_NAME | elasticsearch |
| KafkaBrokers | Kafka broker addresses (CSV). Get from env is not available in consul | BARITO_KAFKA_BROKERS| "127.0.0.1:9092,192.168.10.11:9092" |
| KafkaGroupID | kafka consumer group id | BARITO_KAFKA_GROUP_ID | barito-group |
| KafkaMaxRetry | Number of retry to connect to kafka during startup | BARITO_KAFKA_MAX_RETRY | 0 (unlimited) |
| KafkaRetryInterval | Interval between retry connecting to kafka (in seconds) | BARITO_KAFKA_RETRY_INTERVAL | 10 |
| ElasticsearchUrls | Elastisearch addresses. Get from env is not available in consul | BARITO_ELASTICSEARCH_URLS | "http://127.0.0.1:9200,http://192.168.10.11:9200" |
| EsIndexMethod | BulkProcessor / SingleInsert | BARITO_ELASTICSEARCH_INDEX_METHOD | BulkProcessor |
| EsBulkSize | BulkProcessor bulk size | BARITO_ELASTICSEARCH_BULK_SIZE | 100 |
| EsFlushIntervalMs | BulkProcessor flush interval (ms) | BARITO_ELASTICSEARCH_FLUSH_INTERVAL_MS | 500 |
| PrintTPS | print estimated consumed every second | BARITO_PRINT_TPS | false |
| PushMetricUrl | push metric api url | BARITO_PUSH_METRIC_URL|   |
| PushMetricInterval | push metric interval | BARITO_PUSH_METRIC_INTERVAL | 30s |

**NOTE**
These following variables will be ignored if `BARITO_ELASTICSEARCH_INDEX_METHOD` is set to `SingleInsert`

- `BARITO_ELASTICSEARCH_BULK_SIZE`
- `BARITO_ELASTICSEARCH_FLUSH_INTERVAL_MS`

### Changelog

See [CHANGELOG.md](CHANGELOG.md)

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

### License

MIT License, See [LICENSE](LICENSE).
