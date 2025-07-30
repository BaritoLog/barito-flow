# Barito Flow

![Build Status](https://travis-ci.org/BaritoLog/barito-flow.svg?branch=master)

Barito Flow is a core component for handling log flow within a Barito cluster. It operates in two distinct modes:

- **Producer Mode**: Receives logs from various sources and forwards them to Kafka
- **Consumer Mode**: Consumes logs from Kafka and forwards them to Elasticsearch

## Features

- **Dual API Support**: Compatible with both gRPC and REST API
- **High Performance**: Optimized for high-throughput log processing
- **Flexible Configuration**: Environment-based configuration for different deployment scenarios
- **Rate Limiting**: Built-in rate limiting to prevent system overload
- **Auto-discovery**: Supports Consul for service discovery
- **Resilient**: Built-in retry mechanisms and error handling

## Architecture

Barito Flow uses [gRPC-gateway](https://github.com/grpc-ecosystem/grpc-gateway) as a reverse-proxy server to translate RESTful HTTP API into gRPC. The gRPC messages and services are declared in the [barito-proto](https://github.com/bentol/barito-proto) repository.

### Producer Mode Flow

1. Receives logs via HTTP/gRPC endpoints
2. Automatically creates Kafka topics if they don't exist
3. Publishes logs to appropriate Kafka topics

### Consumer Mode Flow

1. Creates topic events and generates workers
2. Consumes logs from Kafka topics
3. Stores logs to Elasticsearch using either bulk operations or single inserts
4. Implements retry mechanisms with backoff for failed operations

## Development Setup

### Prerequisites

For ARM-based local machines (e.g., Apple M1, Apple M2), set the Go architecture:

```sh
go env -w GOARCH=amd64
```

### Build from Source

Fetch and build the project:

```sh
git clone https://github.com/BaritoLog/barito-flow
cd barito-flow
go build
```

### Generate Mock Classes

```sh
mockgen -source=flow/leaky_bucket.go -destination=mock/leaky_bucket.go -package=mock
mockgen -source=flow/kafka_admin.go -destination=mock/kafka_admin.go -package=mock
mockgen -source=flow/Vendor/github.com/sarama/sync_producer.go -destination=mock/sync_producer.go -package=mock
```

### Running Test Stack using Docker Compose

First, install Docker on your local machine. Then run docker-compose:

```sh
docker-compose -f docker/docker-compose.yml up -d
```

This will pull Elasticsearch, Kafka, and build producer and consumer images. The ports are mapped as if they are running on local machine.

### Testing

Run unit tests:

```sh
make test
```

Check for vulnerabilities:

```sh
make vuln
```

Check for dead code:

```sh
make deadcode
```

## Producer Mode

Producer mode is responsible for:

- Receiving logs by exposing HTTP/gRPC endpoints
- Producing messages to Kafka cluster

After the project is built, run:

```sh
./barito-flow producer

# or
./barito-flow p
```

### REST API Endpoints

#### POST /produce

Single log entry endpoint:

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
  }
}
```

#### POST /produce_batch

Batch log entries endpoint:

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

### Producer Configuration

These environment variables can be modified to customize producer behavior:

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

Consumer mode is responsible for:

- Consuming logs from Kafka
- Committing logs to Elasticsearch

After the project is built, run:

```sh
./barito-flow Consumer

# or
./barito-flow c
```

### Consumer Configuration

These environment variables can be modified to customize consumer behavior:

| Name| Description | ENV | Default Value  |
| ---|---|----|----|
| ConsulUrl | Consul URL | BARITO_CONSUL_URL | |
| ConsulKafkaName  | Kafka service name in consul | BARITO_CONSUL_KAFKA_NAME | kafka |
| ConsulElasticsearchName | Elasticsearch service name in consul | BARITO_CONSUL_ELASTICSEARCH_NAME | elasticsearch |
| KafkaBrokers | Kafka broker addresses (CSV). Get from env is not available in consul | BARITO_KAFKA_BROKERS| "127.0.0.1:9092,192.168.10.11:9092" |
| KafkaGroupID | kafka consumer group id | BARITO_KAFKA_GROUP_ID | barito-group |
| KafkaMaxRetry | Number of retry to connect to kafka during startup | BARITO_KAFKA_MAX_RETRY | 0 (unlimited) |
| KafkaRetryInterval | Interval between retry connecting to kafka (in seconds) | BARITO_KAFKA_RETRY_INTERVAL | 10 |
| ElasticsearchUrls | Elasticsearch addresses. Get from env if not available in consul | BARITO_ELASTICSEARCH_URLS | `"http://127.0.0.1:9200,http://192.168.10.11:9200"` |
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
