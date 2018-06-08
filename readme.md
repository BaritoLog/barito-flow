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

| Name| Description | Default Value  |
| ---|---|----|
| BARITO_PRODUCER_ADDRESS| Http Server Address | :8080 |
| BARITO_KAFKA_BROKERS| Kafka broker addresses (CSV)| localhost:9092 |
| BARITO_KAFKA_PRODUCER_TOPIC| kafka topic | producer-topic |
| BARITO_PRODUCER_MAX_RETRY| set kafka setting max retry | 10 |
| BARITO_PRODUCER_MAX_TPS| producer rate limit trx per second | 100 |


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

| Name| Description | Default Value  |
| ---|---|----|
| BARITO_KAFKA_BROKERS| kafka broker address | localhost:9092 |
| BARITO_KAFKA_GROUP_ID| kafka group id | barito-group |
| BARITO_KAFKA_CONSUMER_TOPICS| kafka consumer topics (CSV) | consumer-topic |
| BARITO_ELASTICSEARCH_URL| elastisearch url | http://localhost:9200 |
| BARITO_PUSH_METRIC_URL| push metric api url | http://localhost:3000/api/increase_log_count |
| BARITO_PUSH_METRIC_TOKEN| push metric api token |  |
| BARITO_PUSH_METRIC_INTERVAL| push metric interval | 30s |


## Changes Log

#### 0.5.0 
- Rate limit trx per second (by default 100)
