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
| BARITO_KAFKA_PRODUCER_TOPIC| kafka topic | topic01 |
| BARITO_PRODUCER_MAX_RETRY| set kafka setting max retry | 10 |


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


INFO[0000] BARITO_KAFKA_BROKERS=[localhost:9092]
INFO[0000] BARITO_KAFKA_GROUP_ID=barito-group
INFO[0000] BARITO_KAFKA_CONSUMER_TOPICS=[topic01]
INFO[0000] BARITO_ELASTICSEARCH_URL=http://localhost:9200
INFO[0000] BARITO_PUSH_METRIC_URL=http://localhost:3000/api/increase_log_count
INFO[0000] BARITO_PUSH_METRIC_TOKEN=
INFO[0000] BARITO_PUSH_METRIC_INTERVAL=30s

Environment Variables

| Name| Description | Default Value  |
| ---|---|----|
| BARITO_KAFKA_BROKERS| kafka broker address | localhost:9092 |
| BARITO_KAFKA_GROUP_ID| kafka group id | barito-group |
| BARITO_KAFKA_CONSUMER_TOPICS| kafka consumer topics (CSV) | topic01 |
| BARITO_ELASTICSEARCH_URL| elastisearch url | http://localhost:9200 |
| BARITO_PUSH_METRIC_URL| push metric api url | http://localhost:3000/api/increase_log_count |
| BARITO_PUSH_METRIC_TOKEN| push metric api token |  |
| BARITO_PUSH_METRIC_INTERVAL| push metric interval | 30s |
