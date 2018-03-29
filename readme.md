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

## Receiver

Responsible to:
- expose a produce URL to receive log message
- forward/produce log message to respective kafka broker

Receiver will retrieve its own configuration from `Barito Market`

Run
```sh
./barito-agent receiver

#or
./barito-agent r
```


## Forwarder

Responsible to:
- consume log message from kafka
- forward log message to respective store (elasticsearch)


Forwarder will retrieve its own configuration from `Barito Market`

Run
```sh
./barito-agent forwarder

# or
./barito-agent f
```

## Kafka

Make sure you have run zookeeper & kafka-server, and change `kafkaBrokers` in `receiver/configuration.go` with your brokers list. 

## Elasticsearch

Make sure you have Elasticsearch 6.x installed and running, and change `elasticsearchUrls` & `elasticsearchIndexPrefix` in `forwarder/configuration.go` with your settings.

## Kubernetes

```sh
$ docker build -t barito-flow:latest .
$ kubectl apply -f barito-flow-kubernetes.yaml
```

Note: You can run `deploy.sh` to automate above steps. 
Usage : 
* `$ deploy.sh development` to build docker from local files
* `$ deploy.sh` to build docker from docker hub