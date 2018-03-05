# Barito Flow

Building flow of Barito river with provide kafka reciever or log forwarder 

## Setup Development

```sh
cd $GOPATH/src
git clone git@source.golabs.io:infrastructure/barito/barito-agent.git 

cd barito-agent
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

## Kubernetes

```sh
$ docker build -t barito-flow:latest .
$ kubectl apply -f barito-flow-kubernetes-deployment.yaml
$ kubectl apply -f barito-flow-kubernetes-service.yaml
```

Note: You can run `deploy.sh` to automate above steps