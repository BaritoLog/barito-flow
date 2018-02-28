# Barito Agent

Provide kafka reciever or log forwarder for Barito project

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
