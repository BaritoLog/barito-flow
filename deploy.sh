#!/bin/sh

ENV=$1
if [ "${ENV}" = "development" ]
then
    GOOS=linux GOARCH=amd64 go build
    docker build -t barito-flow:0.0.1 -f Dockerfile.development .
else
    docker build -t barito-flow:0.0.1 -f Dockerfile .
fi

kubectl delete -f barito-flow-kubernetes.yaml
kubectl apply -f barito-flow-kubernetes.yaml