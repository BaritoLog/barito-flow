#!/bin/sh

ENV=$1
if [ "${ENV}" = "development" ]
then
    GOOS=linux GOARCH=amd64 go build
    docker build -t barito-flow:latest -f Dockerfile.development .
else
    docker build -t barito-flow:latest .
fi

kubectl delete deployment barito-flow
kubectl delete service barito-flow
kubectl delete service ext-kafka
kubectl apply -f barito-flow-kubernetes.yaml