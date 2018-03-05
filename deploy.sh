#!/bin/sh

docker build -t barito-flow:latest .
kubectl delete deployment barito-flow
kubectl delete service barito-flow
kubectl apply -f barito-flow-kubernetes-deployment.yaml
kubectl apply -f barito-flow-kubernetes-service.yaml