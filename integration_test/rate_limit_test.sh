#!/bin/sh


for i in $(seq 11);
do
  curl -X POST \
  http://localhost:8080/ \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -H 'postman-token: 05b0e17e-1c53-1383-f6fc-57338b8eca2a' \
  -d "{\"didiet\": \"$i\"}"
  printf "\n"
done

echo "sleep 1s"
sleep 1

for i in $(seq 10);
do
  curl -X POST \
  http://localhost:8080/ \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -H 'postman-token: 05b0e17e-1c53-1383-f6fc-57338b8eca2a' \
  -d "{\"didiet\": \"$i\"}"
  printf "\n"
done
