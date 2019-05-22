#!/bin/bash

trap "docker-compose -f dynamodb_metrics_lambda/docker-compose.yml rm --force" SIGINT SIGTERM
docker-compose -f dynamodb_metrics_lambda/docker-compose.yml build --no-cache remove_dynamodb_metrics_lambda
docker-compose -f dynamodb_metrics_lambda/docker-compose.yml up remove_dynamodb_metrics_lambda
docker-compose -f dynamodb_metrics_lambda/docker-compose.yml rm --force
