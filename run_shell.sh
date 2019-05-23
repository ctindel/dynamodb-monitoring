#!/bin/bash

trap "docker-compose -f dynamodb_metrics_lambda/docker-compose.yml rm --force" SIGINT SIGTERM
docker-compose -f dynamodb_metrics_lambda/docker-compose.yml build --no-cache run_shell_dynamodb_metrics_lambda
docker-compose -f dynamodb_metrics_lambda/docker-compose.yml up run_shell_dynamodb_metrics_lambda
docker-compose -f dynamodb_metrics_lambda/docker-compose.yml rm --force
