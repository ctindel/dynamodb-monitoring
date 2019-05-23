#!/bin/bash

: "${AWS_ACCESS_KEY_ID:?Need to set AWS_ACCESS_KEY_ID non-empty}"
: "${AWS_SECRET_ACCESS_KEY:?Need to set AWS_SECRET_ACCESS_KEY non-empty}"
: "${AWS_DEFAULT_REGION:?Need to set AWS_DEFAULT_REGION non-empty}"

pip install -t /tmp/src/vendored/ -r /tmp/src/requirements.txt

while true
do
    echo "Press [CTRL+C] to stop.."
    sleep 1
done
