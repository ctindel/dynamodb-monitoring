#!/bin/bash

pip install -t /tmp/src/vendored/ -r /tmp/src/requirements.txt

while true
do
    echo "Press [CTRL+C] to stop.."
    sleep 1
done

cd /tmp/src; serverless deploy || exit 1
