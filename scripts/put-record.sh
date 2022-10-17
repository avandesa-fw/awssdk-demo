#!/usr/bin/env bash

STREAM=$1
KEY=$2
VAL=$3

ENCODED=$(echo "$VAL" | base64)

awslocal kinesis put-record --stream-name="$STREAM" --partition-key="$KEY" --data="$ENCODED"
