#!/bin/sh

SERVER_NAME=$1
SERVER_PORT=$2

echo 'echotest' | nc $SERVER_NAME $SERVER_PORT