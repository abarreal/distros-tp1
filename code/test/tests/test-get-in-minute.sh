#!/bin/sh

UNIX_TIMESTAMP=$1

echo "Requesting blocks in minute that contains time $UNIX_TIMESTAMP"

/node minute $UNIX_TIMESTAMP