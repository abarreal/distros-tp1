#!/bin/sh

HASH_HEX=$1

echo "Requesting block with hash $HASH_HEX"

/node block $HASH_HEX