#!/bin/sh

echo "Sending 5 chunks to be written to the blockchain"

# Execute the node binary, which by default executes the client.
/node write "Chunk 1"
/node write "Chunk 2"
/node write "Chunk 3"
/node write "Chunk 4"
/node write "Chunk 5"