package main

import (
	"os"

	blockchain "tp1.aba.distros.fi.uba.ar/node/blockchain/node"
	client "tp1.aba.distros.fi.uba.ar/node/client/node"
	service "tp1.aba.distros.fi.uba.ar/node/service/node"
)

func main() {
	// Run the specific node depending on program parameters.
	program := os.Args[1]

	switch program {
	case "service":
		// Run the blockchain API service.
		service.Run()
	case "blockchain":
		// Run the blockchain.
		blockchain.Run()
	default:
		// Run the client.
		client.Run()
	}
}
