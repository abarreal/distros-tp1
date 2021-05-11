package node

import (
	"fmt"
	"net"
	"os"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/interface/message"
)

func Run() {
	logging.Initialize("Client")

	// Identify the argument for the program.
	cmd := os.Args[1]
	// Handle depending on first argument.
	switch cmd {
	case "write":
		handleWrite()
	case "block":
		handleBlockRequest()
	case "minute":
		handleBlocksInMinuteRequest()
	}
}

func handleWrite() {
	// The data to write is the second argument.
	data := []byte(os.Args[2])

	if len(data) > 65535 {
		data = data[:65535]
	}

	if len(data) < 32 {
		logging.Log(fmt.Sprintf("Sending write chunk request: %s", data))
	} else {
		logging.Log("Sending write chunk request")
	}

	// Instantiate the write chunk request.
	request := message.CreateWriteChunk(data, uint16(len(data)))
	response, err := send(request)

	if err != nil {
		fmt.Println("There was an error while processing your request")
	}

	r := response.(*message.WriteChunkResponse)

	// Print whether the request was accepted or not.
	logging.Log("Write request sent")
	logging.Log(fmt.Sprintf("Accepted: %t", r.Accepted()))
}

func handleBlockRequest() {
	// TODO
}

func handleBlocksInMinuteRequest() {
	// TODO
}

func send(request message.Message) (message.Message, error) {
	// Open a connection with the blockchain service.
	serverName := config.GetStringOrDefault("ServerName", "localhost")
	serverPort := config.GetStringOrDefault("ServerPort", "9000")

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", serverName, serverPort))

	if err != nil {
		logging.LogError("Could not connect to server", err)
	}

	defer conn.Close()

	// Send the request through the channel.
	if err := request.Write(conn); err != nil {
		logging.LogError("Could not send message", err)
	}
	// Attempt to receive a response.
	response, err := message.ReadMessage(conn)

	if err != nil {
		logging.LogError("Could not receive response", err)
	}

	return response, nil
}
