package node

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/common/server"
	"tp1.aba.distros.fi.uba.ar/interface/message"
	service "tp1.aba.distros.fi.uba.ar/node/service/domain"
	"tp1.aba.distros.fi.uba.ar/node/service/middleware"
)

// Define a configuration path.
const configPath string = "/etc/distros/config/service.env"

// Run the code for the Blockchain API service.
func Run() {
	logging.Initialize("Service")

	// Load configuration.
	logging.Log("Loading service configuration")
	config.UseFile(configPath)

	// Instantiate the blockchain middleware.
	logging.Log("Initializing blockchain middleware")
	blockchain, err := middleware.CreateBlockchain()

	if err != nil {
		logging.LogError("Could not initialize middleware", err)
		return
	}

	// Instantiate the blockchain service domain object.
	logging.Log("Initializing blockchain service")
	svc := service.CreateBlockchainService(blockchain)

	// Instantiate and configure the server.
	serverPort, _ := config.GetIntOrDefault("ServerPort", 9000)
	serverConf := &server.ServerConfig{
		Port:        uint16(serverPort),
		WorkerCount: 4,
	}
	server := server.CreateNew(serverConf, func(conn *net.Conn) {
		handleClientConnection(svc, *conn)
	})

	// Handle signals.
	go handleSignals(server)
	// Run the server.
	logging.Log(fmt.Sprintf("Launching server listening on port %d", serverPort))
	server.Run()
}

// Initialize signal handling to quit the server when any of the specified
// signals are provided. When a signal is received, the server will stop.
func handleSignals(server *server.Server) {
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	// There are only quit signals to handle. The program should
	// quit as soon as one is received.
	<-sigchannel
	// Stop the server.
	server.Stop()
}

func handleClientConnection(svc *service.BlockchainService, conn net.Conn) {
	// Read the incoming message. The blockchain service only accepts
	// read messages and a write message in which the block is not a block
	// but a sequence of bytes to write to the blockchain.
	msg, err := message.ReadMessage(conn)

	if err != nil {
		logging.LogError("Could not read client message", err)
		return
	}

	// Handle the message depending on opcode.
	switch msg.Opcode() {
	case message.OpWriteChunk:
		handleWriteChunkRequest(svc, msg, conn)
	case message.OpGetBlockWithHash:
		handleGetBlockWithHashRequest(svc, msg, conn)
	case message.OpGetBlocksInMinute:
		handleGetBlocksInMinute(svc, msg, conn)
	}
}

func handleWriteChunkRequest(svc *service.BlockchainService, msg message.Message, conn net.Conn) {
	logging.Log("Handling write chunk request")
	if response, err := svc.HandleWriteChunk(msg.(*message.WriteChunk)); err != nil {
		logging.LogError("Write request failed", err)
	} else {
		logging.Log("Writing response")
		response.Write(conn)
	}
}

func handleGetBlockWithHashRequest(svc *service.BlockchainService, msg message.Message, conn net.Conn) {
	logging.Log("Handling get block by hash request")
	if response, err := svc.HandleGetBlock(msg.(*message.GetBlockByHashRequest)); err != nil {
		logging.LogError("Find with hash request failed", err)
	} else {
		logging.Log("Writing response")
		response.Write(conn)
	}
}

func handleGetBlocksInMinute(svc *service.BlockchainService, msg message.Message, conn net.Conn) {
	logging.Log("Handling get blocks in minute request")
	if response, err := svc.HandleGetBlocksFromMinute(msg.(*message.ReadBlocksInMinuteRequest)); err != nil {
		logging.LogError("Find in minute request failed", err)
	} else {
		logging.Log("Writing response")
		response.Write(conn)
	}
}
