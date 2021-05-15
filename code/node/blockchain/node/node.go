package node

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/common/server"
	"tp1.aba.distros.fi.uba.ar/interface/message"
	"tp1.aba.distros.fi.uba.ar/node/blockchain/domain"
	"tp1.aba.distros.fi.uba.ar/node/blockchain/repository"
)

// Define the path to a configuration file for the blockchain.
const configPath string = "/etc/distros/config/blockchain.env"

func Run() {
	logging.Initialize("Blockchain")

	// Load configuration.
	logging.Log("Loading configuration file")
	config.UseFile(configPath)

	// Instantiate a block repository object.
	logging.Log("Initializing repository")
	repo, _ := repository.CreateBlockRepository()

	logging.Log("Initializing blockchain")
	// Instantiate a Blockchain object.
	blockchain := domain.CreateBlockchain(repo)

	// Instantiate read and write server configuration.
	logging.Log("Reading server configuration")
	rServerPort, _ := config.GetIntOrDefault("ReadServerPort", 8000)
	rServerConfig := &server.ServerConfig{
		Port:        uint16(rServerPort),
		WorkerCount: 4,
	}
	wServerPort, _ := config.GetIntOrDefault("WriteServerPort", 8010)
	wServerConfig := &server.ServerConfig{
		Port:        uint16(wServerPort),
		WorkerCount: 1,
	}

	// Instantiate the servers.
	wServer := server.CreateNew(wServerConfig, func(conn *net.Conn) {
		handleWriteConnection(blockchain, conn)
	})
	rServer := server.CreateNew(rServerConfig, func(conn *net.Conn) {
		handleReadConnection(blockchain, conn)
	})

	// Handle control connections.
	logging.Log("Setting up signal handlers")
	go handleSignals([]*server.Server{wServer, rServer})

	// Initialize read and write servers. The last server to stop will
	// run on the main thread.
	logging.Log("Launching server")
	go wServer.Run()
	rServer.Run()
}

// Initialize signal handling to quit the server when any of the specified
// signals are provided. When a signal is received, all given servers will
// be told to stop.
func handleSignals(servers []*server.Server) {
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	// There are only quit signals to handle. The program should
	// quit as soon as one is received.
	<-sigchannel
	// Stop all servers.
	for i, srv := range servers {
		logging.Log(fmt.Sprintf("Stopping server %d", i))
		srv.Stop()
	}
}

func handleWriteConnection(blockchain *domain.Blockchain, conn *net.Conn) {
	msg, err := message.ReadMessage(*conn)

	if err != nil {
		logging.LogError("Write - Could not read message", err)
		return
	}
	if msg.Opcode() != message.OpWriteBlock {
		logging.LogError("Write - Unexpected opcode in response", err)
		return
	}

	// Handle the write request.
	block := msg.(*message.WriteBlock).Block()

	logging.Log("Received block to be written")
	logging.Log(fmt.Sprintf("Block hash: %s", block.Hash().Hex()))
	logging.Log(fmt.Sprintf("Block previous hash: %s", block.PreviousHash().Hex()))
	logging.Log(fmt.Sprintf("Block difficulty: %s", block.Difficulty().Hex()))
	logging.Log(fmt.Sprintf("Block timestamp: %d", block.Timestamp()))

	logging.Log("Attempting to write block to the blockchain")
	err = blockchain.WriteBlock(block)

	if err != nil {
		logging.LogError("Could not write block", err)
	}

	accepted := (err == nil)
	response := message.CreateWriteBlockResponse(
		accepted,
		blockchain.CurrentPreviousHash(),
		blockchain.CurrentDifficulty())

	// Send the response back through the channel.
	if accepted {
		logging.Log("Write request accepted")
	} else {
		logging.LogError("Write request rejected", err)
	}

	response.Write(*conn)
}

func handleReadConnection(blockchain *domain.Blockchain, conn *net.Conn) {
	msg, err := message.ReadMessage(*conn)

	if err != nil {
		logging.LogError("Read - Could not read message", err)
		return
	}

	switch msg.Opcode() {
	case message.OpGetMiningInfo:
		handleGetMiningInfo(blockchain, msg, *conn)
	case message.OpGetBlockWithHash:
		handleGetBlockWithHash(blockchain, msg, *conn)
	case message.OpGetBlocksInMinute:
		handleGetBlocksInMinute(blockchain, msg, *conn)
	}
}

func handleGetMiningInfo(blockchain *domain.Blockchain, msg message.Message, conn net.Conn) {
	logging.Log("Handling GetMiningInfo request")
	previousHash := blockchain.CurrentPreviousHash()
	currentDifficulty := blockchain.CurrentDifficulty()
	response := message.CreateGetMiningInfoResponse(previousHash, currentDifficulty)

	// Log current previous hash and difficulty as returned to the client.
	logging.Log(fmt.Sprintf("Writing GetMiningInfo response (%s, %s)",
		previousHash.Hex(),
		currentDifficulty.Hex()))

	if err := response.Write(conn); err != nil {
		logging.LogError("Could not send response", err)
	}
}

func handleGetBlockWithHash(blockchain *domain.Blockchain, msg message.Message, conn net.Conn) {
	logging.Log("Handling GetBlockByHash request")

	request := msg.(*message.GetBlockByHashRequest)
	hash := request.Hash()

	logging.Log(fmt.Sprintf("Requested hash: %s", hash.Hex()))

	if block, err := blockchain.GetOneWithHash(hash); err != nil {
		logging.LogError("Could not retrieve requested block", err)
	} else {
		logging.Log(fmt.Sprintf("Block %s found, sending response", block.Hash().Hex()))
		// Generate response.
		response := message.CreateGetBlockByHashResponse(block)
		// Send response back to the client.
		if err := response.Write(conn); err != nil {
			logging.LogError("Could not send response", err)
		}
	}
}

func handleGetBlocksInMinute(blockchain *domain.Blockchain, msg message.Message, conn net.Conn) {
	logging.Log("Handling ReadBlocksInMinute request")

	request := msg.(*message.ReadBlocksInMinuteRequest)

	requestedTimestamp := request.Timestamp()
	requestedTime := time.Unix(requestedTimestamp, 0).UTC()

	logging.Log(fmt.Sprintf("Requested timestamp: %d", requestedTimestamp))

	if blocks, err := blockchain.GetBlocksFromMinute(requestedTime); err != nil {
		logging.LogError("Could not retrieve list of blocks", err)
	} else {
		logging.Log(fmt.Sprintf("Found %d blocks", len(blocks)))
		// Generate the response.
		response, err := message.CreateReadBlocksInMinuteResponse(requestedTimestamp, blocks)

		if err != nil {
			logging.LogError("Could not create response", err)
		}
		if err := response.Write(conn); err != nil {
			logging.LogError("Could not send response", err)
		}
	}
}
