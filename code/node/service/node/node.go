package node

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/common/server"
	"tp1.aba.distros.fi.uba.ar/interface/message"
	"tp1.aba.distros.fi.uba.ar/node/service/domain"
)

// Define a configuration path.
const configPath string = "/etc/distros/config/service.env"

// Run the code for the Blockchain API service.
func Run() {
	logging.Initialize("Service")

	// Load configuration.
	logging.Log("Loading service configuration")
	config.UseFile(configPath)

	// Create a wait group for servers and other services.
	waitGroup := &sync.WaitGroup{}

	// Instantiate the blockchain service domain object.
	logging.Log("Initializing blockchain service")
	svc, err := domain.CreateBlockchainService()

	if err != nil {
		logging.LogError("Could not initialize blockchain service", err)
		return
	} else {
		svc.RegisterOnWaitGroup(waitGroup)
	}

	// Instantiate read and write server configuration.
	logging.Log("Reading server configuration")
	rServerPort, _ := config.GetIntOrDefault("ReadServerPort", 9000)
	rServerConfig := &server.ServerConfig{
		Port:        uint16(rServerPort),
		WorkerCount: 4,
	}
	wServerPort, _ := config.GetIntOrDefault("WriteServerPort", 9010)
	wServerConfig := &server.ServerConfig{
		Port:        uint16(wServerPort),
		WorkerCount: 1,
	}

	// Instantiate the servers.
	wServer := server.CreateNew(wServerConfig, func(conn *net.Conn) {
		handleWriteConnection(svc, *conn)
	})
	rServer := server.CreateNew(rServerConfig, func(conn *net.Conn) {
		handleReadConnection(svc, *conn)
	})

	wServer.RegisterOnWaitGroup(waitGroup)
	rServer.RegisterOnWaitGroup(waitGroup)

	// Handle control connections.
	logging.Log("Setting up signal handlers")
	go handleSignals([]*server.Server{wServer, rServer}, svc)

	// Run the blockchain service.
	logging.Log("Launching service goroutines")
	go svc.Run()

	// Initialize read and write servers. The last server to stop will
	// run on the main thread.
	logging.Log("Launching server")
	go wServer.Run()
	go rServer.Run()

	// Wait for services to finish.
	waitGroup.Wait()
}

// Initialize signal handling to quit the server when any of the specified
// signals are provided. When a signal is received, all given servers will
// be told to stop.
func handleSignals(servers []*server.Server, svc *domain.BlockchainService) {
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
	// Stop the service as well.
	logging.Log("Stopping service goroutines")
	svc.Stop()
}

//-------------------------------------------------------------------------------------------------
// Write connections
//-------------------------------------------------------------------------------------------------

func handleWriteConnection(svc *domain.BlockchainService, conn net.Conn) {
	// Read incoming message.
	msg, err := message.ReadMessage(conn)

	if err != nil {
		logging.LogError("Could not read client message", err)
		return
	}

	// Only accept WriteChunk requests.
	if msg.Opcode() != message.OpWriteChunk {
		logging.Log("Unexpected request type")
		return
	}

	// Forward the request to the write pipeline.
	request := msg.(*message.WriteChunk)
	if response, err := svc.HandleWriteChunk(request); err != nil {
		logging.LogError("Could not handle write request", err)
	} else {
		response.Write(conn)
	}
}

//-------------------------------------------------------------------------------------------------
// Read connections
//-------------------------------------------------------------------------------------------------

func handleReadConnection(svc *domain.BlockchainService, conn net.Conn) {
	// Read incoming message.
	msg, err := message.ReadMessage(conn)

	if err != nil {
		logging.LogError("Could not read client message", err)
		return
	}

	// Handle the message depending on opcode.
	switch msg.Opcode() {
	case message.OpGetBlockWithHash:
		handleGetBlockWithHashRequest(svc, msg, conn)
	case message.OpGetBlocksInMinute:
		handleGetBlocksInMinute(svc, msg, conn)
	case message.OpGetMiningStatistics:
		handleGetMiningStatistics(svc, msg, conn)
	}
}

func handleGetBlockWithHashRequest(svc *domain.BlockchainService, msg message.Message, conn net.Conn) {
	logging.Log("Handling get block by hash request")
	if response, err := svc.HandleGetBlock(msg.(*message.GetBlockByHashRequest)); err != nil {
		logging.LogError("Find with hash request failed", err)
	} else {
		logging.Log("Writing response")
		response.Write(conn)
	}
}

func handleGetBlocksInMinute(svc *domain.BlockchainService, msg message.Message, conn net.Conn) {
	logging.Log("Handling get blocks in minute request")
	if response, err := svc.HandleGetBlocksFromMinute(msg.(*message.ReadBlocksInMinuteRequest)); err != nil {
		logging.LogError("Find in minute request failed", err)
	} else {
		logging.Log("Writing response")
		response.Write(conn)
	}
}

func handleGetMiningStatistics(svc *domain.BlockchainService, msg message.Message, conn net.Conn) {
	logging.Log("Handling get mining statistics request")
	if response, err := svc.HandleGetMiningStatistics(msg.(*message.GetMiningStatistics)); err != nil {
		logging.LogError("Get mining statistics request failed", err)
	} else {
		logging.Log("Writing response")
		response.Write(conn)
	}
}
