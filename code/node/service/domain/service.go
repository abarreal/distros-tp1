package domain

import (
	"errors"
	"fmt"
	"sync"

	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/interface/message"
	"tp1.aba.distros.fi.uba.ar/node/service/middleware"
)

// The blockchain service acts as the entry point and the request dispatcher.
type BlockchainService struct {
	waitGroup      *sync.WaitGroup
	blockchain     *middleware.Blockchain
	controlChannel chan int
	stopping       bool
	inputQueue     *ChunkQueue
}

func CreateBlockchainService() (*BlockchainService, error) {
	svc := &BlockchainService{}
	svc.stopping = false
	svc.waitGroup = nil
	svc.controlChannel = make(chan int)
	// Instantiate blockchain middleware.
	if blockchain, err := middleware.CreateBlockchain(); err != nil {
		return nil, err
	} else {
		svc.blockchain = blockchain
	}
	// Instantiate an input queue to hold incoming write requests.
	svc.inputQueue = CreateChunkQueue()
	return svc, nil
}

func (svc *BlockchainService) RegisterOnWaitGroup(wg *sync.WaitGroup) error {
	if svc.waitGroup != nil {
		return errors.New("already registered on WG")
	}
	logging.Log("Registering blockchain service on wait group")
	svc.waitGroup = wg
	svc.waitGroup.Add(1)
	return nil
}

//=================================================================================================
// Run
//-------------------------------------------------------------------------------------------------

func (svc *BlockchainService) Run() {
	logging.Log("Blockchain service starting")

	// Create a wait group for subservices.
	svcGroup := &sync.WaitGroup{}

	// Run packer.
	logging.Log("Starting block packer")
	packer := CreateBlockPacker(svc.inputQueue)
	packer.RegisterOnWaitGroup(svcGroup)
	go packer.Run()

	// Run writer.
	logging.Log("Starting block writer")
	writer := CreateBlockWriter(svc.blockchain, packer.BlockQueue(), packer.ResponseChannel())
	writer.RegisterOnWaitGroup(svcGroup)
	go writer.Run()

	// Begin main loop.
	svc.stopping = false

	for !svc.stopping {
		// Wait for a signal through the control channel. The only signal currently implemented
		// is the stop signal, so just block until any signal is received and then stop.
		<-svc.controlChannel
		svc.stopping = true
	}

	// Stop and wait for subservices.
	packer.Stop()
	writer.Stop()
	// Wait for subservices to finish.
	svcGroup.Wait()
	// Indicate termination if part of a wait group.
	if svc.waitGroup != nil {
		svc.waitGroup.Done()
	}

	logging.Log("Blockchain service stopping")
}

func (svc *BlockchainService) Stop() {
	logging.Log("Sending stop signal to blockchain service")
	svc.controlChannel <- 0
}

//=================================================================================================
// Read
//-------------------------------------------------------------------------------------------------

func (svc *BlockchainService) HandleGetBlock(req *message.GetBlockByHashRequest) (
	*message.GetBlockByHashResponse, error) {
	// Simply delegate the request to the blockchain middleware.
	response, err := svc.blockchain.GetOneWithHash(req)

	if err == nil {
		hash := response.Block().Hash().Hex()
		logging.Log(fmt.Sprintf("Retrieved block with hash %s", hash))
	}
	return response, err
}

func (svc *BlockchainService) HandleGetBlocksFromMinute(req *message.ReadBlocksInMinuteRequest) (
	*message.ReadBlocksInMinuteResponse, error) {
	// Simply delegate the request to the blockchain middleware.
	return svc.blockchain.GetBlocksFromMinute(req)
}

//=================================================================================================
// Write
//-------------------------------------------------------------------------------------------------

func (svc *BlockchainService) HandleWriteChunk(req *message.WriteChunk) (
	*message.WriteChunkResponse, error) {
	// Push the request through the input queue.
	return svc.inputQueue.PushRequest(req), nil
}
