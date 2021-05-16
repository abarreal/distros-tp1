package domain

import (
	"fmt"
	"sync"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
	"tp1.aba.distros.fi.uba.ar/interface/message"
	"tp1.aba.distros.fi.uba.ar/node/service/middleware"
)

const BlockWriterOpQuit int = 0

const BlockWriterStateBooting = 0
const BlockWriterStateWaitingForBlock = 1
const BlockWriterStateWaitingForMiners = 2

type BlockWriter struct {
	stopping bool
	state    int

	// The blockchain middleware to delegate write requests.
	blockchain *middleware.Blockchain
	// A queue through which the writer receives blocks for writing.
	inputQueue <-chan *blockchain.Block
	// A queue through which the writer will send writer responses.
	responseQueue chan<- *message.WriteBlockResponse
	// A channel used to tell the writer to stop.
	quitChannel chan int
	// A wait group for the writer to register to.
	waitGroup *sync.WaitGroup
	// A wait group for the miners to register to.
	minerWaitGroup *sync.WaitGroup
	// The currently outstanding mining request.
	currentMiningRequest *MiningRequest
	// The collection of miners under this writer.
	miners []*Miner
}

func CreateBlockWriter(
	blockchain *middleware.Blockchain,
	inputQueue <-chan *blockchain.Block,
	responseQueue chan<- *message.WriteBlockResponse) *BlockWriter {

	writer := &BlockWriter{}
	writer.blockchain = blockchain
	writer.inputQueue = inputQueue
	writer.responseQueue = responseQueue
	writer.state = BlockWriterStateBooting
	writer.currentMiningRequest = nil
	writer.stopping = false
	writer.quitChannel = make(chan int)

	// Create miners.
	minerCount, _ := config.GetIntOrDefault("MinerCount", 4)
	writer.miners = make([]*Miner, minerCount)
	writer.minerWaitGroup = &sync.WaitGroup{}

	for i := 0; i < len(writer.miners); i++ {
		writer.miners[i] = CreateMiner(i)
		writer.miners[i].RegisterOnWaitGroup(writer.minerWaitGroup)
	}

	return writer
}

func (writer *BlockWriter) RegisterOnWaitGroup(waitGroup *sync.WaitGroup) {
	writer.waitGroup = waitGroup
	writer.waitGroup.Add(1)
}

func (writer *BlockWriter) Stop() {
	logging.Log("Sending stop signal to the block writer")
	writer.quitChannel <- 1
}

func (writer *BlockWriter) Run() {
	// Run miners.
	for i := 0; i < len(writer.miners); i++ {
		go writer.miners[i].Run()
	}

	// Initiate main loop.
	for !writer.stopping {
		writer.loop()
	}

	logging.Log("Block writer now stopping")

	// Stop miners.
	for i := 0; i < len(writer.miners); i++ {
		logging.Log(fmt.Sprintf("Sending stop request to miner %d", i))
		writer.miners[i].Stop()
	}
	// Wait for miners to finish.
	logging.Log("Waiting for miners to finish")
	writer.minerWaitGroup.Wait()
	// Send notification of writer termination.
	if writer.waitGroup != nil {
		writer.waitGroup.Done()
	}
}

func (wr *BlockWriter) loop() {
	// Proceed depending on current state.
	switch wr.state {
	case BlockWriterStateBooting:
		wr.boot()
	case BlockWriterStateWaitingForBlock:
		wr.awaitBlock()
	case BlockWriterStateWaitingForMiners:
		wr.awaitMiners()
	}
}

func (wr *BlockWriter) boot() {
	// Send a message through the response queue to notify about the writer being ready to
	// handle incoming blocks.
	h := wr.blockchain.CurrentPreviousHash()
	d := wr.blockchain.CurrentDifficulty()
	wr.responseQueue <- message.CreateWriteBlockResponse(true, h, d)
	wr.state = BlockWriterStateWaitingForBlock
}

func (wr *BlockWriter) awaitBlock() {
	logging.Log("Block writer now waiting for a new block")
	select {
	case block := <-wr.inputQueue:
		wr.handleIncomingBlock(block)
	case <-wr.quitChannel:
		wr.finalize()
	}
}

func (wr *BlockWriter) handleIncomingBlock(block *blockchain.Block) {
	logging.Log("Block writer now handling an incoming block")
	// Create a channel for the miners to answer through.
	channel := make(chan *blockchain.Block, len(wr.miners))
	// Create a mining request and send to each miner for mining.
	wr.currentMiningRequest = CreateMiningRequest(block, channel)
	// Send the request to the miners.
	logging.Log("Pushing mining request to the miners")
	for _, miner := range wr.miners {
		miner.StartMining(wr.currentMiningRequest)
	}
	// Change writer state.
	wr.state = BlockWriterStateWaitingForMiners
}

func (wr *BlockWriter) awaitMiners() {
	logging.Log("Block writer now waiting for the miners to finish mining the current block")
	select {
	case block := <-wr.currentMiningRequest.ResponseChannel():
		wr.handleMiningResponse(block)
	case <-wr.quitChannel:
		wr.finalize()
	}
}

func (wr *BlockWriter) handleMiningResponse(block *blockchain.Block) {
	logging.Log("Block writer now handling a response from the miners")
	// Send the mined block to the blockchain server. Create a write request first.
	blockRequest := message.CreateWriteBlock(block)
	// Send the request to the server.
	if blockResponse, err := wr.blockchain.WriteBlock(blockRequest); err != nil {
		logging.LogError("Write request failed", err)
	} else {
		// Notify all remaining miners that mining for the current block is done and they should stop.
		for _, miner := range wr.miners {
			miner.StopMining()
		}
		// Send the response back upstream to notify results and change state.
		wr.responseQueue <- blockResponse
		wr.state = BlockWriterStateWaitingForBlock
	}
}

func (wr *BlockWriter) finalize() {
	logging.Log("Block writer received stop signal")
	wr.stopping = true
}
