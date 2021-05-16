package domain

import (
	"fmt"
	"sync"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
)

const BlockWriterOpQuit int = 0

type BlockWriter struct {
	// The write group for the writer to register to when starting.
	waitGroup *sync.WaitGroup
	// The amount of miners managed by this writer.
	minerCount int
	// A slice of miners managed by this writer.
	miners []*Miner
	// The channel through which blocks to be written will be sent to the writer.
	writeChannel chan *blockchain.Block
	// The channel through which control signals will be sent to the writer.
	controlChannel chan int
	// The channel through which miners will send mined blocks back to the writer.
	currentResponseChannel chan *blockchain.Block
	// A flag to keep track of whether the server is stopping.
	stopping bool
}

func CreateBlockWriter(wg *sync.WaitGroup) *BlockWriter {
	wr := &BlockWriter{}
	wr.waitGroup = wg
	wr.stopping = false

	// Instantiate miners.
	wr.minerCount, _ = config.GetIntOrDefault("MinerCount", 4)
	wr.miners = make([]*Miner, wr.minerCount)

	for i := 0; i < len(wr.miners); i++ {
		wr.miners[i] = CreateMiner(wg)
	}

	return wr
}

func (writer *BlockWriter) Run() {
	writer.waitGroup.Add(1)

	// Launch miners one by one.
	for _, miner := range writer.miners {
		go miner.Run()
	}

	for !writer.stopping {
		writer.loop()
	}

	// Stop all miners.
	for _, miner := range writer.miners {
		miner.Quit()
	}

	writer.waitGroup.Done()
}

func (writer *BlockWriter) loop() {
	// The writer can receive messages from three sources: from the source that
	// creates blocks for writing, from the miners, and through the control channel.
	select {
	case signal := <-writer.controlChannel:
		// Handle incoming control signal.
		writer.handle(signal)
	case block := <-writer.writeChannel:
		// Handle incoming block for writing.
		writer.handleBlockForMining(block)
	case block := <-writer.currentResponseChannel:
		// Handle mined block coming from miners.
		writer.handleMinedBlock(block)
	}
}

func (writer *BlockWriter) handle(signal int) {
	switch signal {
	case BlockWriterOpQuit:
		writer.stopping = true
	}
}

func (writer *BlockWriter) handleBlockForMining(block *blockchain.Block) {
	// We received a new block for mining. Create a mining request for the block.
	// Create a channel in which to receive the responses from the miners fist,
	// and make it as to hold responses from all workers so that they do not block.
	writer.currentResponseChannel = make(chan *blockchain.Block, writer.minerCount)
	request := CreateMiningRequest(block, writer.currentResponseChannel)
	// Send the request to each miner for them to start mining the block.
	for _, miner := range writer.miners {
		miner.StartMining(request)
	}
}

func (writer *BlockWriter) handleMinedBlock(block *blockchain.Block) {
	// We received a mined block from the miners.
	// Send the write request to the blockchain.
	// TODO
	fmt.Printf("DEBUG: HANDLING MINED BLOCK WITH HASH %s\n", block.Hash().Hex())

	// Stop the remaining miners from mining the block, until we send
	// them a new request.
	for _, miner := range writer.miners {
		miner.StopMining()
	}
}

func (writer *BlockWriter) Quit() {
	writer.controlChannel <- BlockWriterOpQuit
}
