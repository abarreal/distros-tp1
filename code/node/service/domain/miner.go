package domain

import (
	"fmt"
	"sync"

	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/common/number/big32"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
	"tp1.aba.distros.fi.uba.ar/interface/message"
)

//=================================================================================================
// Mining request
//-------------------------------------------------------------------------------------------------

type MiningRequest struct {
	// The block to mine.
	block *blockchain.Block
	// The channel to which the result should be written.
	responseChannel chan *blockchain.Block
}

// Given a block, create a request for the miners to mine that block. Once mined, the complete
// block with nonce and hash will be written to the given output channel. To prevent issues,
// the output channel should be non blocking.
func CreateMiningRequest(block *blockchain.Block, output chan *blockchain.Block) *MiningRequest {
	request := &MiningRequest{}
	request.block = block
	request.responseChannel = output
	return request
}

func (request *MiningRequest) ResponseChannel() <-chan *blockchain.Block {
	return request.responseChannel
}

//=================================================================================================
// Miner
//-------------------------------------------------------------------------------------------------

const MinerOpQuit int = 0
const MinerOpStopMining int = 1

const MinerStateIdle int = 0
const MinerStateMining int = 1

type Miner struct {
	id             int
	stopping       bool
	state          int
	waitGroup      *sync.WaitGroup
	controlChannel chan int
	requestChannel chan *MiningRequest
	currentRequest *MiningRequest
	// Keep statistics of the amount of mined blocks.
	miningSuccessCount   int
	miningFailureCount   int
	miningStatisticsLock *sync.RWMutex
}

func CreateMiner(id int) *Miner {
	miner := &Miner{}
	miner.id = id
	miner.state = MinerStateIdle
	miner.stopping = false
	miner.controlChannel = make(chan int)
	miner.requestChannel = make(chan *MiningRequest)
	miner.currentRequest = nil
	miner.miningSuccessCount = 0
	miner.miningFailureCount = 0
	miner.miningStatisticsLock = &sync.RWMutex{}
	return miner
}

func (miner *Miner) RegisterOnWaitGroup(waitGroup *sync.WaitGroup) {
	miner.waitGroup = waitGroup
	miner.waitGroup.Add(1)
}

func (miner *Miner) Run() {
	// Begin main loop.
	for !miner.stopping {
		miner.loop()
	}
	// Begin finalization procedures.
	logging.Log(fmt.Sprintf("Miner %d now stopping", miner.id))
	if miner.waitGroup != nil {
		miner.waitGroup.Done()
	}
}

func (miner *Miner) StartMining(request *MiningRequest) {
	miner.requestChannel <- request
}

func (miner *Miner) StopMining() {
	miner.controlChannel <- MinerOpStopMining
}

func (miner *Miner) Stop() {
	logging.Log(fmt.Sprintf("Sending quit signal to miner %d", miner.id))
	miner.controlChannel <- MinerOpQuit
}

func (miner *Miner) MiningStats() *message.MiningStats {
	miner.miningStatisticsLock.RLock()
	defer miner.miningStatisticsLock.RUnlock()
	stats := &message.MiningStats{}
	stats.MinerId = miner.id
	stats.MiningSuccessCount = miner.miningSuccessCount
	stats.MiningFailureCount = miner.miningFailureCount
	return stats
}

func (miner *Miner) loop() {
	// Act depending on miner state.
	switch miner.state {
	case MinerStateIdle:
		miner.awaitMiningRequest()
	case MinerStateMining:
		miner.mine()
	}
}

func (miner *Miner) awaitMiningRequest() {
	logging.Log(fmt.Sprintf("Miner %d waiting for mining request", miner.id))

	select {
	case request := <-miner.requestChannel:
		miner.handleMiningRequest(request)
	case signal := <-miner.controlChannel:
		miner.handleSignal(signal)
	}
}

func (miner *Miner) handleMiningRequest(request *MiningRequest) {
	logging.Log(fmt.Sprintf("Miner %d received a mining request", miner.id))
	// Create a mutable copy of the block.
	block := blockchain.CreateBlockFromBuffer(
		big32.Zero,
		request.block.Buffer(),
		request.block.DataLength())
	// Create a copy of the request, with the mutable copy of the block.
	// Set request for mining and transition to the mining state.
	miner.currentRequest = CreateMiningRequest(block, request.responseChannel)
	miner.state = MinerStateMining
}

func (miner *Miner) handleSignal(signal int) {
	switch signal {
	case MinerOpQuit:
		miner.stopping = true
	case MinerOpStopMining:
		miner.currentRequest = nil
		miner.state = MinerStateIdle
	}
}

func (miner *Miner) mine() {
	// Check if there are signals to be handled.
	select {
	case signal := <-miner.controlChannel:
		miner.handleSignal(signal)
		return
	default:
		// There are no signals to be handled. Continue with the code
		// that follows.
	}
	// Get the current block and update values to generate a new hash.
	currentBlock := miner.currentRequest.block
	// Determine whether the current hash value is less than the computed value.
	if currentBlock.AttemptHash() {
		// The hash is less than the maximum value, so we take this as a valid block.
		// Send the block with the nonce through the response channel.
		logging.Log(fmt.Sprintf("Miner %d found a valid block", miner.id))
		miner.currentRequest.responseChannel <- miner.currentRequest.block
		// Increase the count of successfully mined blocks.
		miner.miningStatisticsLock.Lock()
		miner.miningSuccessCount++
		miner.miningStatisticsLock.Unlock()
		// Move back to the idle state.
		miner.currentRequest = nil
		miner.state = MinerStateIdle
	} else {
		// Increase the count of mining failures.
		miner.miningStatisticsLock.Lock()
		miner.miningFailureCount++
		miner.miningStatisticsLock.Unlock()
	}
}
