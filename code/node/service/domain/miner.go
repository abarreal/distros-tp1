package domain

import (
	"math/big"
	"sync"

	"tp1.aba.distros.fi.uba.ar/common/number/big32"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
)

//=================================================================================================
// Mining request
//-------------------------------------------------------------------------------------------------

type MiningRequest struct {
	// The block to mine.
	block *blockchain.Block
	// The channel to which the result should be written.
	responseChannel chan<- *blockchain.Block
}

// Given a block, create a request for the miners to mine that block. Once mined, the complete
// block with nonce and hash will be written to the given output channel. To prevent issues,
// the output channel should be non blocking.
func CreateMiningRequest(block *blockchain.Block, output chan<- *blockchain.Block) *MiningRequest {
	request := &MiningRequest{}
	request.block = block
	request.responseChannel = output
	return request
}

//=================================================================================================
// Miner
//-------------------------------------------------------------------------------------------------

// Define control signal constants for the miner.
const MinerOpQuit int = 0
const MinerOpStartMining int = 1
const MinerOpStopMining int = 2

type Miner struct {
	// The wait group for the miner to join when starting.
	waitGroup *sync.WaitGroup
	// The request currently being handled by the miner.
	currentRequest *MiningRequest
	// The channel through which the miner will receive control signals.
	controlChannel chan int
	// The channel through which the miner will receive mining requests.
	requestChannel chan *MiningRequest
	// A flag that indicates whether the miner should be stopping or not.
	stopping bool
}

func CreateMiner(wg *sync.WaitGroup) *Miner {
	miner := &Miner{}
	miner.waitGroup = wg
	miner.currentRequest = nil
	miner.stopping = false
	return miner
}

func (miner *Miner) Run() {
	miner.waitGroup.Add(1)

	for !miner.stopping {
		miner.loop()
	}

	miner.waitGroup.Done()
}

func (miner *Miner) loop() {
	// If there is a non null mining request, then the miner is currently mining.
	mining := miner.currentRequest == nil

	if mining {
		// We have some mining task, currently. Before proceeding, we check if there
		// is some control signal we should care about.
		select {
		case signal := <-miner.controlChannel:
			// Handle the signal and continue in the following cycle.
			miner.handle(signal)
			return
		default:
		}
		// There were no signals to be handled, so we perform the mining work.
		currentBlock := miner.currentRequest.block
		currentBlock.GenerateNonce()
		currentBlock.UpdateTimestamp()
		hash := currentBlock.Hash()
		difficulty := currentBlock.Difficulty()
		// Compute the hash and evaluate whether it meets the difficulty requirements.
		// Compute the expected maximum hash value first.
		numerator := new(big.Int).Exp(big.NewInt(2), big.NewInt(256), nil)
		max := new(big.Int).Div(numerator, difficulty.ToBig())
		// Determine whether the current hash value is less than the computed value.
		if max.Cmp(hash.ToBig()) > 0 {
			// The hash is less than the maximum value, so we take this as a valid block.
			// Send the block with the nonce through the response channel.
			miner.currentRequest.responseChannel <- miner.currentRequest.block
			miner.currentRequest = nil
		}
	} else {
		// We are not currently mining. We proceed then to lock and wait for
		// a new control signal.
		signal := <-miner.controlChannel
		miner.handle(signal)
	}
}

func (miner *Miner) handle(signal int) {
	switch signal {
	case MinerOpQuit:
		miner.stopping = true
	case MinerOpStopMining:
		// Set current mining request to nil to indicate that there is nothing to mine.
		miner.currentRequest = nil
	case MinerOpStartMining:
		// We were asked to start mining. Read the mining request from the channel.
		request := <-miner.requestChannel
		// Replace the current block with a mutable copy.
		request.block = blockchain.CreateBlockFromBuffer(
			big32.Zero,
			request.block.Buffer(),
			request.block.DataLength())
		// Set the request on the miner to begin mining in the following loop.
		miner.currentRequest = request
	}
}

func (miner *Miner) StartMining(request *MiningRequest) {
	miner.StopMining()
	miner.requestChannel <- request
	miner.controlChannel <- MinerOpStartMining
}

func (miner *Miner) StopMining() {
	miner.controlChannel <- MinerOpStopMining
}

func (miner *Miner) Quit() {
	miner.controlChannel <- MinerOpQuit
}
