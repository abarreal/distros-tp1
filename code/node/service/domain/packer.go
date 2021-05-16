package domain

import (
	"fmt"
	"sync"
	"time"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/common/number/big32"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
	"tp1.aba.distros.fi.uba.ar/interface/message"
)

//=================================================================================================
// Chunk Queue
//-------------------------------------------------------------------------------------------------

type ChunkQueue struct {
	head          *blockchain.Chunk
	tail          *blockchain.Chunk
	lock          *sync.Mutex
	count         int
	capacity      int
	notifications chan int
}

func CreateChunkQueue() *ChunkQueue {
	queue := &ChunkQueue{}
	queue.head = nil
	queue.tail = nil
	queue.lock = &sync.Mutex{}
	queue.count = 0
	queue.capacity, _ = config.GetIntOrDefault("InputChunkQueueCapacity", 8)
	// Create a notifications queue that can hold at least as many elements as
	// the queue for non blocking write behaviour.
	queue.notifications = make(chan int, queue.capacity+1)
	return queue
}

func (q *ChunkQueue) NotificationsChannel() <-chan int {
	return q.notifications
}

func (q *ChunkQueue) PushRequest(request *message.WriteChunk) *message.WriteChunkResponse {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.isFull() {
		// The queue is full. Reject the message.
		return message.CreateWriteChunkResponse(false)
	}

	// Save the chunk to the queue.
	chunk := blockchain.CreateChunk(request.ChunkData())

	if q.count == 0 {
		// Set the chunk as the head and tail of the linked list.
		q.head = chunk
		q.tail = chunk
	} else {
		// Add the chunk to the linked list.
		q.tail.SetNext(chunk)
		q.tail = chunk
	}

	q.count++
	q.notifications <- 1
	return message.CreateWriteChunkResponse(true)
}

func (q *ChunkQueue) Count() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.count
}

func (q *ChunkQueue) PopChunks() *blockchain.Chunk {
	q.lock.Lock()
	defer q.lock.Unlock()
	// Get the list of all chunks in the queue.
	chunks := q.head
	// Leave the queue as empty.
	q.head = nil
	q.tail = nil
	q.count = 0
	return chunks
}

func (q *ChunkQueue) isFull() bool {
	return q.count == q.capacity
}

//=================================================================================================
// Packer
//-------------------------------------------------------------------------------------------------

// A signal used to tell the block packer to stop.
const BlockPackerOpQuit = 0

type BlockPacker struct {
	controlChannel      chan int
	inputQueue          *ChunkQueue
	waitGroup           *sync.WaitGroup
	stopping            bool
	isDownstreamReady   bool
	timer               *time.Ticker
	timerDeltaSeconds   int
	chunkThreshold      int
	currentDifficulty   *big32.Big32
	currentPreviousHash *big32.Big32
	blockChannel        chan *blockchain.Block
	updateChannel       chan *message.WriteBlockResponse
}

func CreateBlockPacker(inputQueue *ChunkQueue) *BlockPacker {
	packer := &BlockPacker{}
	packer.inputQueue = inputQueue
	packer.controlChannel = make(chan int, 1)
	packer.stopping = false
	packer.isDownstreamReady = false
	// Create a channel for the packer to send blocks downstream for processing.
	packer.blockChannel = make(chan *blockchain.Block, 1)
	// Create a channel for upstream services to notify the packer on new block writes, so that
	// the packer can use the latest information to create new blocks.
	packer.updateChannel = make(chan *message.WriteBlockResponse, 1)
	return packer
}

func (packer *BlockPacker) RegisterOnWaitGroup(wg *sync.WaitGroup) {
	packer.waitGroup = wg
	packer.waitGroup.Add(1)
}

func (packer *BlockPacker) ResponseChannel() chan<- *message.WriteBlockResponse {
	return packer.updateChannel
}

func (packer *BlockPacker) BlockQueue() <-chan *blockchain.Block {
	return packer.blockChannel
}

func (packer *BlockPacker) Stop() {
	logging.Log("Sending stop signal to the block packer")
	packer.controlChannel <- BlockPackerOpQuit
}

func (packer *BlockPacker) Run() {
	logging.Log("The block packer is starting")

	// Get the chunk threshold from configuration.
	packer.chunkThreshold, _ = config.GetIntOrDefault("PackerChunkThreshold", 5)
	// Get the periodic wake up duration from configuration.
	packer.timerDeltaSeconds, _ = config.GetIntOrDefault("PackerInterruptionInterval", 30)
	// Initiate a timer that periodically sends an interrupt signal.
	packer.timer = time.NewTicker(time.Duration(packer.timerDeltaSeconds) * time.Second)

	// Begin main loop.
	for !packer.stopping {
		packer.loop()
	}

	logging.Log("The block packer is stopping")

	// Finalize the timer.
	packer.timer.Stop()
	// Indicate termination if part of a wait group.
	if packer.waitGroup != nil {
		packer.waitGroup.Done()
	}

	logging.Log("The block packer has finished executing")
}

func (packer *BlockPacker) loop() {
	// Await incoming signals.
	select {
	case signal := <-packer.controlChannel:
		packer.handle(signal)
	case <-packer.timer.C:
		packer.handleInterrupt()
	case <-packer.inputQueue.NotificationsChannel():
		// A new chunk was queued upstream. Evaluate whether we should create a new block
		// and pass it downstream for mining and writing. Only create a block if there
		// are enough chunks to make it worthwhile.
		packer.evaluateBlockCreation(false)
	case response := <-packer.updateChannel:
		// A write block response was received from downstream. Proceed to update
		// packer state according to results.
		packer.evaluateWriteBlockResponse(response)
	}
}

func (packer *BlockPacker) handle(signal int) {
	// Act depending on the signal and the current status of the system.
	switch signal {
	case BlockPackerOpQuit:
		logging.Log("Packer received stop signal")
		packer.stopping = true
	}
}

func (packer *BlockPacker) evaluateWriteBlockResponse(response *message.WriteBlockResponse) {
	// Update state needed to create new blocks.
	packer.currentPreviousHash = response.NewPreviousHash()
	packer.currentDifficulty = response.NewDifficulty()
	logging.Log(fmt.Sprintf("Packer received new previous hash: %s", packer.currentPreviousHash.Hex()))
	logging.Log(fmt.Sprintf("Packer received new difficulty: %s", packer.currentDifficulty.Hex()))
	// Register that the downstream services are ready to handle new blocks.
	packer.isDownstreamReady = true
}

func (packer *BlockPacker) handleInterrupt() {
	// A periodic interrupt was issued. Evaluate block creation, but ignore the threshold;
	// collect all chunks and create a block.
	logging.Log("Handling periodic interrupt")
	packer.evaluateBlockCreation(true)
}

func (packer *BlockPacker) evaluateBlockCreation(ignoreThreshold bool) {
	// If the downstream services are not ready, keep waiting until they are.
	if !packer.isDownstreamReady {
		if ignoreThreshold {
			logging.Log("Downstream services are not ready to handle a new block, skipping")
		}
		return
	}

	// The downstream services are ready to handle a new block. If there are enough
	// chunks to create a new block, or if the ignoreThreshold flag was set,
	// create one and pass it downstream. Otherwise just keep waiting.
	queuedCount := packer.inputQueue.Count()

	if queuedCount == 0 {
		if ignoreThreshold {
			logging.Log("There are no queued chunks to create a block")
		}
		return
	}

	if queuedCount >= packer.chunkThreshold || ignoreThreshold {
		// Get all chunks from the queue and create a block.
		logging.Log("Creating new block for mining")
		chunks := packer.inputQueue.PopChunks()
		// Construct a block from the chunks.
		block, err := blockchain.CreateBlock(
			packer.currentPreviousHash,
			packer.currentDifficulty, chunks)

		if err != nil {
			logging.LogError("Packer could not create block", err)
			return
		}

		// Push the block downstream and wait until downstream services notify
		// the packer that they are ready to handle an additional block.
		packer.blockChannel <- block
		packer.isDownstreamReady = false
	}
}
