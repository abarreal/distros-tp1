package domain

import (
	"sync"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
	"tp1.aba.distros.fi.uba.ar/interface/message"
)

//=================================================================================================
// Chunk Queue
//-------------------------------------------------------------------------------------------------

type ChunkQueue struct {
	head     *blockchain.Chunk
	tail     *blockchain.Chunk
	lock     *sync.Mutex
	count    int
	capacity int
}

func CreateChunkQueue() *ChunkQueue {
	queue := &ChunkQueue{}
	queue.head = nil
	queue.tail = nil
	queue.lock = &sync.Mutex{}
	queue.count = 0
	queue.capacity, _ = config.GetIntOrDefault("InputChunkQueueCapacity", 8)
	return queue
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

// A signal used to tell the block packer that a new block is available in the queue.
const BlockPackerOpChunkAvailable = 1

// A signal used to tell the packer that the services downstream are ready to accept another block.
const BlockPackerOpDownstreamReady = 2

type BlockPacker struct {
	inputQueue *ChunkQueue
}

func CreateBlockPacker(inputQueue *ChunkQueue) *BlockPacker {
	packer := &BlockPacker{}
	packer.inputQueue = inputQueue
	return packer
}

func (packer *BlockPacker) Run() {
	// TODO
}
