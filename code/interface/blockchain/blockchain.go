package blockchain

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"time"

	b32 "tp1.aba.distros.fi.uba.ar/common/number/big32"
	"tp1.aba.distros.fi.uba.ar/interface/communication"
)

//=================================================================================================
// Blockchain Interface
//-------------------------------------------------------------------------------------------------

type IBlockchainWrite interface {
	WriteBlock(block *Block) error
}

type IBlockchainRead interface {
	GetOneWithHash(*b32.Big32) (*Block, error)
	GetBlocksFromMinute(timestamp time.Time) ([]*Block, error)
}

//=================================================================================================
// Init
//-------------------------------------------------------------------------------------------------

func init() {
}

//=================================================================================================
// Blocks
//-------------------------------------------------------------------------------------------------

// Define the length of each header element in bytes.
var headerLength map[string]uint32 = map[string]uint32{
	"PreviousHash": 32,
	"Nonce":        32,
	"Timestamp":    8,
	"EntryCount":   1,
	"Difficulty":   32,
}

// Define a map to keep track of the offset of each field to access in the buffer.
var headerOffset map[string]uint32 = map[string]uint32{
	"PreviousHash": 0,
	"Nonce":        32,
	"Timestamp":    64,
	"EntryCount":   72,
	"Difficulty":   73,
	"Data":         105,
}

type Block struct {
	// A 32 byte buffer holding the hash of the block, if computed.
	hash [32]byte
	// The buffer holding the actual block data. Does not include metadata, just
	// the header and a sequence of entry length, entry data records.
	buffer []byte
	// A boolean that tells whether the buffer was modified, which means that the hash needs
	// to be recomputed.
	bufferDirty bool
}

type Chunk struct {
	// Pointer to the next chunk in the linked list.
	next *Chunk
	// The length of the data.
	Length uint16
	// The data itself.
	Data []byte
}

func CreateChunk(data []byte) *Chunk {
	chunk := &Chunk{}
	chunk.Length = uint16(len(data))
	chunk.Data = make([]byte, chunk.Length)
	copy(chunk.Data, data)
	return chunk
}

func (chunk *Chunk) SetNext(next *Chunk) {
	chunk.next = next
}

func CreateBlockFromBuffer(hash *b32.Big32, buffer []byte, bufferLength uint32) *Block {
	block := &Block{}
	block.buffer = make([]byte, bufferLength)
	block.bufferDirty = false
	copy(block.hash[:], hash.Bytes[:])
	copy(block.buffer, buffer[:bufferLength])
	return block
}

func CreateBlock(previousHash *b32.Big32, difficulty *b32.Big32, entries *Chunk) (*Block, error) {
	// Ensure that all pointers are non null.
	if previousHash == nil {
		return nil, errors.New("previousHash cannot be nil")
	}
	if difficulty == nil {
		return nil, errors.New("difficulty cannot be nil")
	}
	if entries == nil {
		return nil, errors.New("entries cannot be nil")
	}

	// Iterate through entries and compute the total amount of space needed to hold
	// the whole of the data. Count the number of entries as well.
	var total uint32 = 0
	var count uint8 = 0

	for current := entries; current != nil; current = current.next {
		count++
		total += uint32(current.Length)
		total += 2 // Add to bytes per entry to store its length.
	}

	// Add the length of the header into the total.
	for _, value := range headerLength {
		total += value
	}

	// Create a Block object and proceed to initialize data elements.
	block := &Block{}
	block.bufferDirty = true

	// Instantiate a buffer of the appropriate size to hold both header and data.
	block.buffer = make([]byte, total)

	// Initialize header data.
	block.setPreviousHash(previousHash)
	block.setDifficulty(difficulty)
	block.setTimestamp(time.Now().UTC().Unix())
	block.setEntryCount(count)

	// Copy entries into the buffer.
	currentOffset := headerOffset["Data"]

	for current := entries; current != nil; current = current.next {
		// Copy the length of the chunk into the buffer.
		binary.LittleEndian.PutUint16(block.buffer[currentOffset:currentOffset+2], current.Length)
		currentOffset += 2
		// Get current length as a 32 bit integer to avoid casting later.
		currentLength := uint32(current.Length)
		// Copy the data from the entry object buffer into the block buffer.
		copy(block.buffer[currentOffset:currentOffset+currentLength], current.Data[:currentLength])
		currentOffset += currentLength
	}
	// Return a pointer to the block.
	return block, nil
}

func (block *Block) LenghtWithMetadata() uint32 {
	// Get the length of the block with metadata, including four bytes for the length
	// of the buffer, 32 bytes for the hash, and the length of the buffer itself.
	return uint32(4 + 32 + len(block.buffer))
}

func (block *Block) DataLength() uint32 {
	return uint32(len(block.buffer))
}

func (block *Block) Buffer() []byte {
	return block.buffer
}

func (block *Block) BufferWithMetadata() []byte {
	// Instantiate a buffer object.
	buffer := bytes.NewBuffer(make([]byte, 0, block.LenghtWithMetadata()))
	// Write the block to the buffer and return.
	block.WriteWithMetadata(buffer)
	return buffer.Bytes()
}

func (block *Block) Hash() *b32.Big32 {
	if block.bufferDirty {
		hash := sha256.Sum256(block.buffer)
		copy(block.hash[:], hash[:])
		block.bufferDirty = false
	}
	return b32.FromBytes(&block.hash)
}

func (block *Block) PreviousHash() *b32.Big32 {
	return block.getBig32("PreviousHash")
}

func (block *Block) setPreviousHash(hash *b32.Big32) {
	block.setBig32("PreviousHash", hash)
}

func (block *Block) Difficulty() *b32.Big32 {
	return block.getBig32("Difficulty")
}

func (block *Block) setDifficulty(difficulty *b32.Big32) {
	block.setBig32("Difficulty", difficulty)
}

func (block *Block) Nonce() *b32.Big32 {
	return block.getBig32("Nonce")
}

func (block *Block) GenerateNonce() {
	offset, length := getFieldPositionInfo("Nonce")
	rand.Read(block.buffer[offset : offset+length])
	block.bufferDirty = true
}

func (block *Block) EntryCount() uint8 {
	return block.buffer[headerOffset["EntryCount"]]
}

func (block *Block) setEntryCount(count uint8) {
	block.buffer[headerOffset["EntryCount"]] = count
	block.bufferDirty = true
}

func (block *Block) Timestamp() int64 {
	offset, length := getFieldPositionInfo("Timestamp")
	return int64(binary.LittleEndian.Uint64(block.buffer[offset : offset+length]))
}

func (block *Block) SetCreationTime(time time.Time) {
	block.setTimestamp(time.UTC().Unix())
}

func (block *Block) setTimestamp(timestamp int64) {
	offset, length := getFieldPositionInfo("Timestamp")
	binary.LittleEndian.PutUint64(block.buffer[offset:offset+length], uint64(timestamp))
	block.bufferDirty = true
}

// Reads a block from the given reader, assuming that it was previously written through
// the WriteWithMetadata method.
func ReadBlock(reader io.Reader) (*Block, error) {
	// Read the length of the buffer.
	blocklenBuffer := make([]byte, 4)
	if err := communication.Read(reader, blocklenBuffer); err != nil {
		return nil, err
	}
	blocklen := binary.LittleEndian.Uint32(blocklenBuffer)
	// Read the hash of the block.
	hash := make([]byte, 32)
	if err := communication.Read(reader, hash); err != nil {
		return nil, err
	}
	// Read the actual block buffer.
	blockBuffer := make([]byte, blocklen)
	var err error = communication.Read(reader, blockBuffer)
	// Check if there is an error and is not an EOF.
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	// Instantiate a block from the buffer and return. Return the error as well, which
	// if EOF will indicate the end of the file.
	block := CreateBlockFromBuffer(b32.FromSlice(hash), blockBuffer, blocklen)

	/*
		fmt.Println("DEBUG: RECEIVED BLOCK")
		fmt.Println(block.buffer)
		fmt.Println("Hash")
		fmt.Println(block.Hash().Bytes)
		fmt.Println("Previous Hash")
		fmt.Println(block.PreviousHash().Bytes)
		fmt.Println("Nonce")
		fmt.Println(block.Nonce().Bytes)
		fmt.Println("Timestamp")
		fmt.Println(block.Timestamp())
		fmt.Println("Entry Count")
		fmt.Println(block.EntryCount())
		fmt.Println("Difficulty")
		fmt.Println(block.Difficulty().Bytes)
	*/

	return block, err
}

func (block *Block) WriteWithMetadata(writer io.Writer) error {
	// Write the block length to ease navigating the file.
	blocklen := make([]byte, 4)
	binary.LittleEndian.PutUint32(blocklen, uint32(len(block.buffer)))
	if err := writeAll(blocklen, 4, writer); err != nil {
		return err
	}
	// Ensure that the hash has been computed.
	block.Hash()
	// Write the hash of the block.
	if err := writeAll(block.hash[:], uint64(len(block.hash)), writer); err != nil {
		return err
	}
	// Write the actual content of the block, including header and chunks.
	if err := block.Write(writer); err != nil {
		return err
	}
	// Return no error.
	return nil
}

func (block *Block) Write(writer io.Writer) error {

	/*
		fmt.Println("DEBUG: WRITING BLOCK")
		fmt.Println(block.buffer)
		fmt.Println("Hash")
		fmt.Println(block.Hash().Bytes)
		fmt.Println("Previous Hash")
		fmt.Println(block.PreviousHash().Bytes)
		fmt.Println("Nonce")
		fmt.Println(block.Nonce().Bytes)
		fmt.Println("Timestamp")
		fmt.Println(block.Timestamp())
		fmt.Println("Entry Count")
		fmt.Println(block.EntryCount())
		fmt.Println("Difficulty")
		fmt.Println(block.Difficulty().Bytes)
	*/

	return writeAll(block.buffer, uint64(len(block.buffer)), writer)
}

func writeAll(data []byte, length uint64, writer io.Writer) error {
	return communication.Write(data, length, writer)
}

func (block *Block) getBig32(name string) *b32.Big32 {
	offset, length := getFieldPositionInfo(name)
	return b32.FromSlice(block.buffer[offset : offset+length])
}

func (block *Block) setBig32(field string, n *b32.Big32) {
	offset, length := getFieldPositionInfo(field)
	copy(block.buffer[offset:offset+length], n.Bytes[:])
	block.bufferDirty = true
}

func getFieldPositionInfo(name string) (uint32, uint32) {
	return headerOffset[name], headerLength[name]
}

//=================================================================================================
// Block Chunk Iterator
//-------------------------------------------------------------------------------------------------

type ChunkIterator struct {
	block         *Block
	entryCount    uint8
	currentOffset uint32
	currentIndex  uint8
}

func (block *Block) Entries() *ChunkIterator {
	return &ChunkIterator{block, block.EntryCount(), headerOffset["Data"], 0}
}

func (it *ChunkIterator) Count() uint8 {
	return it.entryCount
}

func (it *ChunkIterator) HasNext() bool {
	return it.currentIndex < it.entryCount
}

func (it *ChunkIterator) Advance() {
	// Only advance if we are not at the end of the list.
	if it.HasNext() {
		// Advance the current index past the chunk's data and length header.
		it.currentOffset += uint32(it.ChunkLength()) + 2
		it.currentIndex++
	}
}

func (it *ChunkIterator) Chunk() *Chunk {
	// Get the length of the data.
	dataLength := it.ChunkLength()
	// Get the offset from where the chunk data begins.
	dataStart := it.currentOffset + 2
	// Construct a chunk from the data.
	chunk := &Chunk{}
	chunk.Length = dataLength
	chunk.Data = it.block.buffer[dataStart : dataStart+uint32(dataLength)]
	return chunk
}

func (it *ChunkIterator) ChunkLength() uint16 {
	offset := it.currentOffset
	return binary.LittleEndian.Uint16(it.block.buffer[offset : offset+2])
}

//=================================================================================================
// Test Data
//-------------------------------------------------------------------------------------------------

func CreateDummyBlock() *Block {
	return CreateDummyBlockWithKnownData(get32(), get32())
}

func CreateDummyBlockWithKnownData(previousHash *b32.Big32, difficulty *b32.Big32) *Block {
	block, _ := CreateBlock(previousHash, difficulty, createTestEntries())
	return block
}

func get32() *b32.Big32 {
	buff := make([]byte, 32)
	rand.Read(buff)
	return b32.FromSlice(buff)
}

func createTestChunk(data string) *Chunk {
	chunkData := []byte(data)
	chunk := CreateChunk(chunkData)
	return chunk
}

func createTestEntries() *Chunk {
	head := createTestChunk("Hello")
	tail := createTestChunk("World")
	head.SetNext(tail)
	return head
}
