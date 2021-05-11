package message

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	number "tp1.aba.distros.fi.uba.ar/common/number/big32"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
	"tp1.aba.distros.fi.uba.ar/interface/communication"
)

type handler = func(opcode uint8, reader io.Reader) (Message, error)

// Define some constants post facto to export. These were added later
// after creating the map.
const OpGetMiningInfo uint8 = 0x00
const OpGetBlockWithHash uint8 = 0x02
const OpGetBlocksInMinute uint8 = 0x04
const OpWriteBlock uint8 = 0x06
const OpWriteChunk uint8 = 0x08

var opcodes map[string]uint8 = map[string]uint8{
	"GetMiningInfo":              OpGetMiningInfo,
	"GetMiningInfoResponse":      0x01,
	"GetBlockByHash":             OpGetBlockWithHash,
	"GetBlockByHashResponse":     0x03,
	"ReadBlocksInMinute":         OpGetBlocksInMinute,
	"ReadBlocksInMinuteResponse": 0x05,
	"WriteBlock":                 OpWriteBlock,
	"WriteBlockResponse":         0x07,
	"WriteChunk":                 OpWriteChunk,
	"WriteChunkResponse":         0x09,
}

var handlers map[uint8]handler = map[uint8]handler{
	opcodes["GetMiningInfo"]:              handleGetMiningInfo,
	opcodes["GetMiningInfoResponse"]:      handleGetMiningInfoResponse,
	opcodes["GetBlockByHash"]:             handleGetBlockByHash,
	opcodes["GetBlockByHashResponse"]:     handleGetBlockByHashResponse,
	opcodes["ReadBlocksInMinute"]:         handleReadBlocksInMinute,
	opcodes["ReadBlocksInMinuteResponse"]: handleReadBlocksInMinuteResponse,
	opcodes["WriteBlock"]:                 handleWriteBlock,
	opcodes["WriteBlockResponse"]:         handleWriteBlockResponse,
	opcodes["WriteChunk"]:                 handleWriteChunk,
	opcodes["WriteChunkResponse"]:         handleWriteChunkResponse,
}

//=================================================================================================
// Messages
//-------------------------------------------------------------------------------------------------

type Message interface {
	// The opcode of the message.
	Opcode() uint8
	// The length of everything that comes after the opcode.
	DataLength() uint64
	// The data of the message.
	Data() []byte
	// A Write method.
	Write(writer io.Writer) error
}

type message struct {
	opcode  uint8
	datalen uint64
	data    []byte
}

func (m *message) Opcode() uint8 {
	return m.opcode
}

func (m *message) DataLength() uint64 {
	return m.datalen
}

func (m *message) Data() []byte {
	return m.data
}

func (m *message) Write(writer io.Writer) error {
	var total int = 0
	var current int = 0
	var err error = nil
	// Write the single byte opcode.
	for total < 1 {
		current, err = writer.Write([]byte{m.opcode})
		if err != nil {
			return err
		}
		total += current
	}
	// Exit now if there is no data to write.
	if m.datalen == 0 {
		return nil
	}
	// Reset the total count and write the data.
	for total = 0; uint64(total) < m.datalen; {
		if current, err = writer.Write(m.data[total:]); err != nil {
			return err
		} else {
			total += current
		}
	}
	// Return no error.
	return nil
}

//=================================================================================================
// Get mining info message
//-------------------------------------------------------------------------------------------------

// Opcode : 1 byte
type GetMiningInfo struct {
	message
}

func CreateGetMiningInfoRequest() Message {
	request := &GetMiningInfo{}
	request.opcode = opcodes["GetMiningInfoRequest"]
	request.datalen = 0
	request.data = nil
	return request
}

func handleGetMiningInfo(opcode uint8, reader io.Reader) (Message, error) {
	// The GetMiningInfo request message is just a single byte, so there is no data.
	request := &GetMiningInfo{}
	request.opcode = opcode
	return request, nil
}

// Opcode        :  1 byte
// Previous hash : 32 bytes
// Difficulty    : 32 bytes
type GetMiningInfoResponse struct {
	message
}

func CreateGetMiningInfoResponse(previousHash *number.Big32, difficulty *number.Big32) *GetMiningInfoResponse {
	// Construct the data buffer.
	data := make([]byte, 64)
	copy(data[0:32], previousHash.Bytes[:])
	copy(data[32:64], difficulty.Bytes[:])
	// Construct and return the response.
	response := &GetMiningInfoResponse{}
	response.opcode = opcodes["GetMiningInfoResponse"]
	response.datalen = uint64(len(data))
	response.data = data
	return response
}

func handleGetMiningInfoResponse(opcode uint8, reader io.Reader) (Message, error) {
	// Read 64 bytes from the reader (hash and difficulty).
	msg, err := readCount(opcode, reader, 64)
	if err != nil {
		return nil, err
	}
	// Instantiate the response.
	response := &GetMiningInfoResponse{*msg}
	return response, nil
}

func (m *GetMiningInfoResponse) PreviousHash() *number.Big32 {
	// Extract the hash from the data buffer.
	return number.FromSlice(m.data[0:32])
}

func (m *GetMiningInfoResponse) Difficulty() *number.Big32 {
	// Extract the difficulty from the data buffer.
	data := m.data[32:64]
	return number.FromSlice(data)
}

//=================================================================================================
// Get block by hash
//-------------------------------------------------------------------------------------------------

// Opcode :  1 byte
// Hash   : 32 bytes
type GetBlockByHashRequest struct {
	message
}

func CreateGetBlockByHashRequest(hash *number.Big32) *GetBlockByHashRequest {
	// Instantiate the data buffer.
	buffer := make([]byte, 32)
	copy(buffer, hash.Bytes[:])
	// Instantiate the request.
	request := &GetBlockByHashRequest{}
	request.opcode = opcodes["GetBlockByHash"]
	request.datalen = uint64(len(buffer))
	request.data = buffer
	return request
}

func handleGetBlockByHash(opcode uint8, reader io.Reader) (Message, error) {
	// Read 32 bytes of data (the hash).
	msg, err := readCount(opcode, reader, 32)
	if err != nil {
		return nil, err
	}
	// Initialize the concrete message.
	request := &GetBlockByHashRequest{*msg}
	return request, nil
}

func (r *GetBlockByHashRequest) Hash() *number.Big32 {
	// Create a Big32 from the data.
	return number.FromSlice(r.data[0:32])
}

// Opcode : 1 byte
// Found  : 1 byte
// Block  : dynamic
type GetBlockByHashResponse struct {
	message
	block *blockchain.Block
}

func CreateGetBlockByHashResponse(block *blockchain.Block) *GetBlockByHashResponse {
	// Create the response object itself.
	response := &GetBlockByHashResponse{}
	response.opcode = opcodes["GetBlockByHashResponse"]

	if block != nil {
		response.block = block
		// Set the length of the data: 1 byte for the found flag, plus the block.
		response.datalen = uint64(block.LenghtWithMetadata()) + 1
		buffer := bytes.NewBuffer(make([]byte, 0, response.datalen))
		// Write 1 to the buffer to indicate that the block was found.
		buffer.Write([]byte{1})
		// Write the block itself with its metadata.
		block.WriteWithMetadata(buffer)
		response.data = buffer.Bytes()
		return response
	} else {
		response.datalen = 1
		response.data = []byte{0}
	}

	return response
}

func handleGetBlockByHashResponse(opcode uint8, reader io.Reader) (Message, error) {
	// Initialize response object.
	response := &GetBlockByHashResponse{}
	response.opcode = opcode
	// Read the byte that tells whether the block was found or not.
	b := make([]byte, 1)
	read(reader, b)
	found := (b[0] == 1)
	// If found, proceed to read the block as well.
	if found {
		if block, err := blockchain.ReadBlock(reader); err != nil {
			return nil, err
		} else {
			response.block = block
			response.datalen = uint64(block.LenghtWithMetadata()) + 1
			response.data = make([]byte, response.datalen)
			response.data[0] = 1
			copy(response.data[1:], block.BufferWithMetadata())
		}
	}
	return response, nil
}

func (m *GetBlockByHashResponse) Block() *blockchain.Block {
	return m.block
}

func (m *GetBlockByHashResponse) Found() bool {
	// The value in the first data byte determines whether the block was found or not.
	return m.data[0] == 1
}

//=================================================================================================
// Read blocks in minute
//-------------------------------------------------------------------------------------------------

// opcode         : 1 byte
// unix timestamp : 8 bytes
type ReadBlocksInMinuteRequest struct {
	message
}

func CreateReadBlocksInMinute(timestamp int64) *ReadBlocksInMinuteRequest {
	// The data for the request is just the timestamp.
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(timestamp))
	// Instantiate and return the request.
	request := &ReadBlocksInMinuteRequest{}
	request.opcode = opcodes["ReadBlocksInMinute"]
	request.datalen = uint64(len(data))
	request.data = data
	return request
}

func handleReadBlocksInMinute(opcode uint8, reader io.Reader) (Message, error) {
	timestamp := make([]byte, 8)
	if err := read(reader, timestamp); err != nil {
		return nil, err
	}
	rbim := &ReadBlocksInMinuteRequest{}
	rbim.datalen = uint64(len(timestamp))
	rbim.data = timestamp
	return rbim, nil
}

func (r *ReadBlocksInMinuteRequest) Timestamp() int64 {
	return int64(binary.LittleEndian.Uint64(r.data[0:8]))
}

// Opcode      : 1 byte
// Timestamp   : 8 bytes
// Block count : 4 bytes
// List of blocks, each with metadata.
type ReadBlocksInMinuteResponse struct {
	message
}

func CreateReadBlocksInMinuteResponse(timestamp int64, blocks []*blockchain.Block) (*ReadBlocksInMinuteResponse, error) {
	// Create a buffer to hold the timestamp.
	timebuffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(timebuffer, uint64(timestamp))
	// Create a buffer to hold the amount of entries.
	countbuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(countbuffer, uint32(len(blocks)))
	// Create the buffer that will hold blocks.
	// Compute the total length of the blocks first.
	total := 0
	for _, block := range blocks {
		total += int(block.LenghtWithMetadata())
	}
	// Instantiate a buffer to hold all blocks.
	blockbuffer := bytes.NewBuffer(make([]byte, 0, total))
	// Write blocks one by one into the buffer.
	for _, block := range blocks {
		block.WriteWithMetadata(blockbuffer)
	}
	blockdata := blockbuffer.Bytes()

	// Instantiate the response.
	response := &ReadBlocksInMinuteResponse{}
	response.opcode = opcodes["ReadBlocksInMinuteResponse"]
	response.datalen = uint64(len(timebuffer) + len(countbuffer) + len(blockdata))
	response.data = make([]byte, response.datalen)
	copy(response.data[0:8], timebuffer)
	copy(response.data[8:12], countbuffer)
	copy(response.data[12:], blockdata)
	return response, nil
}

func handleReadBlocksInMinuteResponse(opcode uint8, reader io.Reader) (Message, error) {
	// Read the timestamp.
	timestamp := make([]byte, 8)
	read(reader, timestamp)
	// Read the block count.
	countBytes := make([]byte, 4)
	read(reader, countBytes)
	count := binary.LittleEndian.Uint32(countBytes)
	// Read all blocks one by one into a list.
	blocks := make([]*blockchain.Block, count)
	length := uint32(0)

	for i := uint32(0); i < count; i++ {
		if block, err := blockchain.ReadBlock(reader); err != nil {
			return nil, err
		} else {
			blocks[i] = block
			length += block.LenghtWithMetadata()
		}
	}

	// Write all blocks to a buffer.
	blockbuffer := bytes.NewBuffer(make([]byte, 0, int(length)))
	for _, block := range blocks {
		block.WriteWithMetadata(blockbuffer)
	}
	blockdata := blockbuffer.Bytes()

	// Generate the data buffer.
	data := make([]byte, len(timestamp)+len(countBytes)+len(blockdata))
	copy(data[0:8], timestamp)
	copy(data[8:12], countBytes)
	copy(data[12:], blockdata)

	response := &ReadBlocksInMinuteResponse{}
	response.opcode = opcode
	response.datalen = uint64(len(data))
	response.data = data

	return response, nil
}

func (m *ReadBlocksInMinuteResponse) Timestamp() int64 {
	return int64(binary.LittleEndian.Uint64(m.data[0:8]))
}

func (m *ReadBlocksInMinuteResponse) BlockCount() uint32 {
	return binary.LittleEndian.Uint32(m.data[8:12])
}

func (m *ReadBlocksInMinuteResponse) Blocks() []*blockchain.Block {
	// Instantiate a slice to hold the expected amount of blocks.
	blocks := make([]*blockchain.Block, m.BlockCount())
	// Create a reader for the data buffer.
	reader := bytes.NewReader(m.data)
	// Read the first 12 bytes (timestamp and block count).
	reader.Read(make([]byte, 12))
	// Read blocks one by one.
	for count := 0; count < len(blocks); count++ {
		current, _ := blockchain.ReadBlock(reader)
		blocks[count] = current
	}
	return blocks
}

//=================================================================================================
// Write block
//-------------------------------------------------------------------------------------------------

// Opcode              : 1 byte
// Block with metadata : 32 bytes
type WriteBlock struct {
	message
	block *blockchain.Block
}

func CreateWriteBlock(block *blockchain.Block) *WriteBlock {
	request := &WriteBlock{}
	request.opcode = opcodes["WriteBlock"]
	request.datalen = uint64(block.LenghtWithMetadata())
	// Serialize the block and set it on the request.
	buffer := bytes.NewBuffer(make([]byte, 0, request.datalen))
	block.WriteWithMetadata(buffer)
	request.data = buffer.Bytes()
	request.block = block
	return request
}

func (m *WriteBlock) Block() *blockchain.Block {
	return m.block
}

func handleWriteBlock(opcode uint8, reader io.Reader) (Message, error) {
	request := &WriteBlock{}
	request.opcode = opcode

	// Read the block.
	block, err := blockchain.ReadBlock(reader)

	if err != nil {
		return nil, err
	}

	request.block = block
	request.datalen = uint64(block.LenghtWithMetadata())
	request.data = block.BufferWithMetadata()
	return request, nil
}

// Opcode                : 1 byte
// Accepted              : 1 byte
// Current previous hash : 32 bytes
// Current difficulty    : 32 bytes
type WriteBlockResponse struct {
	message
}

func CreateWriteBlockResponse(
	accepted bool, newPreviousHash *number.Big32, newDifficulty *number.Big32) *WriteBlockResponse {

	response := &WriteBlockResponse{}
	response.opcode = opcodes["WriteBlockResponse"]
	response.datalen = 65
	response.data = make([]byte, response.datalen)
	// Set acceptance flag.
	if accepted {
		response.data[0] = 1
	} else {
		response.data[0] = 0
	}
	// Copy current previous hash into the response's data.
	copy(response.data[1:33], newPreviousHash.Bytes[:])
	copy(response.data[33:65], newDifficulty.Bytes[:])
	return response
}

func handleWriteBlockResponse(opcode uint8, reader io.Reader) (Message, error) {
	response := &WriteBlockResponse{}
	response.opcode = opcode
	// Create a buffer to read response data.
	buffer := make([]byte, 65)
	if err := read(reader, buffer); err != nil {
		return nil, err
	}
	response.datalen = uint64(len(buffer))
	response.data = buffer
	return response, nil
}

func (response *WriteBlockResponse) Ok() bool {
	return response.data[0] == 1
}

func (response *WriteBlockResponse) NewPreviousHash() *number.Big32 {
	return number.FromSlice(response.data[1:33])
}

func (response *WriteBlockResponse) NewDifficulty() *number.Big32 {
	return number.FromSlice(response.data[33:65])
}

//=================================================================================================
// Write data
//-------------------------------------------------------------------------------------------------

// Opcode : 1 bytes
// Length : 2 bytes
// Data   : variable
type WriteChunk struct {
	message
}

func CreateWriteChunk(data []byte, datalen uint16) *WriteChunk {
	request := &WriteChunk{}
	request.opcode = opcodes["WriteChunk"]
	request.datalen = uint64(datalen)
	// Create the buffer to hold the length of the data and the data itself.
	request.data = make([]byte, datalen+2)
	binary.LittleEndian.PutUint16(request.data[0:2], datalen)
	copy(request.data[2:], data)
	return request
}

func handleWriteChunk(opcode uint8, reader io.Reader) (Message, error) {
	// Read data length.
	datalenBuffer := make([]byte, 2)
	if err := read(reader, datalenBuffer); err != nil {
		return nil, err
	}
	datalen := binary.LittleEndian.Uint16(datalenBuffer)
	// Read data.
	data := make([]byte, datalen)
	if err := read(reader, data); err != nil {
		return nil, err
	}
	// Instantiate.
	return CreateWriteChunk(data, datalen), nil
}

func (wc *WriteChunk) ChunkData() []byte {
	return wc.data[2:]
}

// Opcode   : 1 bytes
// Accepted : 1 byte
type WriteChunkResponse struct {
	message
}

func CreateWriteChunkResponse(accepted bool) *WriteChunkResponse {
	response := &WriteChunkResponse{}
	response.opcode = opcodes["WriteChunkResponse"]
	response.datalen = 1
	response.data = make([]byte, 1)

	if accepted {
		response.data[0] = 1
	} else {
		response.data[0] = 0
	}

	return response
}

func handleWriteChunkResponse(opcode uint8, reader io.Reader) (Message, error) {
	// Read whether the chunk was accepted or not.
	accepted := make([]byte, 1)
	if err := read(reader, accepted); err != nil {
		return nil, err
	} else {
		return CreateWriteChunkResponse(accepted[0] == 1), nil
	}
}

func (r *WriteChunkResponse) Accepted() bool {
	return r.data[0] == 1
}

//=================================================================================================
// Readers
//-------------------------------------------------------------------------------------------------

func ReadMessage(reader io.Reader) (Message, error) {
	// Read the opcode of the message.
	opcode := make([]byte, 1)
	if err := read(reader, opcode); err != nil {
		return nil, err
	}

	// Call the appropriate handler depending on the opcode.
	if handler, ok := handlers[opcode[0]]; ok {
		return handler(opcode[0], reader)
	} else {
		return nil, errors.New("unexpected opcode")
	}
}

func readCount(opcode uint8, reader io.Reader, datalength int) (*message, error) {
	// Read a fixed amount of bytes as data for the message.
	data := make([]byte, datalength)
	// Try reading the whole data buffer from the reader.
	if err := read(reader, data); err != nil {
		return nil, err
	}
	// Set data on the response.
	msg := &message{}
	msg.opcode = opcode
	msg.datalen = uint64(len(data))
	msg.data = data
	return msg, nil
}

func read(reader io.Reader, buffer []byte) error {
	return communication.Read(reader, buffer)
}
