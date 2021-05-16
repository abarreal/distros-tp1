package message

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	b32 "tp1.aba.distros.fi.uba.ar/common/number/big32"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
)

func TestGetMiningInfoRequest(t *testing.T) {
	// Instantiate the request.
	gmi := CreateGetMiningInfoRequest().(*GetMiningInfo)
	// Ensure that the GMI opcode is correct.
	if gmi.Opcode() != opcodes["GetMiningInfo"] {
		t.Fatal("unexpected opcode")
	}
	// Ensure that the GMI request has no data.
	if gmi.DataLength() != 0 {
		t.Fatal("unexpected data length")
	}
	// Try to write the request into a buffer.
	buffer := bytes.NewBuffer(make([]byte, 0, 256))
	if err := gmi.Write(buffer); err != nil {
		t.Fatal(err.Error())
	}
	// Try to read from the buffer.
	output, err := ReadMessage(buffer)

	if err != nil {
		t.Fatal(err.Error())
	}

	// Verify output fields.
	if output.Opcode() != opcodes["GetMiningInfo"] {
		t.Fatal("unexpected opcode after write")
	}
	if output.DataLength() != 0 {
		t.Fatal("unexpected data length")
	}
}

func TestGetMiningInfoResponse(t *testing.T) {
	// Instantiate the response.
	response := CreateGetMiningInfoResponse(random32(), random32())
	// Write the response to a buffer.
	buffer := bytes.NewBuffer(make([]byte, 0, response.DataLength()))
	if err := response.Write(buffer); err != nil {
		t.Fatalf("could not write buffer: %s", err.Error())
	}
	// Read the response from the buffer.
	msg, err := ReadMessage(buffer)

	if err != nil {
		t.Fatal("could not read response after writing")
	}

	// Ensure that the fields are what is expected.
	response2 := msg.(*GetMiningInfoResponse)

	if response2.Opcode() != opcodes["GetMiningInfoResponse"] {
		t.Fatal("unexpected opcode")
	}
	if !response2.PreviousHash().Equals(response.PreviousHash()) {
		t.Fatal("unexpected previous hash")
	}
	if !response2.Difficulty().Equals(response.Difficulty()) {
		t.Fatal("unexpected difficulty")
	}
}

func TestGetBlockByHashRequest(t *testing.T) {
	// Instantiate the request.
	request := CreateGetBlockByHashRequest(random32())
	// Write the request into a buffer.
	buffer := bytes.NewBuffer(make([]byte, 0, request.DataLength()))
	if err := request.Write(buffer); err != nil {
		t.Fatalf("could not write buffer: %s", err.Error())
	}
	// Read the request from the buffer.
	msg, err := ReadMessage(buffer)

	if err != nil {
		t.Fatal("could not read request after writing")
	}

	// Ensure that the fields are what is expected.
	request2 := msg.(*GetBlockByHashRequest)
	// Ensure that the fields are what is expected.
	if request2.Opcode() != opcodes["GetBlockByHash"] {
		t.Fatal("unexpected opcode")
	}
	if !request2.Hash().Equals(request.Hash()) {
		t.Fatal("unexpected hash")
	}
}

func TestGetBlockByHashResponse(t *testing.T) {
	// Create a block.
	block := blockchain.CreateDummyBlock()
	// Instantiate the response.
	response := CreateGetBlockByHashResponse(block)

	if !response.Found() {
		t.Fatal("response created as not found")
	}

	// Write the response into a buffer.
	buffer := bytes.NewBuffer(make([]byte, 0, response.DataLength()))
	if err := response.Write(buffer); err != nil {
		t.Fatalf("could not write buffer: %s", err.Error())
	}
	// Read the response from the buffer.
	msg, err := ReadMessage(buffer)

	if err != nil {
		t.Fatal("could not read request after writing")
	}
	// Ensure that the fields are what is expected.
	response2 := msg.(*GetBlockByHashResponse)

	if response2.Opcode() != response.Opcode() {
		t.Fatal("unexpected opcode")
	}
	if !response2.Found() {
		t.Fatal("block not found")
	}

	block2 := response2.Block()

	if !block2.Hash().Equals(block.Hash()) {
		t.Fatal("unexpected block hash")
	}

}

func TestHandleReadBlocksInMinute(t *testing.T) {
	// Create the timestamp.
	now := time.Now().UTC().Unix()
	// Instantiate the request.
	request := CreateReadBlocksInMinute(now)
	if request.Timestamp() != now {
		t.Fatal("unexpected timestamp in request")
	}

	// Write the request into a buffer.
	buffer := bytes.NewBuffer(make([]byte, 0, request.DataLength()))
	if err := request.Write(buffer); err != nil {
		t.Fatalf("could not write buffer: %s", err.Error())
	}
	// Read the request from the buffer.
	msg, err := ReadMessage(buffer)

	if err != nil {
		t.Fatal("could not read request after writing")
	}

	request2 := msg.(*ReadBlocksInMinuteRequest)

	// Check that the timestamp matches.
	if request2.Timestamp() != request.Timestamp() {
		t.Fatal("unexpected timestamp")
	}
}

func TestHandleReadBlocksInMinuteResponse(t *testing.T) {
	// Create basic data.
	block1 := blockchain.CreateDummyBlock()
	block2 := blockchain.CreateDummyBlock()
	blocks := []*blockchain.Block{block1, block2}

	timestamp := time.Now().UTC().Unix()

	// Create response.
	response, err := CreateReadBlocksInMinuteResponse(timestamp, blocks)

	if err != nil {
		t.Fatal("could not create response")
	}
	if response.BlockCount() != 2 {
		t.Fatal("unexpected block count when creating")
	}
	if response.Timestamp() != timestamp {
		t.Fatal("unexpected timestamp when creating")
	}

	// Write the response into a buffer.
	buffer := bytes.NewBuffer(make([]byte, 0, response.DataLength()))
	if err := response.Write(buffer); err != nil {
		t.Fatalf("could not write buffer: %s", err.Error())
	}
	// Read the request from the buffer.
	msg, err := ReadMessage(buffer)

	if err != nil {
		t.Fatalf("could read message: %s", err.Error())
	}

	response2 := msg.(*ReadBlocksInMinuteResponse)

	// Ensure that data properly matches.
	if response2.Timestamp() != response.Timestamp() {
		t.Fatal("unexpected timestamp")
	}
	if response2.BlockCount() != 2 {
		t.Fatal("unexpected block count")
	}
	// Retrieve all blocks.
	blocks1 := response.Blocks()
	blocks2 := response2.Blocks()

	// Ensure that block data matches.
	for i := 0; i < int(response2.BlockCount()); i++ {
		a := blocks1[0]
		b := blocks2[0]
		if !a.Hash().Equals(b.Hash()) {
			t.Fatalf("unexpected hash in block %d", i)
		}
	}
}

func TestWriteBlock(t *testing.T) {
	block := blockchain.CreateDummyBlock()
	request := CreateWriteBlock(block)

	if request.Opcode() != opcodes["WriteBlock"] {
		t.Fatal("unexpected opcode")
	}

	// Write into a buffer.
	buffer := bytes.NewBuffer(make([]byte, 0, request.DataLength()))
	if err := request.Write(buffer); err != nil {
		t.Fatalf("could not write buffer: %s", err.Error())
	}
	// Read the request from the buffer.
	msg, err := ReadMessage(buffer)
	if err != nil {
		t.Fatalf("could not read request: %s", err.Error())
	}
	request2 := msg.(*WriteBlock)

	// Check request properties.
	if request2.Opcode() != request.Opcode() {
		t.Fatal("unexpected opcode")
	}
	// Get the block and check properties.
	block2 := request2.Block()
	if !block.Hash().Equals(block2.Hash()) {
		t.Fatal("unexpected hash")
	}
}

func TestWriteBlockResponse(t *testing.T) {
	hash := random32()
	diff := random32()

	response := CreateWriteBlockResponse(true, hash, diff)

	// Check properties.
	if !response.NewPreviousHash().Equals(hash) {
		t.Fatal("unexpected hash after creation")
	}
	if !response.NewDifficulty().Equals(diff) {
		t.Fatal("unexpected difficulty after creation")
	}

	// Write into a buffer.
	buffer := bytes.NewBuffer(make([]byte, 0, response.DataLength()))
	if err := response.Write(buffer); err != nil {
		t.Fatalf("could not write buffer: %s", err.Error())
	}
	// Read the request from the buffer.
	msg, err := ReadMessage(buffer)
	if err != nil {
		t.Fatalf("could not read request: %s", err.Error())
	}
	response2 := msg.(*WriteBlockResponse)

	// Check properties.
	if !response2.Ok() {
		t.Fatal("write should have been accepted")
	}
	if !response2.NewPreviousHash().Equals(response.NewPreviousHash()) {
		t.Fatal("unexpected hash")
	}
	if !response2.NewDifficulty().Equals(response.NewDifficulty()) {
		t.Fatal("unexpected new difficulty")
	}
}

func TestWriteChunk(t *testing.T) {
	data := "helloworld"
	request := CreateWriteChunk([]byte(data), uint16(len(data)))

	if request.opcode != opcodes["WriteChunk"] {
		t.Fatal("unexpected opcode")
	}

	// Write request to a buffer.
	buffer := bytes.NewBuffer(make([]byte, 0, request.DataLength()))
	if err := request.Write(buffer); err != nil {
		t.Fatalf("could not write buffer: %s", err.Error())
	}
	// Read the request from the buffer.
	msg, err := ReadMessage(buffer)
	if err != nil {
		t.Fatalf("could not read request: %s", err.Error())
	}
	request2 := msg.(*WriteChunk)

	// Check properties.
	if request2.opcode != opcodes["WriteChunk"] {
		t.Fatal("unexpected opcode")
	}
	if request2.DataLength() != uint64(len(data)) {
		t.Fatal("unexpected data length")
	}
	if string(request2.ChunkData()) != data {
		t.Fatal("unexpected data")
	}
}

func random32() *b32.Big32 {
	buff := make([]byte, 32)
	rand.Read(buff)
	return b32.FromSlice(buff)
}
