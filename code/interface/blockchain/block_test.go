package blockchain

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	b32 "tp1.aba.distros.fi.uba.ar/common/number/big32"
)

// Initialize some test data to create blocks.
var previousHash *b32.Big32 = random32()
var difficulty *b32.Big32 = random32()
var entries *Chunk = testEntries()

func TestBasicParameters(t *testing.T) {
	block := testBlock(t)
	// Check block properties and ensure that they match what was set.
	// Check previous hash.
	if !previousHash.Equals(block.PreviousHash()) {
		t.Fatalf("unexpected previous hash")
	}
	// Check difficulty.
	if !difficulty.Equals(block.Difficulty()) {
		t.Fatalf("unexpected difficulty")
	}
}

func TestTimestamp(t *testing.T) {
	before := time.Now().UTC().Unix()
	block := testBlock(t)

	after := time.Now().UTC().Unix()
	timestamp := block.Timestamp()

	if timestamp < before {
		t.Fatalf("Unexpected timestamp: %d < %d", timestamp, before)
	}
	if timestamp > after {
		t.Fatalf("Unexpected timestamp: %d > %d", timestamp, after)
	}
}

func TestHashing(t *testing.T) {
	block := testBlock(t)

	// Generate a nonce.
	block.GenerateNonce()
	// Get the hash twice.
	hash1 := block.Hash()
	hash2 := block.Hash()

	// Ensure that both hashes match.
	if !hash1.Equals(hash2) {
		t.Fatalf("Unexpected hash mismatch")
	}

	// Regenerate nonce and recompute hash.
	block.GenerateNonce()
	hash3 := block.Hash()

	if hash3.Equals(hash1) {
		t.Fatalf("Unexpected hash match")
	}
}

func TestEntries(t *testing.T) {
	block := testBlock(t)

	// Check entry count.
	entryCount := block.EntryCount()
	if entryCount != 2 {
		t.Fatalf("Unexpected entry count: %d", entryCount)
	}
	// Test entry data contents.
	testEntryContents(block, t)
}

func TestReadWrite(t *testing.T) {
	// Create a block that we will be writing.
	block := testBlock(t)
	// Create a buffer to hold block data.
	buffer := bytes.NewBuffer(make([]byte, 0, 256))
	// Write the block to the buffer.
	block.WriteWithMetadata(buffer)
	// Read the block.
	block2, err := ReadBlock(buffer)
	if err != nil {
		t.Fatalf("Unexpected error while reading block")
	}
	// Ensure that the block has all required properties.
	if !block2.PreviousHash().Equals(block.PreviousHash()) {
		t.Fatalf("Written and read blocks do not match")
	}
	// Check entries and ensure that all data matches.
	testEntryContents(block2, t)
}

func testEntryContents(block *Block, t *testing.T) {
	// Create a slice to collect test data from the chunks.
	entries := make([]string, 0, 2)

	// Iterate through block entries.
	for it := block.Entries(); it.HasNext(); it.Advance() {
		// Get the current chunk.
		chunk := it.Chunk()
		// Add the data as a string to a slice.
		entries = append(entries, string(chunk.Data))
	}

	if entries[0] != "Hello" {
		t.Fatalf("Unexpected data: %s != Hello", entries[0])
	}
	if entries[1] != "World" {
		t.Fatalf("Unexpected data: %s != World", entries[1])
	}
}

func TestHashAttempt(t *testing.T) {
	block := testBlock(t)

	// Set block difficulty to 1.
	block.setDifficulty(b32.One)
	// Attempt hashing and expect result to be true.
	found := block.AttemptHash()

	if !found {
		t.Fatal("Unexpected hash attempt result for easy difficulty")
	}

	// Attempt hashing with a large difficulty.
	large := make([]byte, 32)

	for i := 0; i < len(large); i++ {
		large[i] = 0xff
	}

	block.setDifficulty(b32.FromSlice(large))
	found = block.AttemptHash()

	if found {
		t.Fatal("Unexpected hash attempt result for hard difficulty")
	}
}

func random32() *b32.Big32 {
	buff := make([]byte, 32)
	rand.Read(buff)
	return b32.FromSlice(buff)
}

func testChunk(data string) *Chunk {
	chunkData := []byte(data)
	chunk := CreateChunk(chunkData)
	return chunk
}

func testEntries() *Chunk {
	head := testChunk("Hello")
	tail := testChunk("World")
	head.SetNext(tail)
	return head
}

func testBlock(t *testing.T) *Block {
	block, err := CreateBlock(previousHash, difficulty, entries)

	if err != nil {
		t.Fatalf("could not create block")
	}

	return block
}
