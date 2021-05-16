package repository

import (
	"crypto/rand"
	"os"
	"testing"
	"time"

	b32 "tp1.aba.distros.fi.uba.ar/common/number/big32"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
)

func TestCreation(t *testing.T) {
	// Create the repository.
	repo, err := CreateBlockRepository()
	defer cleanup(repo)
	// Ensure that creation was successful.
	if err != nil {
		t.Fatalf("could not create repository: %s", err.Error())
	}
	// Ensure that all default files have been created.
	if _, err := os.Stat(repo.IndexDir); err != nil {
		t.Fatalf("directory %s does not exist", repo.IndexDir)
	}
	if _, err := os.Stat(repo.BlockchainDir); err != nil {
		t.Fatalf("directory %s does not exist", repo.BlockchainDir)
	}
	// Remove all files.
	cleanup(repo)
	// Ensure that all default files have been deleted.
	if _, err := os.Stat(repo.IndexDir); !os.IsNotExist(err) {
		t.Fatalf("directory %s still seems to exist", repo.IndexDir)
	}
	if _, err := os.Stat(repo.BlockchainDir); !os.IsNotExist(err) {
		t.Fatalf("directory %s still seems to exist", repo.BlockchainDir)
	}
}

func TestDefaultHash(t *testing.T) {
	// Create a new repository.
	repo, _ := CreateBlockRepository()
	defer cleanup(repo)
	// Ensure that the default previous hash is all zeros.
	if hash := repo.PreviousBlockHash(); !hash.IsZero() {
		t.Fatalf("the initial previous hash seems not to be zero")
	}
}

func TestBlockStorage(t *testing.T) {
	// Create a new repository.
	repo, _ := CreateBlockRepository()
	defer cleanup(repo)
	// Create a new block.
	block := testBlock(t, true)
	// Ensure that the block has the previous hash.
	if !repo.PreviousBlockHash().Equals(block.PreviousHash()) {
		t.Fatalf("the block does not seem to have the previous hash in the repo")
	}
	// Save the block in the repository.
	if err := repo.Save(block, computeDifficulty); err != nil {
		t.Fatalf("could not write block: %s", err.Error())
	}
	// Ensure that a file was created in the blockchain directory.
	entries, err := os.ReadDir(repo.BlockchainDir)

	if err != nil {
		t.Fatalf("could not read files in blockchain directory: %s", err.Error())
	}

	found := false
	for _, entry := range entries {
		if !entry.IsDir() {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("could not find a file in blockchain directory")
	}

	// Find the block by hash.
	block2, err := repo.GetOneWithHash(block.Hash())

	// Ensure that the block was read correctly.
	if err != nil {
		t.Fatalf("could not retrieve block by hash: %s", err.Error())
	}
	// Ensure that the block pointer is not nil.
	if block2 == nil {
		t.Fatalf("The block pointer is nil")
	}
	// Ensure that properties match.
	if !block.Hash().Equals(block2.Hash()) {
		t.Fatalf("block hashes do not match")
	}
	if !block.PreviousHash().Equals(block2.PreviousHash()) {
		t.Fatalf("previous block hashes do not match")
	}
}

func TestBlockRetrievalByMinute(t *testing.T) {

	// Instantiate a repository.
	repo, _ := CreateBlockRepository()
	defer cleanup(repo)

	timebase := time.Now().UTC()
	timebase = timebase.Add(-time.Duration(timebase.Second()) * time.Second)

	// Instantiate block A. Set it to a known minute.
	blockA := testBlock(t, true)
	blockA.SetCreationTime(timebase)
	// Instantiate block B. Set it to the same minute as block A.
	// Set the hash as well.
	blockB := testBlockAfter(t, true, blockA)
	blockB.SetCreationTime(timebase.Add(5 * time.Second))
	// Instantiate block C. Set it to a different minute than the previous blocks.
	blockC := testBlockAfter(t, true, blockB)
	blockC.SetCreationTime(timebase.Add(5 * time.Minute))

	// Write all blocks to the repository.
	if err := repo.Save(blockA, computeDifficulty); err != nil {
		t.Fatalf("There was some error while writing block A: %s", err.Error())
	}
	if err := repo.Save(blockB, computeDifficulty); err != nil {
		t.Fatalf("There was some error while writing block B: %s", err.Error())
	}
	if err := repo.Save(blockC, computeDifficulty); err != nil {
		t.Fatalf("There was some error while writing block C: %s", err.Error())
	}

	// Retrieve blocks by minute.
	blocks, err := repo.GetBlocksFromMinute(timebase.Add(5 * time.Second))

	if err != nil {
		t.Fatalf("could not read blocks")
	}

	// Verify that two blocks were read.
	if len(blocks) != 2 {
		t.Fatalf("unexpected block count: %d", len(blocks))
	}
	// Verify that they are the correct blocks.
	if !blocks[0].Hash().Equals(blockA.Hash()) {
		t.Fatalf("unexpected first block")
	}
	if !blocks[1].Hash().Equals(blockB.Hash()) {
		t.Fatalf("unexpected second block")
	}
}

func cleanup(repo *BlockRepository) {
	// Delete all directories and files.
	os.Remove(repo.BlockchainHeadFilepath)
	os.RemoveAll(repo.IndexDir)
	os.RemoveAll(repo.BlockchainDir)
}

func testBlockAfter(t *testing.T, zeroHash bool, block *blockchain.Block) *blockchain.Block {
	var previousHash *b32.Big32 = nil

	if block != nil {
		previousHash = block.Hash()
	} else if zeroHash {
		previousHash = b32.Zero
	} else {
		previousHash = random32()
	}

	if block, err := blockchain.CreateBlock(previousHash, random32(), testEntries()); err != nil {
		t.Fatalf("could not create block")
		return nil
	} else {
		return block
	}
}

func testBlock(t *testing.T, zeroHash bool) *blockchain.Block {
	return testBlockAfter(t, zeroHash, nil)
}

func random32() *b32.Big32 {
	buff := make([]byte, 32)
	rand.Read(buff)
	return b32.FromSlice(buff)
}

func testChunk(data string) *blockchain.Chunk {
	chunkData := []byte(data)
	chunk := blockchain.CreateChunk(chunkData)
	return chunk
}

func testEntries() *blockchain.Chunk {
	head := testChunk("Hello")
	tail := testChunk("World")
	head.SetNext(tail)
	return head
}

func computeDifficulty() *b32.Big32 {
	return b32.Zero
}
