package domain

import (
	"testing"
	"time"

	"tp1.aba.distros.fi.uba.ar/node/blockchain/repository"

	blocks "tp1.aba.distros.fi.uba.ar/interface/blockchain"
)

func TestBlockchain(t *testing.T) {
	// Instantiate a repository and ensure cleanup.
	repo := createRepository(t)
	defer repo.Cleanup()
	// Instantiate the blockchain itself.
	blockchain := CreateBlockchain(repo)
	// Verify initial arguments.
	validateSeedStatus(blockchain, t)
	// Test basic block writes.
	testWrites(blockchain, t)
	// Test retrieval by timestamp.
	testRetrievalByTimestamp(blockchain, t)
}

func createRepository(t *testing.T) *repository.BlockRepository {
	repo, err := repository.CreateBlockRepository()
	if err != nil {
		t.Fatal("could not create repository")
	}
	return repo
}

func validateSeedStatus(blockchain *Blockchain, t *testing.T) {
	if !blockchain.CurrentDifficulty().IsOne() {
		t.Fatal("unexpected initial difficulty")
	}
	if !blockchain.CurrentPreviousHash().IsZero() {
		t.Fatal("unexpected previous hash")
	}
}

func testWrites(blockchain *Blockchain, t *testing.T) {
	// Instantiate a dummy block.
	block := blocks.CreateDummyBlockWithKnownData(
		blockchain.CurrentPreviousHash(),
		blockchain.CurrentDifficulty())
	// Write the block to the blockchain.
	if err := blockchain.WriteBlock(block); err != nil {
		t.Fatalf("could not write first block: %s", err.Error())
	}
	// Ensure that the difficulty has increased.
	if blockchain.CurrentDifficulty().IsOne() {
		t.Fatal("difficulty did not change after writing first block")
	}
	// Ensure that the previous hash was updated.
	if !blockchain.CurrentPreviousHash().Equals(block.Hash()) {
		t.Fatal("previous hash was not properly updated")
	}

	// Retrieve the block by hash.
	retrieved, err := blockchain.GetOneWithHash(block.Hash())

	if err != nil {
		t.Fatalf("could not retrieve block after initial write: %s", err.Error())
	}

	// Compare the blocks.
	if !retrieved.PreviousHash().Equals(block.PreviousHash()) {
		t.Fatal("unexpected previous hash after reading initial write")
	}
}

func testRetrievalByTimestamp(blockchain *Blockchain, t *testing.T) {

	blockA := blocks.CreateDummyBlockWithKnownData(
		blockchain.CurrentPreviousHash(),
		blockchain.CurrentDifficulty())

	// Set the creation time of the block to some time in the future.
	timeA := time.Now().UTC().Add(10 * time.Minute)
	timeA = timeA.Add(-time.Duration(timeA.Second()) * time.Second)
	blockA.SetCreationTime(timeA)
	blockchain.WriteBlock(blockA)

	// Create a second block a few seconds after the current one.
	blockB := blocks.CreateDummyBlockWithKnownData(
		blockchain.CurrentPreviousHash(),
		blockchain.CurrentDifficulty())

	timeB := timeA.Add(5 * time.Second)
	blockB.SetCreationTime(timeB)
	blockchain.WriteBlock(blockB)

	// Attempt to retrieve the blocks in the minute of time A.
	blocks, err := blockchain.GetBlocksFromMinute(timeA)

	if err != nil {
		t.Fatal("could not read blocks in minute")
	}

	// Ensure that the amount of blocks retrieved is precisely 2.
	if len(blocks) != 2 {
		t.Fatal("unexpected block count")
	}
	// Verify the properties of each block.
	if !blocks[0].Hash().Equals(blockA.Hash()) {
		t.Fatal("unexpected hash in block A")
	}
	if !blocks[1].Hash().Equals(blockB.Hash()) {
		t.Fatal("unexpected hash in block B")
	}
}
