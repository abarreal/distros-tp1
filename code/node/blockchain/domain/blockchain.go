package domain

import (
	"errors"
	"math/big"
	"sync"
	"time"

	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
	"tp1.aba.distros.fi.uba.ar/node/blockchain/repository"

	number "tp1.aba.distros.fi.uba.ar/common/number/big32"
)

type Blockchain struct {
	writeLock         sync.Mutex
	repository        *repository.BlockRepository
	currentDifficulty *number.Big32
	lastWrite         time.Time
}

func CreateBlockchain(repo *repository.BlockRepository) *Blockchain {
	blockchain := &Blockchain{}
	blockchain.repository = repo
	// When booting up, set the current difficulty to be equal to the
	// difficulty of the block last written, and set the write time
	// to be now.
	blockchain.currentDifficulty = repo.PreviousBlockDifficulty()
	blockchain.lastWrite = time.Now().UTC()
	return blockchain
}

func (blockchain *Blockchain) CurrentDifficulty() *number.Big32 {
	return blockchain.currentDifficulty
}

func (blockchain *Blockchain) CurrentPreviousHash() *number.Big32 {
	return blockchain.repository.PreviousBlockHash()
}

// Writes the given block to the storage. There can be only a single thread
// writing, although there can be multiple readers reading at the same time.
func (blockchain *Blockchain) WriteBlock(block *blockchain.Block) error {

	// Get a lock to ensure that this method is called sequentially.
	blockchain.writeLock.Lock()
	defer blockchain.writeLock.Unlock()

	// Check that the difficulty of the block matches what is expected.
	if !block.Difficulty().Equals(blockchain.currentDifficulty) {
		return errors.New("unexpected difficulty")
	}

	// TODO: Implement check to see if the hash is less than or equal
	// to the expected value, possibly in the save method.

	// Try writing the block to the storage.
	var newDifficulty *number.Big32 = nil
	writeTime := time.Now().UTC()

	computeDifficulty := func() *number.Big32 {
		// The block has been accepted, so we mark the successful write attempt
		// and recompute difficulty.
		deltaSeconds := int64(writeTime.Sub(blockchain.lastWrite).Seconds())
		// Adjust the amount of seconds to avoid dividing by zero.
		if deltaSeconds == 0 {
			deltaSeconds = 1
		}

		// Convert previous difficulty into a big.
		difficulty := block.Difficulty().ToBig()
		// The formula is: new difficulty = (previous difficulty)*(12/(deltaSeconds/256))
		// Compute the denominator.
		denominator := big.NewInt(deltaSeconds)
		// Compute the numerator.
		numerator := new(big.Int).Mul(difficulty, big.NewInt(12*256))

		// Compute the division.
		difficulty = new(big.Int).Div(numerator, denominator)
		newDifficulty = number.FromBig(difficulty)
		return newDifficulty
	}

	if err := blockchain.repository.Save(block, computeDifficulty); err != nil {
		return err
	}

	// Keep track of the current difficulty.
	blockchain.currentDifficulty = newDifficulty
	blockchain.lastWrite = writeTime
	return nil
}

func (blockchain *Blockchain) GetOneWithHash(hash *number.Big32) (*blockchain.Block, error) {
	return blockchain.repository.GetOneWithHash(hash)
}

func (blockchain *Blockchain) GetBlocksFromMinute(timestamp time.Time) ([]*blockchain.Block, error) {
	return blockchain.repository.GetBlocksFromMinute(timestamp)
}
