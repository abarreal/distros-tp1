package repository

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/common/synchro"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"

	number "tp1.aba.distros.fi.uba.ar/common/number/big32"
)

// Define default paths for the directories that will hold blockchain and index files.
const defaultBlockchainDir string = "/tmp/distros/blockchain"
const defaultIndexDir string = "/tmp/distros/blockchain/index"
const defaultHeadPath string = "/tmp/distros/blockchain/head"

type BlockRepository struct {
	// The path to the directory that holds blockchain files.
	BlockchainDir string
	// The path to the directory that holds index files.
	IndexDir string
	// The path to a file that will store information about the block last written to the blockchain.
	BlockchainHeadFilepath string
	// Keep information about the block last added to the blockchain.
	previousBlockHash       *number.Big32
	previousBlockTimestamp  int64
	previousBlockDifficulty *number.Big32
	previousBlockLock       sync.RWMutex
}

func CreateBlockRepository() (*BlockRepository, error) {
	// Instantiate a repository object.
	repo := &BlockRepository{}

	// Load directory paths into the repository.
	repo.BlockchainHeadFilepath = config.GetStringOrDefault("BlockchainHeadFilepath", defaultHeadPath)
	repo.BlockchainDir = config.GetStringOrDefault("BlockchainDir", defaultBlockchainDir)
	repo.IndexDir = config.GetStringOrDefault("IndexDir", defaultIndexDir)

	// Create directories that do not exist.
	directories := []string{repo.BlockchainDir, repo.IndexDir}

	for _, dir := range directories {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			os.MkdirAll(dir, 0700)
		}
	}

	// Load the data from the blockchain head file if it exists. If it does not exist,
	// initialize the repository records with zero. The file will be created later when
	// a block is actually written.
	if _, err := os.Stat(repo.BlockchainHeadFilepath); err != nil && os.IsNotExist(err) {
		logging.Log("Blockchain head file could not be found, initializing defaults")
		repo.previousBlockHash = number.Zero
		repo.previousBlockDifficulty = number.One
		repo.previousBlockTimestamp = 0
	} else {
		logging.Log("Blockchain head seems to exist")
		path := repo.BlockchainHeadFilepath
		err := synchro.HandleFileAtomically(path, os.O_RDONLY, func(file *os.File) error {
			// Read the hash of the last created block.
			repo.previousBlockHash = &number.Big32{}
			file.Read(repo.previousBlockHash.Bytes[:])
			// Read the difficulty of the last created block.
			repo.previousBlockDifficulty = &number.Big32{}
			file.Read(repo.previousBlockDifficulty.Bytes[:])
			// Read the timestamp of the last created block.
			timestamp := make([]byte, 8)
			file.Read(timestamp)
			repo.previousBlockTimestamp = int64(binary.LittleEndian.Uint64(timestamp))
			// Return no error.
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	logging.Log("Block repository successfully initialized")
	logging.Log(fmt.Sprintf("Current previous hash: %s", repo.previousBlockHash.Hex()))
	logging.Log(fmt.Sprintf("Current difficulty: %s", repo.previousBlockDifficulty.Hex()))

	return repo, nil
}

//=================================================================================================
// Block Reads
//-------------------------------------------------------------------------------------------------

func (repo *BlockRepository) GetOneWithHash(hash *number.Big32) (*blockchain.Block, error) {
	// Identify the index file from the hash.
	indexFilepath := repo.getIndexPathForHash(hash)
	// Declare a variable to hold the name of the file from which to read the block.
	var blockFilename string = ""
	// Declare a variable to hold the position of the block in the file in which it is stored.
	var blockPosition int64 = 0

	// Open the index file and find the name of the file that holds the block.
	err := synchro.HandleFileAtomically(indexFilepath, os.O_RDONLY, func(file *os.File) error {

		// Define a single byte buffer to hold index entry lengths.
		b := make([]byte, 1)
		// Define a variable to hold read errors.
		var err error = nil

		// Read entries until one matches, or until the end of the file.
		for _, err = file.Read(b); err == nil; _, err = file.Read(b) {
			if err != nil {
				// This case should trigger only if the file is empty, which should in fact
				// not happen.
				break
			}
			// Proceed to read the actual entry and determine if it matches the hash we are
			// looking for. Get the length of the entry as a byte first.
			currentEntryLength := b[0]
			// Read the hash.
			currentHash := make([]byte, 32)
			file.Read(currentHash)
			// Read the position of the block in the file.
			blockpos := make([]byte, 8)
			file.Read(blockpos)
			blockPosition = int64(binary.LittleEndian.Uint64(blockpos))
			// Read the name of the file.
			filename := make([]byte, currentEntryLength-byte(len(currentHash))-8)
			file.Read(filename)
			// If the hash matches, copy the filename and return no error.
			if number.FromSlice(currentHash).Equals(hash) {
				blockFilename = string(filename)
				return nil
			}
		}

		// Either there was an error, or EOF was reached without finding the hash.
		// Return the error.
		if errors.Is(err, io.EOF) {
			return nil
		} else {
			return err
		}
	})
	if err != nil {
		return nil, err
	}

	// Get the path to the file that holds the block.
	blockFilepath := repo.getPathToBlockchainFile(blockFilename)
	// Define a variable to hold the block that we will be reading.
	var block *blockchain.Block = nil

	// Now that we have the name of the file, open it and find the block.
	err = synchro.HandleFileAtomically(blockFilepath, os.O_RDONLY, func(file *os.File) error {
		// Seek to the target position.
		file.Seek(blockPosition, 0)
		// Read the block from the file.
		block, _ = blockchain.ReadBlock(file)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (repo *BlockRepository) GetBlocksFromMinute(t time.Time) ([]*blockchain.Block, error) {
	// Get the name of the file that holds the timestamp.
	filename := getFilenameForTime(t)
	filepath := repo.getPathToBlockchainFile(filename)

	// Get the date.
	year, month, day := t.Date()
	// Get hour and minutes to partition storage by that.
	h := t.Hour()
	m := t.Minute()

	// Instantiate a slice to hold blocks.
	blocks := make([]*blockchain.Block, 0)
	// Read the file and get all blocks that fall in the specific minute from the timestamp.
	err := synchro.HandleFileAtomicallyIfFound(filepath, os.O_RDONLY, func(file *os.File) error {
		// Define an error object to iterate through file blocks.
		var currentError error = nil
		// Define a block pointer.
		var block *blockchain.Block = nil

		// Read the first block.
		block, currentError = blockchain.ReadBlock(file)

		// Read blocks until hitting EOF.
		for ; ; block, currentError = blockchain.ReadBlock(file) {

			if currentError != nil && errors.Is(currentError, io.EOF) {
				// We found the end of the file, so we break here.
				break
			} else if currentError != nil {
				// There was some error and it's not an EOF.
				return currentError
			}

			// Get the timestamp of the block in unix time.
			currentTime := time.Unix(block.Timestamp(), 0).UTC()
			// Check if the minute of the current block matches that of the timestamp.
			currentYear, currentMonth, currentDay := currentTime.Date()
			// Get hour and minutes to partition storage by that.
			currentH := currentTime.Hour()
			currentM := currentTime.Minute()

			matches := currentYear == year && currentMonth == month && currentDay == day
			matches = matches && currentH == h && currentM == m
			if matches {
				// This block was generated in the specified minute.
				// Save the current block to the slice.
				blocks = append(blocks, block)
			}
		}

		return nil
	},
		// Define the function that will handle the case in which the file is not found.
		func() error {
			logging.Log(fmt.Sprintf("File %s not found", filepath))
			logging.Log("There is no file holding blocks in the given minute")
			return nil // Just leave the block list empty.
		})

	if err != nil {
		return nil, err
	}

	// Return the list of blocks.
	return blocks, nil
}

//=================================================================================================
// Block Writes
//-------------------------------------------------------------------------------------------------

// Saves the given block to the file storage. Not thread safe, do not call from multiple threads;
// writes must be sequential. The block will not be available for all queries until it is written
// to the index.
func (repo *BlockRepository) Save(block *blockchain.Block, computeDifficulty func() *number.Big32) error {

	// Ensure that the given block has the right properties.
	if err := repo.validateBlock(block); err != nil {
		return err
	}

	// Get a block specific filepath in which to store the block.
	filepath := repo.getFilepath(block)
	// Keep track of the position in which the block is written.
	var fpos int64 = 0

	// Write the block to the file and get its hash.
	if pos, err := writeBlockToFile(block, filepath); err != nil {
		return err
	} else {
		fpos = pos
	}

	// Write indexes for faster queries.
	if err := repo.indexBlock(block, fpos); err != nil {
		return err
	}

	// Call the callback to get the new difficulty.
	newDifficulty := computeDifficulty()

	// Update the data of the previous block.
	if err := repo.updatePreviousBlockData(block, newDifficulty); err != nil {
		// The given block does not seem to be valid, so we reject it.
		return err
	}

	// Everything went well, apparently. Return no error.
	return nil
}

func (repo *BlockRepository) PreviousBlockHash() *number.Big32 {
	repo.previousBlockLock.Lock()
	defer repo.previousBlockLock.Unlock()
	return repo.previousBlockHash
}

func (repo *BlockRepository) PreviousBlockDifficulty() *number.Big32 {
	repo.previousBlockLock.Lock()
	defer repo.previousBlockLock.Unlock()
	return repo.previousBlockDifficulty
}

func (repo *BlockRepository) validateBlock(block *blockchain.Block) error {
	// Check that the block is valid. Take the lock first.
	repo.previousBlockLock.Lock()
	defer repo.previousBlockLock.Unlock()

	// Ensure that the hashes match.
	if !repo.previousBlockHash.Equals(block.PreviousHash()) {
		return errors.New("the given block does not have the current previous hash")
	}

	// Ensure that the timestamp is correct.
	if repo.previousBlockTimestamp > block.Timestamp() {
		return errors.New("the given block is older than the last created block")
	}

	return nil
}

func (repo *BlockRepository) updatePreviousBlockData(block *blockchain.Block, newDifficulty *number.Big32) error {
	// Persist the information so that we can retrieve it later. Open the blockchain head file
	// for writing, and create it if it does not exist.
	filepath := repo.BlockchainHeadFilepath
	flags := os.O_WRONLY | os.O_CREATE

	err := synchro.HandleFileAtomically(filepath, flags, func(file *os.File) error {

		// Write the hash of the block to the file.
		file.Write(block.Hash().Bytes[:])

		// Write the NEW difficulty to the file.
		file.Write(newDifficulty.Bytes[:])

		// Write the timestamp of the block to the file.
		timestamp := make([]byte, 8)
		binary.LittleEndian.PutUint64(timestamp, uint64(block.Timestamp()))
		file.Write(timestamp)

		// Sync and return.
		return file.Sync()
	})
	if err != nil {
		return err
	}

	// Do keep track of the update.
	repo.previousBlockHash = block.Hash()
	repo.previousBlockDifficulty = block.Difficulty()
	repo.previousBlockTimestamp = block.Timestamp()
	return nil
}

func writeBlockToFile(block *blockchain.Block, filepath string) (int64, error) {
	// Declare a variable to hold the position in the file from which the block is written.
	var fpos int64 = 0
	// Open the file for appending and create it if it does not exist.
	flags := os.O_APPEND | os.O_WRONLY | os.O_CREATE

	err := synchro.HandleFileAtomically(filepath, flags, func(file *os.File) error {
		// Get the current position of the file.
		info, _ := file.Stat()
		fpos = info.Size()
		// Write block header to the file.
		var writer io.Writer = file
		err := block.WriteWithMetadata(writer)
		// Everything went well, apparently. Return no error.
		return err
	})

	// Return the error, if any, or nil if everything went well.
	return fpos, err
}

func (repo *BlockRepository) indexBlock(block *blockchain.Block, fpos int64) error {

	// Get the name of the file to which the index entry should be written.
	indexPath := repo.getIndexPath(block)

	// Open the file for appending, and create it if it does not exist.
	flags := os.O_APPEND | os.O_WRONLY | os.O_CREATE

	// The index file will have the following entry format:
	//
	// * The length of the entry (1 byte)
	//   - The length of the hash (32 bytes) +
	//   - The length of the name of the file +
	//   - The length of the position of the block in the file (8 bytes).
	// * The hash.
	// * The position of the block in the file.
	// * The name of the file that holds the block.
	err := synchro.HandleFileAtomically(indexPath, flags, func(file *os.File) error {
		// Get the name of the file that stores the block.
		blockfile := getFilename(block)
		// Convert the position of the block into bytes.
		blockpos := make([]byte, 8)
		binary.LittleEndian.PutUint64(blockpos, uint64(fpos))
		// Compute the length of the entry.
		length := len(block.Hash().Bytes) + len(blockfile) + len(blockpos)
		// Write the length of the entry to the file.
		file.Write([]byte{byte(length)})
		// Write the hash of the block to the file.
		file.Write(block.Hash().Bytes[:])
		// Write the position of the block in the file that holds it.
		file.Write(blockpos)
		// Write the name of the file that holds the block.
		file.Write([]byte(blockfile))
		// Flush contents and return.
		return file.Sync()
	})

	// Return the error, if any, or nil if everything went well.
	return err
}

func (repo *BlockRepository) getFilepath(block *blockchain.Block) string {
	return path.Join(repo.BlockchainDir, getFilename(block))
}

func (repo *BlockRepository) getPathToBlockchainFile(filename string) string {
	return path.Join(repo.BlockchainDir, filename)
}

func getFilename(block *blockchain.Block) string {
	// Convert the timestamp into a Time object.
	time := time.Unix(block.Timestamp(), 0).UTC()
	return getFilenameForTime(time)
}

func getFilenameForTime(time time.Time) string {
	// Convert to UTC.
	time = time.UTC()
	// Get the date.
	year, month, day := time.Date()
	// Get hour and minutes to partition storage by that.
	h := time.Hour()
	m := time.Minute()
	// Construct the name of the file.
	filename := fmt.Sprintf("blockchain-%d-%d-%d-%d-%d", year, month, day, h, m)
	return filename
}

func (repo *BlockRepository) getIndexPath(block *blockchain.Block) string {
	return repo.getIndexPathForHash(block.Hash())
}

func (repo *BlockRepository) getIndexPathForHash(hash *number.Big32) string {
	// Use the first byte of the hash as an index.
	firstByteFromHash := hash.Bytes[0]
	indexFilename := fmt.Sprintf("index-%d", firstByteFromHash)
	return path.Join(repo.IndexDir, indexFilename)
}

func (repo *BlockRepository) Cleanup() {
	// Delete all directories and files.
	os.Remove(repo.BlockchainHeadFilepath)
	os.RemoveAll(repo.IndexDir)
	os.RemoveAll(repo.BlockchainDir)
}
