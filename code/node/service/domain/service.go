package service

import (
	"fmt"

	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
	"tp1.aba.distros.fi.uba.ar/interface/message"
	"tp1.aba.distros.fi.uba.ar/node/service/middleware"
)

// The blockchain service handles client requests. Read requests are delegated to the
// blockchain service itself. Write requests are delegated to workers that will
// generate an appropriate nonce and return it back to the service, which will
// send it to the blockchain.
type BlockchainService struct {
	blockchain *middleware.Blockchain
}

func CreateBlockchainService(blockchain *middleware.Blockchain) *BlockchainService {
	svc := &BlockchainService{}
	svc.blockchain = blockchain
	return svc
}

func (svc *BlockchainService) HandleWriteChunk(req *message.WriteChunk) (
	*message.WriteChunkResponse, error) {
	// TODO: Store the chunk for writing later. Have a goroutine in the background periodically
	// reading cached chunks and constructing blocks from them. Delegate those blocks to the
	// miners to find a good nonce.

	// For the moment just construct a block from the chunk and send.
	chunk := blockchain.CreateChunk(req.ChunkData())

	// Get previous hash and difficulty from the blockchain. They will be used to
	// instantiate the new block.
	previousHash, difficulty := svc.blockchain.CurrentPreviousHash(), svc.blockchain.CurrentDifficulty()

	if block, err := blockchain.CreateBlock(previousHash, difficulty, chunk); err != nil {
		return nil, err
	} else {

		logging.Log("Created block for request")
		logging.Log(fmt.Sprintf("Created block's hash: %s", block.Hash().Hex()))
		logging.Log(fmt.Sprintf("Created block's previous hash: %s", block.PreviousHash().Hex()))
		logging.Log(fmt.Sprintf("Created block's difficulty: %s", block.Difficulty().Hex()))

		blockRequest := message.CreateWriteBlock(block)
		if blockResponse, err := svc.blockchain.WriteBlock(blockRequest); err != nil {
			return nil, err
		} else {
			return message.CreateWriteChunkResponse(blockResponse.Ok()), nil
		}
	}

}

func (svc *BlockchainService) HandleGetBlock(req *message.GetBlockByHashRequest) (
	*message.GetBlockByHashResponse, error) {
	// Simply delegate the request to the blockchain middleware.
	return svc.blockchain.GetOneWithHash(req)
}

func (svc *BlockchainService) HandleGetBlocksFromMinute(req *message.ReadBlocksInMinuteRequest) (
	*message.ReadBlocksInMinuteResponse, error) {
	// Simply delegate the request to the blockchain middleware.
	return svc.blockchain.GetBlocksFromMinute(req)
}
