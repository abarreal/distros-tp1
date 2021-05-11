package api

type IBlockchainServiceApi interface {

	// TODO: Write documentation
	Save(data []byte, len uint16) error

	// TODO: Define interface, write documentation
	GetBlock() error

	// TODO: Define interface, write documentation
	GetBlocks() error

	// TODO: Define interface, write documentation
	GetMiningStatistics() error
}
