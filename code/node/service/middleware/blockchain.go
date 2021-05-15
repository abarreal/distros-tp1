package middleware

import (
	"fmt"
	"net"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/common/number/big32"
	"tp1.aba.distros.fi.uba.ar/interface/message"
)

// Blockchain middleware object that delegates requests to the actual blockchain.
type Blockchain struct {
	currentPreviousHash *big32.Big32
	currentDifficulty   *big32.Big32
}

func CreateBlockchain() (*Blockchain, error) {
	blockchain := &Blockchain{}
	if err := blockchain.initializeMiningInfo(); err != nil {
		return nil, err
	} else {
		return blockchain, nil
	}
}

func (b *Blockchain) CurrentPreviousHash() *big32.Big32 {
	return b.currentPreviousHash
}

func (b *Blockchain) CurrentDifficulty() *big32.Big32 {
	return b.currentDifficulty
}

func (b *Blockchain) initializeMiningInfo() error {
	logging.Log("Requesting initial mining info")
	if conn, err := b.openReadConnection(); err != nil {
		return err
	} else {
		req := message.CreateGetMiningInfoRequest()
		if res, err := b.delegate(req, conn); err != nil {
			return err
		} else {
			r := res.(*message.GetMiningInfoResponse)
			b.currentDifficulty = r.Difficulty()
			b.currentPreviousHash = r.PreviousHash()
			logging.Log(fmt.Sprintf("Current previous hash: %s", b.currentPreviousHash.Hex()))
			logging.Log(fmt.Sprintf("Current difficulty: %s", b.currentDifficulty.Hex()))
			return nil
		}
	}
}

func (b *Blockchain) WriteBlock(req *message.WriteBlock) (*message.WriteBlockResponse, error) {
	logging.Log("Sending write block request")
	if conn, err := b.openWriteConnection(); err != nil {
		return nil, err
	} else {
		defer conn.Close()
		// Delegate the request to the server. From the response, update current difficulty
		// and previous hash.
		res1, err := b.delegate(req, conn)

		if err != nil {
			return nil, err
		}

		res2 := res1.(*message.WriteBlockResponse)
		b.currentPreviousHash = res2.NewPreviousHash()
		b.currentDifficulty = res2.NewDifficulty()

		// Retrieved write response, log new state.
		logging.Log(fmt.Sprintf("Obtained WriteBlock response. Accepted: %t", res2.Ok()))
		logging.Log(fmt.Sprintf("New previous hash: %s", res2.NewPreviousHash().Hex()))
		logging.Log(fmt.Sprintf("New difficulty: %s", res2.NewDifficulty().Hex()))

		return res2, nil
	}
}

func (b *Blockchain) GetOneWithHash(req *message.GetBlockByHashRequest) (*message.GetBlockByHashResponse, error) {
	if conn, err := b.openReadConnection(); err != nil {
		return nil, err
	} else {
		defer conn.Close()
		res, err := b.delegate(req, conn)
		return res.(*message.GetBlockByHashResponse), err
	}
}

func (b *Blockchain) GetBlocksFromMinute(req *message.ReadBlocksInMinuteRequest) (*message.ReadBlocksInMinuteResponse, error) {
	if conn, err := b.openReadConnection(); err != nil {
		return nil, err
	} else {
		defer conn.Close()
		if res, err := b.delegate(req, conn); err != nil {
			return nil, err
		} else {
			return res.(*message.ReadBlocksInMinuteResponse), nil
		}
	}
}

func (b *Blockchain) delegate(req message.Message, conn net.Conn) (message.Message, error) {
	// Send the request.
	if err := req.Write(conn); err != nil {
		return nil, err
	}
	// Read the response.
	if response, err := message.ReadMessage(conn); err != nil {
		return nil, err
	} else {
		return response, nil
	}
}

func (b *Blockchain) openReadConnection() (net.Conn, error) {
	serverName := config.GetStringOrDefault("BlockchainServerName", "localhost")
	serverPort := config.GetStringOrDefault("BlockchainReadPort", "8000")
	return net.Dial("tcp", fmt.Sprintf("%s:%s", serverName, serverPort))
}

func (b *Blockchain) openWriteConnection() (net.Conn, error) {
	serverName := config.GetStringOrDefault("BlockchainServerName", "localhost")
	serverPort := config.GetStringOrDefault("BlockchainReadPort", "8010")
	return net.Dial("tcp", fmt.Sprintf("%s:%s", serverName, serverPort))
}
