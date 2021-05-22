package node

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/common/number/big32"
	"tp1.aba.distros.fi.uba.ar/interface/message"
)

const DefaultReadServerPort = 9000
const DefaultWriteServerPort = 9010

const clientConfigPath = "/etc/distros/config/client.env"

func Run() {
	logging.Initialize("Client")
	config.UseFile(clientConfigPath)

	// Identify the argument for the program.
	cmd := os.Args[1]
	// Handle depending on first argument.
	switch cmd {
	case "write":
		handleWrite()
	case "block":
		handleBlockRequest()
	case "minute":
		handleBlocksInMinuteRequest()
	case "stats":
		handleGetMiningStats()
	}
}

func handleWrite() {
	// The data to write is the second argument.
	data := []byte(os.Args[2])

	if len(data) > 65535 {
		data = data[:65535]
	}

	if len(data) < 32 {
		logging.Log(fmt.Sprintf("Sending write chunk request: %s", data))
	} else {
		logging.Log("Sending write chunk request")
	}

	// Instantiate the write chunk request.
	request := message.CreateWriteChunk(data, uint16(len(data)))
	serverPort, _ := config.GetIntOrDefault("WriteServerPort", DefaultWriteServerPort)
	response, err := send(request, serverPort)

	if err != nil {
		logging.LogError("Chunk could not be written", err)
	}

	r := response.(*message.WriteChunkResponse)

	// Print whether the request was accepted or not.
	logging.Log("Write request sent")
	logging.Log(fmt.Sprintf("Accepted: %t", r.Accepted()))
}

func handleBlockRequest() {
	// Get the hash of the block being requested, as an hex string.
	// Then instantiate a Big32 from the hash.
	hashx := os.Args[2]
	hash := big32.FromHexString(hashx)
	// Instantiate a query request from the hash.
	request := message.CreateGetBlockByHashRequest(hash)

	logging.Log(fmt.Sprintf("Sending block request: %s", hashx))
	serverPort, _ := config.GetIntOrDefault("ReadServerPort", DefaultReadServerPort)
	if response, err := send(request, serverPort); err != nil {
		logging.LogError("Could not retrieve block", err)
	} else {
		r := response.(*message.GetBlockByHashResponse)

		if r.Found() {
			block := r.Block()
			logging.Log(fmt.Sprintf("Retrieved block %s", block.Hash().Hex()))

			for it := block.Entries(); it.HasNext(); it.Advance() {
				chunk := it.Chunk()
				logging.Log(fmt.Sprintf("Found entry: %s", string(chunk.Data)))
			}

		} else {
			logging.Log("Block could not be found")
		}
	}
}

func handleBlocksInMinuteRequest() {
	// Get the UNIX timestamp from the first argument.
	requestDatetime, timestampInt, err := parseTimestamp(os.Args[2])

	if err != nil {
		logging.LogError("Could not parse timestamp", err)
	}

	// Instantiate a query request.
	minuteString := requestDatetime.Format("2006-01-02 15:04")
	request := message.CreateReadBlocksInMinute(timestampInt)

	logging.Log(fmt.Sprintf("Sending query for blocks in minute: %s", minuteString))

	serverPort, _ := config.GetIntOrDefault("ReadServerPort", DefaultReadServerPort)
	if response, err := send(request, serverPort); err != nil {
		logging.LogError("The request could not be processed", err)
	} else {
		r := response.(*message.ReadBlocksInMinuteResponse)
		// Notify the amount of blocks found.
		minuteString := time.Unix(r.Timestamp(), 0)
		logging.Log(fmt.Sprintf("Found %d blocks for minute %s", r.BlockCount(), minuteString))
		// Iterate through blocks and write content.
		blocks := r.Blocks()

		for i := 0; i < int(r.BlockCount()); i++ {
			currentBlock := blocks[i]
			logging.Log(fmt.Sprintf("Found block %s", currentBlock.Hash().Hex()))
			for it := currentBlock.Entries(); it.HasNext(); it.Advance() {
				chunk := it.Chunk()
				logging.Log(fmt.Sprintf("Found entry: %s", string(chunk.Data)))
			}
		}
	}
}

func handleGetMiningStats() {
	// Instantiate the request.
	request := message.CreateGetMiningStatistics()
	serverPort, _ := config.GetIntOrDefault("ReadServerPort", DefaultReadServerPort)
	if response, err := send(request, serverPort); err != nil {
		logging.LogError("The request could not be processed", err)
	} else {
		r := response.(*message.GetMiningStatisticsResponse)
		// Display statistics for each miner.
		stats := r.MinerStats()

		for _, stat := range stats {
			logging.Log(fmt.Sprintf("Miner %d", stat.MinerId))
			logging.Log(fmt.Sprintf("Blocks mined successfully: %d", stat.MiningSuccessCount))
			logging.Log(fmt.Sprintf("Failed mining attempts: %d", stat.MiningFailureCount))
		}
	}
}

func parseTimestamp(unixTimestamp string) (time.Time, int64, error) {
	if timestampInt, err := strconv.ParseInt(unixTimestamp, 10, 64); err != nil {
		return time.Now(), 0, err
	} else {
		return time.Unix(timestampInt, 0), timestampInt, err
	}
}

func send(request message.Message, serverPort int) (message.Message, error) {
	// Open a connection with the blockchain service.
	serverName := config.GetStringOrDefault("ServiceHostName", "localhost")

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", serverName, serverPort))

	if err != nil {
		logging.LogError("Could not connect to server", err)
	}

	defer conn.Close()

	// Send the request through the channel.
	if err := request.Write(conn); err != nil {
		logging.LogError("Could not send message", err)
	}
	// Attempt to receive a response.
	response, err := message.ReadMessage(conn)

	if err != nil {
		logging.LogError("Could not receive response", err)
	}

	return response, nil
}
