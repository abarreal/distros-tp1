package node

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"tp1.aba.distros.fi.uba.ar/common/config"
	"tp1.aba.distros.fi.uba.ar/common/logging"
	"tp1.aba.distros.fi.uba.ar/common/number/big32"
	"tp1.aba.distros.fi.uba.ar/interface/blockchain"
	"tp1.aba.distros.fi.uba.ar/interface/message"
)

const configurationPath = "/etc/distros/config/autoclient.env"
const ControlStop = 0

const DefaultReadServerPort = 9000
const DefaultWriteServerPort = 9010

// The autoclient sends requests automatically to the blockchain service.
func Run() {
	logging.Initialize("Autoclient")
	config.UseFile(configurationPath)

	// Create a wait group for the workers.
	wg := &sync.WaitGroup{}

	// Get the amount of writer threads and some configuration parameters.
	writerCount, _ := config.GetIntOrDefault("WriterCount", 4)
	writerDelayMsMin, _ := config.GetIntOrDefault("WriterDelayMsMin", 1000)
	writerDelayMsMax, _ := config.GetIntOrDefault("WriterDelayMsMax", 2000)

	// Get the amount of reader threads and some configuration parameters.
	readerCount, _ := config.GetIntOrDefault("ReaderCount", 4)
	readerDelayMsMin, _ := config.GetIntOrDefault("ReaderDelayMsMin", 4000)
	readerDelayMsMax, _ := config.GetIntOrDefault("ReaderDelayMsMax", 8000)
	readerInitialDelayMsMax, _ := config.GetIntOrDefault("ReaderInitialDelayMsMax", 8000)

	// Create a slice to hold control channels.
	writerControl := make([]chan int, writerCount)
	readerControl := make([]chan int, readerCount)

	// Run writers.
	for i := 0; i < writerCount; i++ {
		wg.Add(1)
		writerControl[i] = make(chan int)
		go RunWriter(i, writerControl[i], wg, writerDelayMsMin, writerDelayMsMax)
	}
	// Run readers.
	for i := 0; i < readerCount; i++ {
		wg.Add(1)
		readerControl[i] = make(chan int)
		go RunReader(i, readerControl[i], wg, readerDelayMsMin, readerDelayMsMax, readerInitialDelayMsMax)
	}

	// Wait for the incoming quit signal.
	sigchannel := make(chan os.Signal, 1)
	signal.Notify(sigchannel, syscall.SIGINT, syscall.SIGTERM)
	// There are only quit signals to handle. The program should
	// quit as soon as one is received.
	<-sigchannel

	// Send quit signals to all writers and readers.
	for i := 0; i < writerCount; i++ {
		writerControl[i] <- ControlStop
	}
	for i := 0; i < readerCount; i++ {
		readerControl[i] <- ControlStop
	}

	// Wait for the threads to finish.
	wg.Wait()
}

//=================================================================================================
// Writer.
//-------------------------------------------------------------------------------------------------
func RunWriter(id int, control <-chan int, waitGroup *sync.WaitGroup, delayMin int, delayMax int) {
	// Begin main loop.
	stopping := false

	// Generate a timeout.
	timeoutDelay := rand.Intn(delayMax-delayMin) + delayMin
	timeout := time.After(time.Duration(timeoutDelay) * time.Millisecond)

	for !stopping {
		select {
		case <-control:
			stopping = true
		case <-timeout:
			// Generate a random string to be written.
			data := []byte(randomString(16))
			// Send write request.
			writeDataChunk(id, data)
			// Generate a new timeout.
			timeoutDelay := rand.Intn(delayMax-delayMin) + delayMin
			timeout = time.After(time.Duration(timeoutDelay) * time.Millisecond)
		}
	}

	// Send the finalization signal.
	waitGroup.Done()
}

//=================================================================================================
// Reader.
//-------------------------------------------------------------------------------------------------
func RunReader(id int, control <-chan int, waitGroup *sync.WaitGroup, delayMin int, delayMax int, maxInitialDelay int) {
	// Begin main loop.
	stopping := false

	// Generate a timeout. For the readers, they start after some initial delay time.
	timeoutDelay := rand.Intn(delayMax-delayMin) + delayMin + rand.Intn(maxInitialDelay)
	timeout := time.After(time.Duration(timeoutDelay) * time.Millisecond)

	// Keep an array of known block hashes.
	hashcount := 0
	hashes := make([]string, 64)

	// Keep track of the start time.
	startTime := time.Now().Unix()

	for !stopping {
		select {
		case <-control:
			stopping = true
		case <-timeout:
			// Determine whether to request for a block in some given minute, or to request for
			// a specific block whose hash is known.
			if rand.Intn(2) == 0 {
				// Query for a set of blocks in some randomly chosen minute from the time the system
				// started up until now.
				currentTime := time.Now().Unix()
				// Generate an integer between the start time and the current time.
				selectedTime := rand.Intn(int(currentTime-startTime)) + int(startTime)
				blocks := handleBlocksInMinuteRequest(id, fmt.Sprintf("%d", selectedTime))
				// Record the hash of the retrieved blocks to query by hash later.
				for _, block := range blocks {
					hashes[hashcount%cap(hashes)] = block.Hash().Hex()
					hashcount++
				}
			} else if hashcount > 0 {
				// Query for the hash of a known block in the else case.
				hashidx := rand.Intn(min(hashcount, len(hashes)))
				hash := hashes[hashidx]
				handleBlockRequest(id, hash)
			}

			// Generate a new timeout.
			timeoutDelay := rand.Intn(delayMax-delayMin) + delayMin
			timeout = time.After(time.Duration(timeoutDelay) * time.Millisecond)
		}
	}

	// Send the finalization signal.
	waitGroup.Done()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func handleBlockRequest(readerId int, hashx string) {
	// Get the hash of the block being requested, as an hex string.
	// Then instantiate a Big32 from the hash.
	hash := big32.FromHexString(hashx)
	// Instantiate a query request from the hash.
	request := message.CreateGetBlockByHashRequest(hash)

	logging.Log(fmt.Sprintf("[Reader %d] Sending block request: %s", readerId, hashx))
	serverPort, _ := config.GetIntOrDefault("ReadServerPort", DefaultReadServerPort)
	if response, err := send(request, serverPort); err != nil {
		logging.LogError(fmt.Sprintf("[Reader %d] Could not retrieve block", readerId), err)
		return
	} else {
		r := response.(*message.GetBlockByHashResponse)

		if r.Found() {
			block := r.Block()
			logging.Log(fmt.Sprintf("[Reader %d] Retrieved block %s", readerId, block.Hash().Hex()))

			for it := block.Entries(); it.HasNext(); it.Advance() {
				chunk := it.Chunk()
				logging.Log(fmt.Sprintf("[Reader %d] Found entry: %s", readerId, string(chunk.Data)))
			}

		} else {
			logging.Log(fmt.Sprintf("[Reader %d] Block could not be found", readerId))
		}
	}
}

//=================================================================================================
// Client functions.
//-------------------------------------------------------------------------------------------------
func writeDataChunk(writerId int, data []byte) {
	if len(data) > 65535 {
		data = data[:65535]
	}

	if len(data) < 32 {
		logging.Log(fmt.Sprintf("[Writer %d] Sending write chunk request: %s", writerId, data))
	} else {
		logging.Log(fmt.Sprintf("[Writer %d] Sending write chunk request", writerId))
	}

	// Instantiate the write chunk request.
	request := message.CreateWriteChunk(data, uint16(len(data)))
	serverPort, _ := config.GetIntOrDefault("WriteServerPort", DefaultWriteServerPort)
	response, err := send(request, serverPort)

	if err != nil {
		logging.LogError(fmt.Sprintf("[Writer %d] Chunk could not be written", writerId), err)
	}

	r := response.(*message.WriteChunkResponse)

	// Print whether the request was accepted or not.
	logging.Log(fmt.Sprintf("[Writer %d] Write request sent", writerId))
	logging.Log(fmt.Sprintf("[Writer %d] Accepted: %t", writerId, r.Accepted()))
}

func handleBlocksInMinuteRequest(readerId int, timestampString string) []*blockchain.Block {
	// Get the UNIX timestamp from the first argument.
	requestDatetime, timestampInt, err := parseTimestamp(timestampString)

	if err != nil {
		logging.LogError(fmt.Sprintf("[Reader %d] Could not parse timestamp", readerId), err)
		return nil
	}

	// Instantiate a query request.
	minuteString := requestDatetime.Format("2006-01-02 15:04")
	request := message.CreateReadBlocksInMinute(timestampInt)

	logging.Log(fmt.Sprintf("[Reader %d] Sending query for blocks in minute: %s", readerId, minuteString))

	serverPort, _ := config.GetIntOrDefault("ReadServerPort", DefaultReadServerPort)
	if response, err := send(request, serverPort); err != nil {
		logging.LogError(fmt.Sprintf("[Reader %d] The request could not be processed", readerId), err)
		return nil
	} else {
		r := response.(*message.ReadBlocksInMinuteResponse)
		// Notify the amount of blocks found.
		minuteString := time.Unix(r.Timestamp(), 0)
		logging.Log(fmt.Sprintf("[Reader %d] Found %d blocks for minute %s", readerId, r.BlockCount(), minuteString))
		// Iterate through blocks and write content.
		blocks := r.Blocks()

		for i := 0; i < int(r.BlockCount()); i++ {
			currentBlock := blocks[i]
			logging.Log(fmt.Sprintf("[Reader %d] Found block %s", readerId, currentBlock.Hash().Hex()))
			for it := currentBlock.Entries(); it.HasNext(); it.Advance() {
				chunk := it.Chunk()
				logging.Log(fmt.Sprintf("[Reader %d] Found entry: %s", readerId, string(chunk.Data)))
			}
		}
		return blocks
	}
}

func parseTimestamp(unixTimestamp string) (time.Time, int64, error) {
	if timestampInt, err := strconv.ParseInt(unixTimestamp, 10, 64); err != nil {
		return time.Now(), 0, err
	} else {
		return time.Unix(timestampInt, 0), timestampInt, err
	}
}

//=================================================================================================
// Network functions.
//-------------------------------------------------------------------------------------------------
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

//=================================================================================================
// Random strings
//-------------------------------------------------------------------------------------------------
const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func randomStringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func randomString(length int) string {
	return randomStringWithCharset(length, charset)
}
