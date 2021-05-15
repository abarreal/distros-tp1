package server

import (
	"fmt"
	"log"
	"net"
	"sync"

	"tp1.aba.distros.fi.uba.ar/common/logging"
)

//=================================================================================================
// Server
//-------------------------------------------------------------------------------------------------
const SignalQuit = 0

type ServerConfig struct {
	Port        uint16
	WorkerCount uint
}

type Server struct {
	Config        *ServerConfig
	Control       chan int
	workerControl [](chan<- int)
	work          func(*net.Conn)
}

func CreateNew(config *ServerConfig, handleConnection func(*net.Conn)) *Server {
	return &Server{
		config,
		make(chan int),
		make([](chan<- int), 0, config.WorkerCount),
		handleConnection,
	}
}

func (server *Server) Stop() {
	server.Control <- SignalQuit
}

func (server *Server) Run() error {
	serverLog(fmt.Sprintf("Starting on port %d", server.Config.Port))

	// Instantiate a wait group to wait for all goroutines to finish on quit.
	waitGroup := &sync.WaitGroup{}

	// Instantiate a TCP listener on the given port.
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Config.Port))
	// Return any error that may have occurred.
	if err != nil {
		return err
	}

	// Launch the acceptor goroutine to accept new connections on the listener.
	// The acceptor will handle closing the listener when quit is requested.
	acc := createAcceptor(&ln, waitGroup)
	acc.run()

	// Instantiate a fixed, given amount of worker goroutines.
	serverLog(fmt.Sprintf("Launching %d workers", server.Config.WorkerCount))
	for i := uint(0); i < server.Config.WorkerCount; i++ {
		controlChannel := launchWorker(i, acc.connectionQueue, waitGroup, server.work)
		server.workerControl = append(server.workerControl, controlChannel)
	}

	// Define a local finalization flag.
	done := false

	for !done {
		// Wait for a control signal.
		signal := <-server.Control

		// Handle the concrete signal. There is one single signal type
		// currently implemented.
		switch signal {
		case SignalQuit:
			serverLog("Quit signal received")
			// Finalize the acceptor.
			serverLog("Closing listener")
			acc.quit()
			// Propagate the signal to the workers.
			serverLog("Finalizing workers")
			for _, controlChannel := range server.workerControl {
				controlChannel <- signal
			}
			// Set done to true; we are not taking any more control signals.
			done = true
		}
	}

	// Wait for all goroutines to finish and exit.
	serverLog("Waiting for goroutines to finish before quitting")
	waitGroup.Wait()
	serverLog("Quitting now")
	return nil
}

func serverLog(msg string) {
	log.Println(serverMessage(msg))
}

func serverMessage(msg string) string {
	return fmt.Sprintf("[Server] %s", msg)
}

//=================================================================================================
// Worker
//-------------------------------------------------------------------------------------------------
// Launches a worker that handles a given connection in a separate goroutine.
// Returns a control channel to pass control signals to the worker.
func launchWorker(id uint, connQueue <-chan *net.Conn, wg *sync.WaitGroup, work func(*net.Conn)) chan<- int {
	// Increase the worker count by one.
	wg.Add(1)
	// Instantiate a control channel.
	control := make(chan int)

	// Launch the worker goroutine.
	go func() {
		for {
			workerLog(id, "Ready")

			select {
			case <-control:
				// There is only a quit signal currently implemented.
				// Just finish in all cases.
				workerLog(id, "Quit signal received, proceeding to finalize")
				wg.Done()
				return

			case conn := <-connQueue:
				// Have the connection be handled by the worker function.
				workerLog(id, "Handling incoming connection")
				work(conn)
				// Ensure that the connection is finally closed.
				(*conn).Close()
			}
		}
	}()

	return control
}

func workerLog(id uint, msg string) {
	log.Println(workerMessage(id, msg))
}

func workerMessage(id uint, msg string) string {
	return fmt.Sprintf("[Worker](%d) %s", id, msg)
}

//=================================================================================================
// Acceptor
//-------------------------------------------------------------------------------------------------
type acceptor struct {
	connectionQueueWrite chan<- *net.Conn
	connectionQueue      <-chan *net.Conn
	quitRequested        bool
	quitLock             sync.Mutex
	waitGroup            *sync.WaitGroup
	listener             *net.Listener
}

func createAcceptor(ln *net.Listener, wg *sync.WaitGroup) *acceptor {
	connectionQueue := make(chan *net.Conn)
	return &acceptor{
		connectionQueueWrite: connectionQueue,
		connectionQueue:      connectionQueue,
		quitRequested:        false,
		waitGroup:            wg,
		listener:             ln,
	}
}

func (acc *acceptor) run() {
	// Increase wait group count by one for to wait for this routine to finish
	// before quitting.
	acc.waitGroup.Add(1)

	go func() {
		for {
			// Accept an incoming connection.
			serverLog("Accepting new connections")
			conn, err := (*acc.listener).Accept()

			if err != nil {
				// There was an error while accepting new connections. Check if
				// the quit signal was sent; if it was not, then it is in fact an error.
				if acc.wasQuitRequested() {
					// The error should be due to the socket being closed intentionally.
					// We finish here and return.
					acc.waitGroup.Done()
					return
				} else {
					// There was an actual error.
					logging.LogError("Connection error", err)
				}
			}

			// Push the connection into the queue for a worker to handle.
			serverLog("New connection received, pushing into the work queue")
			acc.connectionQueueWrite <- &conn
		}
	}()
}

func (acc *acceptor) wasQuitRequested() bool {
	acc.quitLock.Lock()
	defer acc.quitLock.Unlock()
	return acc.quitRequested
}

func (acc *acceptor) quit() {
	acc.quitLock.Lock()
	acc.quitRequested = true
	acc.quitLock.Unlock()
	// Close the listener as well. The listener is thread safe.
	(*acc.listener).Close()
}
