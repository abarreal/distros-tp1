package synchro

import (
	"os"
	"sync"
)

//=================================================================================================
// File Management
//-------------------------------------------------------------------------------------------------

// Use a map of locks instead of flock. Using actual file locks requires using cgo and for the
// moment that is not needed. If file locks are required, the implementation here can be changed.
type FileLock = sync.Mutex

var filelocksMutex sync.RWMutex = sync.RWMutex{}
var filelocks map[string]*FileLock = make(map[string]*FileLock)

// Open the file in the given path and call the callback, atomically. The file is created
// it it does not exist.
func HandleFileAtomically(filepath string, flags int, callback func(file *os.File) error) error {
	// Get a lock on the file.
	lock := getFileLock(filepath)
	lock.Lock()
	defer lock.Unlock()

	// Open the file, or create it if it does not exist.
	file, err := os.OpenFile(filepath, flags, 0600)

	if err != nil {
		return err
	}

	// Defer closing the file.
	defer file.Close()
	// Call back with the file for the caller to handle.
	err = callback(file)
	// Everything went well, apparently. Return no error.
	return err
}

func getFileLock(filepath string) *FileLock {
	lock := getExistingFileLock(filepath)

	if lock != nil {
		return lock
	} else {
		return createFileLock(filepath)
	}
}

func createFileLock(filepath string) *FileLock {
	// The lock does not exist. Proceed to create it.
	filelocksMutex.Lock()
	defer filelocksMutex.Unlock()

	// Ensure that the lock does not exist.
	lock, found := filelocks[filepath]

	if found {
		return lock
	} else {
		filelock := &FileLock{}
		filelocks[filepath] = filelock
		return filelock
	}
}

func getExistingFileLock(filepath string) *FileLock {
	// Get a lock on the map of locks.
	filelocksMutex.RLock()
	defer filelocksMutex.RUnlock()

	// Get the lock if it is in fact there.
	lock, found := filelocks[filepath]

	if found {
		return lock
	} else {
		return nil
	}
}
