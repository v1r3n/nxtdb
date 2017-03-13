package nxtdb

import (
	"errors"
	"strconv"
	"sync"
)

var mutex = &sync.RWMutex{}

type MemStore struct {
	data map[string][]byte
}

func NewStore() Store {
	store := MemStore{
		data : make(map[string][]byte),
	}
	return store
}

func (store MemStore) ExecuteCommand(cmd Command) ([][]byte, error) {
	bytes, err := execute(cmd, &store)
	return bytes, err
}

func (s *MemStore) set(key string, value []byte) {
	s.data[key] = value
}

func (s MemStore) get(key string) []byte {
	return s.data[key]
}
func execute(cmd Command, store *MemStore) ([][]byte, error) {
	if cmd.Cmd == "set" {
		if len(cmd.Args) < 1 {
			return nil, errors.New("Missing value for Set")
		}
		mutex.Lock()
		store.set(cmd.Key, cmd.Args[0])
		mutex.Unlock()
		bytes := make([][]byte, 1)
		bytes[0] = []byte("OK")
		return bytes, nil
	} else if cmd.Cmd == "get" {
		mutex.RLock()
		val := store.get(cmd.Key)
		mutex.RUnlock()
		bytes := make([][]byte, 1)
		bytes[0] = val
		return bytes, nil
	} else if cmd.Cmd == "keys" {
		count := 0
		for range store.data {
			count++
		}
		bytes := make([][]byte, 1)
		bytes[0] = []byte(strconv.Itoa(count))
		return bytes, nil
	}
	return nil, errors.New("Unsupported command")
}