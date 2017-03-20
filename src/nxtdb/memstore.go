package nxtdb

import (
	"errors"
	"sync"
	"os"
	"log"
)

var mutex = &sync.RWMutex{}

type MemStore struct {
	data map[string][]byte
	hashes map[string]map[string][]byte
}

func NewStore() Store {
	store := MemStore{
		data : make(map[string][]byte),
		hashes : make(map[string]map[string][]byte),
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

func (s *MemStore) hset(key string, field string, value []byte) {
	if(s.hashes[key] == nil) {
		s.hashes[key] = make(map[string][]byte)
	}
	s.hashes[key][field] = value
}

func (s MemStore) get(key string) []byte {
	return s.data[key]
}

func (s MemStore) hget(key string, field string) []byte {
	return s.hashes[key][field]
}

func (s MemStore) hgetall(key string) [][]byte {
	fields := s.hashes[key]
	if fields == nil {
		return make([][]byte, 0)
	}
	length := len(fields)
	values := make([][]byte, length * 2)
	i := 0
	for k, v := range fields {
		values[i] = []byte(k)
		values[i+1] = v
		i += 2
	}
	return values
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
		length := len(store.data)
		length += len(store.hashes)
		keys := make([][]byte, length)
		i := 0
		for key := range store.data {
			keys[i] = []byte(key)
			i++
		}
		for key, _ := range store.hashes {
			keys[i] = []byte(key)
			i++
		}
		return keys, nil
	} else if cmd.Cmd == "hset" {
		if len(cmd.Args) < 2 {
			return nil, errors.New("hset key field value")
		}
		mutex.Lock()
		store.hset(cmd.Key, string(cmd.Args[0]), cmd.Args[1])
		mutex.Unlock()
		bytes := make([][]byte, 1)
		bytes[0] = []byte("OK")
		return bytes, nil
	} else if cmd.Cmd == "hget" {
		if len(cmd.Args) < 1 {
			return nil, errors.New("hget key field")
		}
		mutex.RLock()
		val := store.hget(cmd.Key, string(cmd.Args[0]))
		mutex.RUnlock()
		bytes := make([][]byte, 1)
		bytes[0] = val
		return bytes, nil
	} else if cmd.Cmd == "hgetall" {
		mutex.RLock()
		val := store.hgetall(cmd.Key)
		mutex.RUnlock()
		return val, nil
	} else if cmd.Cmd == "save" {
		mutex.Lock()
		saveToDisk(store)
		mutex.Unlock()
	}

	return nil, errors.New("Unsupported command")
}

func saveToDisk(store *MemStore) {
	f, err := os.OpenFile("/Users/viren/test.file", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		log.Println(err.Error())
		return
	}
	defer f.Close()

	for key, value := range store.data {
		f.Write([]byte(key))
		f.Write()
		f.Write(value)

	}
	for key, value := range store.hashes {
		f.Write([]byte(key))
		for k1, v1 := range value {
			f.Write([]byte(k1))
			f.Write(v1)
		}
	}
	f.Sync()
}