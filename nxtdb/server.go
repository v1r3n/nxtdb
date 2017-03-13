/**
 Entry point for the nxtdb
 */

package nxtdb

import (
	"bufio"
)

type Command struct {
	Cmd string
	Key string
	Args [][]byte
}

type Operations interface {
	Get(key string) []byte
	Set(key string, value []byte) []byte
	Hset(key string, field string, value []byte) []byte
	Hget(key string, field string) []byte
}

type Store interface {
	ExecuteCommand(cmd Command) ([][]byte, error)
}

type CommandParser interface {
	ParseCommand(reader *bufio.Reader) (Command, error)
}