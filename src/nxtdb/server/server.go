package server

import (
	"bufio"
)

type Command struct {
	Cmd string
	Key string
	Args [][]byte
}

type Store interface {
	ExecuteCommand(cmd Command) ([][]byte, error)
}

type CommandParser interface {
	ParseCommand(reader *bufio.Reader) (Command, error)
}

type Server interface {
	Start(host string, port int, store *Store)
	Stop()
}