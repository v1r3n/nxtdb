package redis

import (
	"strconv"
	"bufio"
	nxtdb  "../../nxtdb"
)

type Redis struct {

}
func NewRedis() nxtdb.CommandParser {
	return Redis{}
}

func (r Redis) ParseCommand(reader *bufio.Reader) (nxtdb.Command, error) {
	return parseCommand(reader)
}

func parseCommand(reader *bufio.Reader) (nxtdb.Command, error) {
	cmd := nxtdb.Command{}
	state := 0	//0 -> read the argument count, 1-> argument size, 2-> argument
	argc := 0
	size := 0
	argc_read := -1
	var args [][]byte
	a := 0

	for argc_read < argc {
		if state == 0 {
			bytes, err := reader.ReadBytes('\r')
			if err != nil {
				return cmd, err
			}

			val, _ := strconv.Atoi(string(bytes[1:len(bytes)-1]))
			argc = val
			argc_read = 0
			state++
			if argc > 2 {
				args = make([][]byte, argc - 2)
				cmd.Args = args
			}
			reader.ReadBytes('\n')
		} else if state == 1 {
			bytes, err := reader.ReadBytes('\r')
			if err != nil {
				return cmd, err
			}
			z, _ := strconv.Atoi(string(bytes[1:len(bytes)-1]))
			size = z
			state++
			reader.ReadBytes('\n')
		} else if state == 2 {
			p := make([]byte, size)
			n, err := reader.Read(p)
			if err != nil {
				return cmd, err
			}
			if a == 0 {
				cmd.Cmd = string(p[:n])
			} else if a == 1 {
				cmd.Key = string(p[:n])
			} else {
				args[a-2] = make([]byte, n)
				args[a-2] = p[:n]
			}
			a++;
			state = 1
			argc_read++
			reader.ReadBytes('\n')
		}

	}

	return cmd, nil
}