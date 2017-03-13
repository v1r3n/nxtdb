package main

import (
	"log"
	"net"
	"strconv"
	"errors"
	"bufio"
	"io"
	"sync"
)
var mutex = &sync.Mutex{}
type store struct {
	data map[string][]byte
}
type command struct {
	cmd string
	key string
	args [][]byte
}

func (s *store) set(key string, value []byte) {
	s.data[key] = value
}

func (s store) get(key string) []byte {
	return s.data[key]
}

func (cmd command) execute(s *store) ([]byte, error) {
	if cmd.cmd == "set" {
		if len(cmd.args) < 1 {
			return nil, errors.New("Missing value for Set")
		}
		mutex.Lock()
		s.set(cmd.key, cmd.args[0])
		mutex.Unlock()
		return []byte("OK"), nil
	}else if cmd.cmd == "get" {
		mutex.Lock()
		val := s.get(cmd.key)
		mutex.Unlock()
		return val, nil
	}
	return nil, errors.New("Unsupported command")
}

func parse(reader *bufio.Reader) (command, error) {
	cmd := command{}
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
				cmd.args = args
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
				cmd.cmd = string(p[:n])
			} else if a == 1 {
				cmd.key = string(p[:n])
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

func handle(conn net.Conn, s store) {
	defer conn.Close()
	for {
		cmd, err := parse(bufio.NewReader(conn))
		if err == nil || err == io.EOF {

			response, err := cmd.execute(&s)
			resp := "+" + string(response) + "\r\n"
			if err != nil {
				resp = "-" + err.Error() + "\r\n"
			}
			if _, err := conn.Write([]byte(resp)); nil != err {
				//log.Println(conn.RemoteAddr(), err)
				return
			}

		} else {
			//log.Println("Error2", err)
		}

	}
}
func main() {


	s := store{
		data : make(map[string][]byte),
	}

	listener, err := net.Listen("tcp", ":22122")
	if nil != err {
		log.Fatalln("Listen Error (port already taken?): ", err.Error())
	}
	log.Println("Starting the REDIS server in go")

	for {
		conn, err := listener.Accept()
		if nil != err {
			log.Fatalln("super bad", err.Error())
		}
		go handle(conn, s)
	}

}
