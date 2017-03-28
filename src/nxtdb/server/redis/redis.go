package redis

import (
	"strconv"
	"bufio"
	"net"
	"log"
	"io"
	server "nxtdb/server"
	"strings"
	"github.com/google/uuid"
)

type RedisCommandParser struct {

}

type RedisServer struct {

}

func NewRedisCmdParser() server.CommandParser {
	return RedisCommandParser{}
}

func NewServer() server.Server {
	return RedisServer{}
}

func (r RedisServer) Start(host string, port int, store *server.Store) {
	listener, err := net.Listen("tcp", host + ":" + strconv.Itoa(port))
	if nil != err {
		log.Fatalln("Listen Error:", err.Error())
	}
	log.Println("Starting the Redis server in go")

	for {
		conn, err := listener.Accept()
		if nil != err {
			log.Fatalln("super bad", err.Error())
		}
		go handle(conn, store)
	}
}

func (r RedisServer) Stop() {

}

func (r RedisCommandParser) ParseCommand(reader *bufio.Reader) (server.Command, error) {
	return parseCommand(reader)
}

func parseCommand(reader *bufio.Reader) (server.Command, error) {
	cmd := server.Command{}
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
			if argc > 1 {
				args = make([][]byte, argc - 1)
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
				cmd.Cmd = strings.ToUpper(string(p[:n]))
			} else {
				args[a-1] = make([]byte, n)
				args[a-1] = p[:n]
			}
			a++;
			state = 1
			argc_read++
			reader.ReadBytes('\n')
		}

	}

	return cmd, nil
}

func handle(conn net.Conn, store *server.Store) {
	defer conn.Close()
	sessionId := uuid.New().String()
	cmdParser := NewRedisCmdParser()

	for {
		cmd, err := cmdParser.ParseCommand(bufio.NewReader(conn))
		cmd.SessionId = sessionId
		if err == nil || err == io.EOF {
			response, err := (*store).ExecuteCommand(&cmd)
			resp := ""
			if err != nil {
				resp = "-" + err.Error() + "\r\n"
			} else {
				length := len(response)
				if length == 0 {
					resp = "+\r\n"
				} else if length == 1 {
					val := string(response[0])
					resp = "$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"

				} else {

					resp = "*" + strconv.Itoa(length) + "\r\n"
					for i := 0; i < length; i++ {
						val := string(response[i])
						resp += "$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"
					}
				}
			}
			if _, err := conn.Write([]byte(resp)); nil != err {
				return
			}

		}
	}
}