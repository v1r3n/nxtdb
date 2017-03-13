package main

import (
	redis "./nxtdb/redis"
	nxtdb "./nxtdb"
	"log"
	"net"
	"bufio"
	"io"
)

func main() {


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
		go handle(conn)
	}

}

func handle(conn net.Conn) {
	defer conn.Close()

	cmdParser := redis.NewRedis()
	store := nxtdb.NewStore()

	for {
		cmd, err := cmdParser.ParseCommand(bufio.NewReader(conn))
		if err == nil || err == io.EOF {
			response, err := store.ExecuteCommand(cmd)
			resp := ""
			if err != nil {
				resp = "-" + err.Error() + "\r\n"
			} else {
				resp = "+" + string(response[0]) + "\r\n"
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