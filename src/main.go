package main

import (
	"./nxtdb/redis"
	"./nxtdb"
)

func main2() {
	server := redis.NewServer()
	store := nxtdb.NewStore()
	server.Start("", 22122, &store)
}