package main

import (
	redis "./nxtdb/redis"
	nxtdb "./nxtdb"
)

func main2() {
	server := redis.NewServer()
	store := nxtdb.NewStore()
	server.Start("", 22122, &store)
}