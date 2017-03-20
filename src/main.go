package main

import (
	redis "./nxtdb/redis"
	nxtdb "./nxtdb"
)

func main() {
	server := redis.NewServer()
	store := nxtdb.NewStore()
	server.Start("", 22122, &store)
}