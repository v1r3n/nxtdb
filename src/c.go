package main

/*
#cgo LDFLAGS: -L/usr/local/Cellar/rocksdb/5.1.4/lib -lrocksdb
#cgo CFLAGS: -I/usr/local/Cellar/rocksdb/5.1.4/include
#include "rocksdb/c.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	grocks "./github.com/v1r3n/gorocksdb"
	uuid "./github.com/google/uuid"
	randomdata "./github.com/Pallinder/go-randomdata"
	"strings"
	"strconv"
	rocksgraph "./nxtdb/rocksdb"
	graph "./nxtdb/graph"
	"time"
	"log"
	"os"
)


func main() {
	count := 1
	if len(os.Args) > 1 {
		c, errx := strconv.Atoi(os.Args[1])
		if errx != nil {
			log.Fatalln("expected number", errx.Error())
		}
		count = c
	}
	log.Println("loop count", count)
	gdb := rocksgraph.NewGraph("./graph.db")
	gdb.Open()
	var writeTime int64 = 0
	var readTime int64 = 0

	for i := 0; i < count; i++ {

		properties := []graph.VertexProperty {
			rocksgraph.Property("first", []byte(randomdata.FirstName(randomdata.RandomGender))),
			rocksgraph.Property("last", []byte(randomdata.LastName())),
			rocksgraph.Property("address", []byte(randomdata.Address())),
			rocksgraph.Property("email", []byte(randomdata.Email())),
			rocksgraph.Property("currency", []byte(randomdata.Currency())),
			rocksgraph.Property("macaddress", []byte(randomdata.MacAddress())),
			rocksgraph.Property("uid", []byte(uuid.New().String())),
		}


		start := time.Now()
		id := gdb.Add("test_label", properties...)
		gdb.AddProperty(id, "bio", []byte(randomdata.Paragraph()))
		diff := time.Now().Sub(start)
		writeTime += diff.Nanoseconds()

		start1 := time.Now()
		gdb.GetVertex(id)
		diff2 := time.Now().Sub(start1)
		readTime += diff2.Nanoseconds()


	}

	log.Println("graph write time", writeTime)
	log.Println("graph read time", readTime)
	log.Println("graph time", writeTime + readTime)

	defer gdb.Close()
}

func main22() {
	fmt.Println("Hello World")
	options := grocks.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	fmt.Println("otpions", options)
	db, err := grocks.OpenDb(options, "/Users/viren/workspace/github/gone/tmp.db")
	if err != nil {
		fmt.Println("Problem opening database", err)
		return
	}
	fmt.Println("db", db)

	putKey := []byte("name")
	putValue := []byte("viren")
	woptions := grocks.NewDefaultWriteOptions()
	roptions := grocks.NewDefaultReadOptions()

	existing, rerr := db.Get(roptions, putKey)
	if rerr != nil {
		fmt.Println("Cannot read the value", rerr)
	} else {
		fmt.Println("Existing value is", existing)
	}

	puterr := db.Put(woptions, []byte("nam"), putValue)
	puterr = db.Put(woptions, []byte("namely"), putValue)
	puterr = db.Put(woptions, putKey, putValue)
	puterr = db.Put(woptions, []byte("zzz"), putValue)
	puterr = db.Put(woptions, []byte("aaa"), putValue)
	if puterr != nil {
		fmt.Println("failed to write the key", puterr)
	} else {
		fmt.Println("Updated successfully!")
	}
	batch := grocks.NewWriteBatch()

	for i := 0; i < 10; i++ {
		id := uuid.New();
		fmt.Println(id)
		key := "xid_name__" + id.String()
		value := "value of " + strconv.Itoa(i)
		batch.Put([]byte(key), []byte(value))
	}
	batcherr := db.Write(woptions, batch)
	if batcherr != nil {
		fmt.Print("Error executing the batch")
		return
	}

	iterator := db.NewIterator(roptions)
	defer iterator.Close()
	mykey := "xid_name__"
	fmt.Println("Attempting to print all the key/values starting from name")
	for iterator.Seek([]byte(mykey)); iterator.Valid(); iterator.Next() {
		key := make([]byte, iterator.Key().Size())
		copy(key, iterator.Key().Data())
		if !strings.HasPrefix(string(key), mykey) {
			break
		}
		value := make([]byte, iterator.Value().Size())
		copy(value, iterator.Value().Data())
		fmt.Println("key:", string(key), "value:", string(value))
	}
}




