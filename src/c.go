package main

/*
#cgo LDFLAGS: -L/usr/local/Cellar/rocksdb/5.1.4/lib -lrocksdb
#cgo CFLAGS: -I/usr/local/Cellar/rocksdb/5.1.4/include
#include "rocksdb/c.h"
#include <stdlib.h>
*/
import (
	"fmt"
	grocks "github.com/v1r3n/gorocksdb"
	"github.com/google/uuid"
	randomdata "github.com/Pallinder/go-randomdata"
	"strconv"
	rocksgraph "nxtdb/store/rocksdb"
	"nxtdb/graph"
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

	countryLabel := gdb.AddLabel("country")
	testLabel := gdb.AddLabel("test")
	belongsTo := gdb.AddLabel("belongsTo")

	country := gdb.Add(countryLabel, rocksgraph.Property("country", []byte(randomdata.Country(randomdata.FullCountry))))

	for i := 0; i < count; i++ {

		properties := []graph.Property {
			rocksgraph.Property("first", []byte(randomdata.FirstName(randomdata.RandomGender))),
			rocksgraph.Property("last", []byte(randomdata.LastName())),
			rocksgraph.Property("address", []byte(randomdata.Address())),
			rocksgraph.Property("email", []byte(randomdata.Email())),
			rocksgraph.Property("currency", []byte(randomdata.Currency())),
			rocksgraph.Property("macaddress", []byte(randomdata.MacAddress())),
			rocksgraph.Property("uid", []byte(uuid.New().String())),
		}


		start := time.Now()
		id := gdb.Add(testLabel, properties...)
		gdb.AddProperty(id, "bio", []byte(randomdata.Paragraph()))
		diff := time.Now().Sub(start)
		writeTime += diff.Nanoseconds()

		gdb.AddEdge(country, id, belongsTo)
		start1 := time.Now()
		gdb.GetVertex(id)
		diff2 := time.Now().Sub(start1)
		readTime += diff2.Nanoseconds()


	}

	start2 := time.Now()
	iterator := gdb.GetVertices(country, belongsTo, true)
	start3 := time.Now()
	for {
		if !iterator.HasNext() {
			break;
		}
		vtx := iterator.Next()
		log.Println("\t-->next,", string(vtx.Property("email")))
	}
	start4 := time.Now()

	log.Println("\n\nEdge Lookup time", start3.Sub(start2).Nanoseconds())
	log.Println("\n\nEdge iteration time", start4.Sub(start3).Nanoseconds())
	log.Println("graph write time", writeTime)
	log.Println("graph read time", readTime)
	log.Println("graph time", writeTime + readTime)

	defer gdb.Close()
}

func main1() {

	options := grocks.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	fmt.Println("otpions", options)
	db, err := grocks.OpenDb(options, "/Users/viren/workspace/github/gone/tmp.db")
	if err != nil {
		fmt.Println("Problem opening database", err)
		return
	}
	fmt.Println("db", db)

	woptions := grocks.NewDefaultWriteOptions()
	roptions := grocks.NewDefaultReadOptions()

	batch := grocks.NewWriteBatch()

	label := "knows"
	from := uuid.New().String();
	key := from + "_" + label + "0"
	for i := 0; i < 100000; i++ {
		to := uuid.New().String();
		batch.Put([]byte(key + ":" + to), []byte(to))
	}
	batcherr := db.Write(woptions, batch)
	if batcherr != nil {
		fmt.Print("Error executing the batch")
		return
	}

	iterator := db.NewIterator(roptions)
	defer iterator.Close()

	for iterator.Seek([]byte(key)); iterator.ValidForPrefix([]byte(key)); iterator.Next() {
		key := make([]byte, iterator.Key().Size())
		copy(key, iterator.Key().Data())
		value := make([]byte, iterator.Value().Size())
		copy(value, iterator.Value().Data())
		fmt.Println("key:", string(key), "value:", string(value))
	}
}




