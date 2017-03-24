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

	g := rocksgraph.OpenGraphDb("./graph.db")
	gdb := g.Tx()

	var country string
	for i := 0; i < count; i++ {
		country = "." + randomdata.Country(randomdata.FullCountry)
		properties := []graph.Property {
			rocksgraph.NewProperty("first", []byte(randomdata.FirstName(randomdata.RandomGender))),
			rocksgraph.NewProperty("last", []byte(randomdata.LastName())),
			rocksgraph.NewProperty("address", []byte(randomdata.Address())),
			rocksgraph.NewProperty("email", []byte(randomdata.Email())),
			rocksgraph.NewProperty("currency", []byte(randomdata.Currency())),
			rocksgraph.NewProperty("macaddress", []byte(randomdata.MacAddress())),
			rocksgraph.NewProperty("uid", []byte(uuid.New().String())),
			rocksgraph.NewProperty("country", []byte(country)),
		}
		vtxLabel := gdb.AddLabel(country)
		id := gdb.Add(vtxLabel, properties...)
		country = string(country)
		gdb.AddProperty(id, "bio", []byte(randomdata.Paragraph()))
	}
	start := time.Now()
	gdb.Commit()
	end := time.Now()
	log.Println("\n\nTx Commit Time", end.Sub(start).Nanoseconds())

	foundLabel2 := gdb.GetLabel(country)
	log.Println("found again label", foundLabel2)

	start2 := time.Now()
	iterator := gdb.GetVerticesByLabel(gdb.GetLabel(country))
	end2 := time.Now()
	log.Println("\n\nGet Iterator time", end2.Sub(start2).Nanoseconds())

	start3 := time.Now()
	log.Println("START")
	if iterator != nil {
		for {
			if !iterator.HasNext() {
				break;
			}
			vtx := iterator.Next()
			log.Println(string(vtx.Property("country")), vtx.Id())
		}
	}
	log.Println("END")
	end3 := time.Now()
	log.Println("\n\nEdge iteration time", end3.Sub(start3).Nanoseconds())

	defer g.Close()
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




