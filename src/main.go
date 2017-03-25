package main

import (
	"fmt"
	grocks "github.com/v1r3n/gorocksdb"
	"github.com/google/uuid"
	randomdata "github.com/Pallinder/go-randomdata"
	"strconv"
	rocksgraph "nxtdb/graph/rocksdb"
	"nxtdb/graph"
	"time"
	"log"
	"os"
	"nxtdb/server/redis"
	"nxtdb"
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
	tx := g.Tx()

	var country string
	for i := 0; i < count; i++ {
		country = "." + randomdata.Country(randomdata.FullCountry)
		properties := []graph.Property {
			g.NewProperty("first", []byte(randomdata.FirstName(randomdata.RandomGender))),
			g.NewProperty("last", []byte(randomdata.LastName())),
			g.NewProperty("address", []byte(randomdata.Address())),
			g.NewProperty("email", []byte(randomdata.Email())),
			g.NewProperty("currency", []byte(randomdata.Currency())),
			g.NewProperty("macaddress", []byte(randomdata.MacAddress())),
			g.NewProperty("uid", []byte(uuid.New().String())),
			g.NewProperty("country", []byte(country)),
		}
		vtxLabel := tx.AddLabel(country)
		id := tx.Add(vtxLabel, properties...)
		country = string(country)
		tx.AddProperty(id, "bio", []byte(randomdata.Paragraph()))
	}
	start := time.Now()
	tx.Commit()

	end := time.Now()
	log.Println("\n\nTx Commit Time", end.Sub(start).Nanoseconds())

	foundLabel2 := tx.GetLabel(country)
	log.Println("found again label", foundLabel2)

	start2 := time.Now()
	iterator := tx.GetVerticesByLabel(tx.GetLabel(country))
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

func main2() {
	server := redis.NewServer()
	store := nxtdb.NewStore()
	server.Start("", 22122, &store)
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




