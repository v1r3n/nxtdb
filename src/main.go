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
	graphstore "nxtdb/server"
	"encoding/json"
)

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds | log.LstdFlags | log.LUTC)
}

func main_json() {
	var g = rocksgraph.OpenGraphDb("./graph.db")
	var properties = []graph.Property{
		g.NewProperty("key1", []byte("value1")),
		g.NewProperty("key2", []byte("value2")),
	}
	log.Println(properties)
	bytes, err := json.Marshal(properties)
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Println(string(bytes))

}

func main() {
	server := redis.NewServer()
	store := graphstore.NewGraphStore("./graph2.db")
	server.Start("", 22122, &store)
}
func main3() {
	var g = rocksgraph.OpenGraphDb("./graph.db")

	for i :=0; i < 10; i++ {
		go main()
	}
	select {

	}
	defer g.Close()
}
func main_g() {

	log.SetFlags(log.Lshortfile | log.Lmicroseconds | log.LstdFlags | log.LUTC)

	var g = rocksgraph.OpenGraphDb("./graph.db")
	var count int64 = 1
	if len(os.Args) > 1 {
		c, errx := strconv.Atoi(os.Args[1])
		if errx != nil {
			log.Fatalln("expected number", errx.Error())
		}
		count = int64(c)
	}
	log.Println("loop count", count)


	var country string
	var i int64 = 0
	tx := g.Tx()
	for ; i < count; i++ {
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
		vtx := tx.Add(vtxLabel, properties...)
		country = string(country)
		vtx.SetProperty("bio", []byte(randomdata.Paragraph()))
	}
	start := time.Now()
	tx.Commit()
	end := time.Now()

	txTime := end.Sub(start).Nanoseconds()
	log.Println("Tx Commit:\t", txTime)
	log.Println(":Time per entity\t", (txTime/count))

	//foundLabel2 := tx.GetLabel(country)
	//log.Println("found again label", foundLabel2)

	start2 := time.Now()
	iterator := tx.GetVerticesByLabel(tx.GetLabel(country))
	end2 := time.Now()
	log.Println("Get Iter :\t", end2.Sub(start2).Nanoseconds(), "country", country)

	start3 := time.Now()
	foundCount := 0
	if iterator != nil {
		for {
			if !iterator.HasNext() {
				break;
			}
			iterator.Next()
			foundCount++
			//log.Println(string(vtx.Property("country")), vtx.Id())
		}
	}
	end3 := time.Now()
	log.Println("Edge Iter:\t", end3.Sub(start3).Nanoseconds(), "Total vertices", foundCount)
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




