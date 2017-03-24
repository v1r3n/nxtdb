package tests

import (
	"log"
	"github.com/Pallinder/go-randomdata"
	"nxtdb/graph"
	"github.com/google/uuid"
	rg "nxtdb/graph/rocksdb"
	"testing"
	"strings"
)

func BenchmarkGraphOps(b *testing.B) {
	//testGraphOps(t, 1000)
}

func TestGraphOps(t *testing.T) {
	testGraphOps(t, 10)
}

func testGraphOps(t *testing.T, count int) {

	g := rg.OpenGraphDb("./graph.db")
	defer g.Close()

	tx := g.Tx()
	countries := make([]string, 0)

	for i := 0; i < count; i++ {
		country := randomdata.Country(randomdata.FullCountry)
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
		foundLabel := tx.GetLabel(country)
		if foundLabel == nil || !strings.EqualFold(foundLabel.Name(), vtxLabel.Name()) {
			t.Error("Labels do not match", vtxLabel.Name(), "not same as", foundLabel.Name())
		}

		id := tx.Add(vtxLabel, properties...)
		if len(id) == 0 {
			t.Error("ID returned for Add... is not valid/empty string")
		}
		log.Println("Adding country", country)
		countries = append(countries, country)
		tx.AddProperty(id, "bio", []byte(randomdata.Paragraph()))
	}
	tx.Commit()
	for indx, country := range countries {
		log.Println("\t\tTesting for country22", country, "indx", indx)
	}
	for indx, country := range countries {
		foundLabel := tx.GetLabel(country)
		log.Println("Testing for country", country, "indx", indx)
		if foundLabel == nil || !strings.EqualFold(foundLabel.Name(), country) {
			t.Error("Labels do not match", country, "not same as", foundLabel.Name())
		}

		iterator := tx.GetVerticesByLabel(tx.GetLabel(country))
		if iterator == nil {
			t.Fail()
		}
		for {
			if !iterator.HasNext() {
				break;
			}
			vtx := iterator.Next()
			if vtx == nil {
				t.Error("nil vertex in iterator")
			}
			log.Println(string(vtx.Property("country")), vtx.Id(), vtx.Label())
			if !strings.EqualFold(country, vtx.Label().Name()) {
				t.Error("received vertex that does not belong to the label")
			}

		}
	}

}