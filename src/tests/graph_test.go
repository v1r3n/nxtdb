package tests

import (
	"log"
	"github.com/Pallinder/go-randomdata"
	"nxtdb/graph"
	"github.com/google/uuid"
	rg "nxtdb/graph/rocksdb"
	"testing"
	"strings"
	"strconv"
)

var g = rg.OpenGraphDb("./graph.db")

type TB interface {
	Error(args ...interface{})
	FailNow()
}

func BenchmarkGraphOps(b *testing.B) {
	testGraphOps(b, 100)
}

func TestGraphOps(t *testing.T) {
	testGraphOps(t, 20)
}

func testGraphOps(t TB, count int) {

	tx := g.Tx()
	countries := make([]string, 0)
	log.Println("count", strconv.Itoa(count))
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
			t.FailNow()
		}

		id := tx.Add(vtxLabel, properties...)
		if len(id) == 0 {
			t.Error("ID returned for Add... is not valid/empty string")
			t.FailNow()
		}
		countries = append(countries, country)
		tx.AddProperty(id, "bio", []byte(randomdata.Paragraph()))
	}
	tx.Commit()
	for _, country := range countries {
		foundLabel := tx.GetLabel(country)
		if foundLabel == nil || !strings.EqualFold(foundLabel.Name(), country) {
			t.Error("Labels do not match", country, "not same as", foundLabel.Name())
			t.FailNow()
		}

		iterator := tx.GetVerticesByLabel(tx.GetLabel(country))
		if iterator == nil {
			t.FailNow()
		}
		for {
			if !iterator.HasNext() {
				break;
			}
			vtx := iterator.Next()
			if vtx == nil {
				t.Error("nil vertex in iterator")
				t.FailNow()
			}
			if vtx.Label() == nil {
				t.Error("Label is missing on the vertex, expected", string(vtx.Property("country")))
				t.FailNow()
			}
			//log.Println(string(vtx.Property("country")), vtx.Id(), vtx.Label())
			if !strings.EqualFold(country, vtx.Label().Name()) {
				t.Error("received vertex that does not belong to the label", country, vtx.Label().Name())
				t.FailNow()
			}

		}
	}

}