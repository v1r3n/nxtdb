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
	b.SetParallelism(32)
	b.RunParallel(func (pb *testing.PB) {
		for pb.Next() {
			testGraphOps(b, 5000)
		}
	})

}

func TestGraphOps(t *testing.T) {
	testGraphOps(t, 20)
}

func TestGraphTx(t *testing.T) {
	tx := g.Tx()

	label := tx.AddLabel("test")
	vtx := tx.Add(label, g.NewProperty("key", []byte("value")))
	found := tx.GetVertex(vtx.Id())
	if found == nil {
		t.Fatal("GetVertex after Add returns nil")
	}
	if found.Label() == nil {
		t.Fatal("Missing label on vertex")
	}
	labelName := found.Label().Name()
	if len(labelName) == 0 {
		t.Fatal("Empty label on vertex")
	}
	tx.Commit()

	tx = g.Tx()

	found = tx.GetVertex(vtx.Id())
	if found == nil {
		t.Fatal("GetVertex after Tx Commit returns nil")
	}
	if found.Label() == nil {
		t.Fatal("Missing label on vertex after Tx commit")
	}
	labelName = found.Label().Name()
	if len(labelName) == 0 {
		t.Fatal("Empty label on vertex after Tx commit")
	}

	found = tx.GetVertex("bad id")
	if found != nil {
		t.Fatal("Found a ghost vertex!")
	}
}

func testGraphOps(t TB, count int) {

	countries := make([]string, 0)
	log.Println("count", strconv.Itoa(count))

	tx := g.Tx()

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

		vtx := tx.Add(vtxLabel, properties...)
		if vtx == nil {
			t.Error("nil return for add")
			t.FailNow()
		}
		countries = append(countries, country)
		vtx.SetProperty("bio", []byte(randomdata.Paragraph()))
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
			//log.Println(string(vg.Property("country")), vg.Id(), vg.Label())
			if !strings.EqualFold(country, vtx.Label().Name()) {
				t.Error("received vertex that does not belong to the label", country, vtx.Label().Name())
				t.FailNow()
			}

		}
	}

}