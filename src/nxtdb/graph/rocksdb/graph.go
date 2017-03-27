package rocksdb

/*
#cgo LDFLAGS: -L/usr/local/Cellar/rocksdb/5.1.4/lib -lrocksdb
#cgo CFLAGS: -I/usr/local/Cellar/rocksdb/5.1.4/include
#include "rocksdb/c.h"
#include <stdlib.h>
*/
import (
	grocks "github.com/v1r3n/gorocksdb"
	"log"
	. "nxtdb/graph"
	"bytes"
	"encoding/binary"
	"fmt"
)

var sharedDB Graph;

type RocksDBGraph struct {
	dbPath       string
	db           *grocks.DB
	cfhVtx       *grocks.ColumnFamilyHandle
	cfhIndx      *grocks.ColumnFamilyHandle
	cfhEdge      *grocks.ColumnFamilyHandle
	indexMeta    []byte
	opened       bool
	transactions *Stack
	currentTx    *GraphTransaction
}

type TransactionLog struct {
	vertices        map[string]Vertex
	deletedVertices []string
	deletedEdges    []GraphEdge
	vtxProperties   map[string]map[string][]byte
	edges           map[string]GraphEdge
	labels          map[string]Label
}

func OpenGraphDb(path string) Graph {

	if sharedDB == nil {
		sharedDB = &RocksDBGraph{
			dbPath: path,
		}
		sharedDB.Open()
	}

	return sharedDB
}

func (graphdb *RocksDBGraph) Open() {

	if graphdb.opened {
		return
	}


	options := grocks.NewDefaultOptions()

	options.SetWriteBufferSize(512)
	options.SetMaxWriteBufferNumber(16)
	options.SetTargetFileSizeBase(256)
	options.SetMaxBackgroundCompactions(48)
	options.SetLevel0SlowdownWritesTrigger(48)
	options.SetLevel0StopWritesTrigger(56)
	options.SetUseDirectWrites(true)

	options.SetCreateIfMissing(true)
	options.SetCreateIfMissingColumnFamilies(true)
	options.SetMergeOperator(PropMergeOp{})
	db, cfh, err := grocks.OpenDbColumnFamilies(options, graphdb.dbPath,
		[]string{"default", "vertex", "edge", "index"},
		[]*grocks.Options{options, options, options, options},
	)
	if err != nil {
		log.Fatal("Cannot open the database", err)
		return
	}

	graphdb.db = db
	graphdb.cfhVtx = cfh[1]
	graphdb.cfhEdge = cfh[2]
	graphdb.cfhIndx = cfh[3]
	graphdb.transactions = &Stack{}
	graphdb.indexMeta = []byte(".IndexedProperties")

	//Start the default transaction
	graphdb.NewTransaction()

	graphdb.opened = true

	return;
}

func (db *RocksDBGraph) Close() {
	db.cfhVtx.Destroy()
	db.cfhEdge.Destroy()
	db.cfhIndx.Destroy()
	db.db.Close()
}

func (db *RocksDBGraph) NewTransaction() {
	tx := NewTransaction(db)
	db.transactions.Push(tx)
	db.currentTx = tx
}

func (db *RocksDBGraph) CommitTransaction() {
	txLog := db.currentTx.GetCommitLog()
	db.flushTx(txLog)
}

func (db *RocksDBGraph) RollbackTransaction() {
	db.currentTx.Rollback()
}

func (db *RocksDBGraph) flushTx(log *TransactionLog) error {

	opts := grocks.NewDefaultWriteOptions()
	batch := grocks.NewWriteBatch()
	db.flushAddLabels(batch, log)
	db.flushAddedVertices(batch, log)
	db.flushProperties(batch, log)
	db.flushEdges(batch, log)

	err := db.db.Write(opts, batch)
	if err != nil {
		fmt.Println("Error writing batch", err.Error())
		return err
	}

	//Clear the current transaction
	db.clearTx()
	return nil
}

func (db *RocksDBGraph) clearTx() {

	//Pop the current tx off the stack
	current := db.transactions.Pop()
	current.init()

	if db.transactions.Len() == 0 {
		//create a default tx
		tx := NewTransaction(db)
		db.transactions.Push(tx)
		db.currentTx = tx
	}
}

func (db *RocksDBGraph) NewProperty(key string, value []byte) Property {
	return GraphProperty{
		Name:   key,
		Val: value,
	}
}

func (db *RocksDBGraph) IndexProperty(property string) {
	opts := grocks.NewDefaultWriteOptions()
	db.db.MergeCF(opts, db.cfhIndx, db.indexMeta, []byte(property))
}

func (db *RocksDBGraph) AddLabel(label string) Label {
	existing := db.GetLabel(label)
	if existing == nil {
		graphLabel := db.currentTx.AddLabel(label)
		return graphLabel
	}
	return existing
}

func (db *RocksDBGraph) GetLabel(label string) Label {
	log.Println("Getting label", label)
	existing := db.currentTx.GetLabel(label)
	log.Println("Getting Label, existing=", existing)
	if existing == nil {
		opts := grocks.NewDefaultReadOptions()
		id, err := db.db.Get(opts, []byte(label))
		log.Println("Got label?", string(id.Data()), "size", id.Size(), "err", err)
		if err != nil {
			log.Println("error in get label", err.Error())
			return nil
		}
		if err != nil || id == nil || id.Size() == 0 {
			return nil
		}
		existing = GraphLabel{
			id:    string(id.Data()),
			label: label,
		}
		db.currentTx.labelMap[existing.Id()] = label
	}
	return existing
}

func (db *RocksDBGraph) Add(label Label, properties ...Property) Vertex {
	vtx := db.currentTx.Add(label, properties...)
	return vtx
}

func (db *RocksDBGraph) AddEdge(from string, to string, label Label) Edge {
	edge := db.currentTx.AddEdge(from, to, label)
	return edge
}

func (db *RocksDBGraph) RemoveEdge(from string, to string, label Label) {
	db.currentTx.RemoveEdge(from, to, label)
}

func (db *RocksDBGraph) SetProperty(id string, key string, value []byte) {
	db.currentTx.SetProperty(id, key, value)
}

func (db *RocksDBGraph) SetProperties(id string, properties ...Property) {
	db.currentTx.SetProperties(id, properties...)
}

func (db *RocksDBGraph) RemoveProperty(id string, key string) {
	db.currentTx.RemoveProperty(id, key)
}

func (db *RocksDBGraph) RemoveProperties(id string, key ...string) {
	db.currentTx.RemoveProperties(id, key...)
}

func (db *RocksDBGraph) RemoveVertex(id string) {
	db.currentTx.RemoveVertex(id)
}

func (db *RocksDBGraph) GetVertex(id string) Vertex {
	existing := db.currentTx.GetVertex(id)
	if existing == nil {
		opts := grocks.NewDefaultReadOptions()
		data, err := db.db.GetCF(opts, db.cfhVtx, []byte(id))
		if err != nil {
			log.Println("error when trying to get vertex", err.Error())
			return nil
		}
		bytes := make([]byte, data.Size())
		copy(bytes, data.Data())
		existing = decode(&bytes, id, db)
	}

	return existing
}

//Get all the vertices with the specified label
func (db *RocksDBGraph) GetVerticesByLabel(vertexLabel Label) VertexIterator {
	if vertexLabel == nil {
		log.Println("Label passed as nil", vertexLabel)
		return nil
	}
	return db.GetVertices(vertexLabel.Id(), vertexLabel, true)
}

//Get the connected vertices from the given vertex
func (db *RocksDBGraph) GetVertices(id string, edgeLabel Label, outgoing bool) VertexIterator {
	buf := new(bytes.Buffer)
	buf.WriteString(id)
	buf.WriteString(edgeLabel.Id())
	buf.WriteByte(0)
	if outgoing {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	opts := grocks.NewDefaultReadOptions()
	iterator := db.db.NewIteratorCF(opts, db.cfhEdge)
	prefix := buf.Bytes()
	vi := NewGraphVertexIterator(prefix, iterator, db)
	return vi
}

func decode(data *[]byte, id string, db *RocksDBGraph) *GraphVertex {
	vtx := GraphVertex{
		id:         []byte(id),
		properties: make(map[string][]byte),
	}

	buf := bytes.NewReader(*data)

	//Read vertex label
	var len uint16
	binary.Read(buf, binary.LittleEndian, &len)

	bytes := make([]byte, len)
	binary.Read(buf, binary.LittleEndian, bytes)
	log.Println("label length is", len, "label=", string(bytes))
	vtx.label = db.getLabelById(string(bytes))

	for ; ; {
		var size uint16
		binary.Read(buf, binary.LittleEndian, &size)
		if size == 0 {
			break;
		}
		key := make([]byte, size)
		binary.Read(buf, binary.LittleEndian, key)

		binary.Read(buf, binary.LittleEndian, &size)
		value := make([]byte, size)
		binary.Read(buf, binary.LittleEndian, value)
		vtx.properties[string(key)] = value
	}
	return &vtx
}

func encode(vertex *Vertex) ([]byte, error) {
	buf := new(bytes.Buffer)
	vtx := *vertex

	log.Println("Adding vertex", vtx.Label().Id())
	labelBytes := []byte(vtx.Label().Id())
	labelSize := uint16(len(labelBytes))

	binary.Write(buf, binary.LittleEndian, labelSize)
	binary.Write(buf, binary.LittleEndian, labelBytes)

	for _, prop := range vtx.Properties() {
		k := prop.Key()
		v := prop.Value()
		keyBytes := []byte(k)
		binary.Write(buf, binary.LittleEndian, uint16(len(keyBytes)))
		binary.Write(buf, binary.LittleEndian, keyBytes)
		binary.Write(buf, binary.LittleEndian, uint16(len(v)))
		binary.Write(buf, binary.LittleEndian, v)
	}
	return buf.Bytes(), nil
}

func encodeKV(key *string, value *[]byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	keyBytes := []byte(*key)
	binary.Write(buf, binary.LittleEndian, uint16(len(keyBytes)))
	binary.Write(buf, binary.LittleEndian, keyBytes)
	binary.Write(buf, binary.LittleEndian, uint16(len(*value)))
	binary.Write(buf, binary.LittleEndian, *value)
	return buf.Bytes(), nil
}

func (db *RocksDBGraph) getLabelById(id string) Label {
	log.Println("getLabelById", id)
	existing := db.currentTx.labelMap[id]
	if len(existing) == 0 {
		opts := grocks.NewDefaultReadOptions()
		id, err := db.db.Get(opts, []byte(id))
		if err != nil {
			log.Println("error in get label by id", err.Error())
			return nil
		}
		if err != nil || id == nil || id.Size() == 0 {
			return nil
		}
		existing = string(id.Data())
	}
	db.currentTx.labelMap[id] = existing
	label := GraphLabel{existing, id}
	db.currentTx.log.labels[existing] = label
	return label
}

//TX Commit to DB
func (db *RocksDBGraph) flushAddedVertices(batch *grocks.WriteBatch, log *TransactionLog) error {
	for id, vtx := range log.vertices {
		bytes, err := encode(&vtx)
		if err != nil {
			return err
		}
		batch.PutCF(db.cfhVtx, []byte(id), bytes)
	}
	for _, id := range log.deletedVertices {
		batch.DeleteCF(db.cfhVtx, []byte(id))
	}
	return nil
}
func (db *RocksDBGraph) flushAddLabels(batch *grocks.WriteBatch, log *TransactionLog) {
	fmt.Println("Adding labels to DB")
	for name, label := range log.labels {
		fmt.Println("Label to be added-->", name, label.Id(), label.Name())
		batch.Put([]byte(name), []byte(label.Id()))
		batch.Put([]byte(label.Id()), []byte(name))
	}

}
func (db *RocksDBGraph) flushEdges(batch *grocks.WriteBatch, log *TransactionLog) {

	for _, edge := range log.edges {
		from := edge.From()
		to := edge.To()
		label := edge.Label()

		buf := new(bytes.Buffer)
		buf.WriteString(from)
		buf.WriteString(label.Id())
		buf.WriteByte(0)
		buf.WriteByte(1)
		buf.WriteString(to)

		buf2 := new(bytes.Buffer)
		buf2.WriteString(to)
		buf2.WriteString(label.Id())
		buf2.WriteByte(0)
		buf2.WriteByte(0)
		buf2.WriteString(from)

		batch.PutCF(db.cfhEdge, buf.Bytes(), []byte(to))
		batch.PutCF(db.cfhEdge, buf2.Bytes(), []byte(from))
	}

	for _, edge := range log.deletedEdges {

		from := edge.From()
		to := edge.To()
		label := edge.Label()

		buf := new(bytes.Buffer)
		buf.WriteString(from)
		buf.WriteString(label.Id())
		buf.WriteByte(0)
		buf.WriteByte(1)
		buf.WriteString(to)

		buf2 := new(bytes.Buffer)
		buf2.WriteString(to)
		buf2.WriteString(label.Id())
		buf2.WriteByte(0)
		buf2.WriteByte(0)
		buf2.WriteString(from)

		batch.DeleteCF(db.cfhEdge, buf.Bytes())
		batch.DeleteCF(db.cfhEdge, buf2.Bytes())
	}
}

func (db *RocksDBGraph) flushProperties(batch *grocks.WriteBatch, txlog *TransactionLog) error {
	for id, props := range txlog.vtxProperties {
		for k, v := range props {
			bytes, err := encodeKV(&k, &v)
			if err != nil {
				log.Println("error in encoding...", err.Error())
				return err
			}
			batch.MergeCF(db.cfhVtx, []byte(id), bytes)
		}
	}
	return nil
}
