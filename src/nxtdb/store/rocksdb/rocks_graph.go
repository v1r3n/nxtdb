package rocksdb

import (
	grocks "github.com/v1r3n/gorocksdb"
	"github.com/google/uuid"
	"log"
	"bytes"
	"fmt"
	"encoding/binary"
	. "nxtdb/graph"
)

type RocksDBGraph struct {
	dbPath  string
	db      *grocks.DB
	cfhVtx  *grocks.ColumnFamilyHandle
	cfhIndx *grocks.ColumnFamilyHandle
	cfhEdge *grocks.ColumnFamilyHandle
}

func (graphdb *RocksDBGraph) Open() {
	options := grocks.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	options.SetCreateIfMissingColumnFamilies(true)
	options.SetMergeOperator(PropMergeOp{})
	db, cfh, err := grocks.OpenDbColumnFamilies(options, graphdb.dbPath,
		[]string{"default", "vertex","edge","index"},
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

}

func (db *RocksDBGraph) Close() {
	db.cfhVtx.Destroy()
	db.cfhEdge.Destroy()
	db.cfhIndx.Destroy()
	db.db.Close()
}

func (db RocksDBGraph) AddLabel(label string) Label {
	opts := grocks.NewDefaultReadOptions()
	existing, err := db.db.Get(opts, []byte(label))
	var data []byte
	if err != nil || existing.Size() == 0 {
		data = []byte(uuid.New().String())
		db.db.Put(grocks.NewDefaultWriteOptions(), []byte(label), data)
	} else {
		data = existing.Data()
	}

	graphLabel := GraphLabel {
		id : string(data),
		label: label,
	}
	return graphLabel
}

func (db RocksDBGraph) GetLabel(label string) Label {
	opts := grocks.NewDefaultReadOptions()
	existing, err := db.db.Get(opts, []byte(label))
	if err != nil || existing == nil {
		return nil
	}
	graphLabel := GraphLabel {
		id : string(existing.Data()),
		label: label,
	}
	return graphLabel
}
func (db *RocksDBGraph) Add(label Label, properties ...Property) string {
	id := uuid.New().String()
	vtx := GraphVertex{
		id : []byte(id),
		label : label,
		properties: make(map[string][]byte),
	}
	for _, prop := range properties {
		vtx.properties[prop.Key()] = prop.Value()
	}
	opts := grocks.NewDefaultWriteOptions()
	bytes, err := encode(&vtx)
	if err != nil {
		fmt.Println("Error converting to bytes", err.Error())
	}
	db.db.PutCF(opts, db.cfhVtx, []byte(id), bytes)
	return id
}

func (db *RocksDBGraph) GetVertex(id string) Vertex {
	opts := grocks.NewDefaultReadOptions()
	data, err := db.db.GetCF(opts, db.cfhVtx, []byte(id))
	if err != nil {
		log.Println("error when trying to get vertex", err.Error())
		return nil
	}
	bytes := make([]byte, data.Size())
	copy(bytes, data.Data())
	vtx := decode(&bytes, db)
	return vtx
}

func (db RocksDBGraph) AddEdge(from string, to string, label Label) {
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

	batch := grocks.NewWriteBatch()
	batch.PutCF(db.cfhEdge, buf.Bytes(), []byte(to))
	batch.PutCF(db.cfhEdge, buf2.Bytes(), []byte(from))

	opts := grocks.NewDefaultWriteOptions()
	db.db.Write(opts, batch)
}

type GraphVertexIterator struct {
	prefix []byte
	iterator *grocks.Iterator
	db *RocksDBGraph
}

func (it *GraphVertexIterator) open() {
	it.iterator.Seek(it.prefix)
}
func (it *GraphVertexIterator) Next() Vertex {
	if it.iterator.ValidForPrefix(it.prefix) {
		id := string(it.iterator.Value().Data())
		it.iterator.Next()
		return it.db.GetVertex(id)
	}
	return nil
}
func (it *GraphVertexIterator) HasNext() bool {
	return it.iterator.ValidForPrefix(it.prefix)
}
func (it *GraphVertexIterator) Close() {
	it.iterator.Close()
}

func (db RocksDBGraph) GetVertices(id string, edgeLabel Label, outgoing bool) VertexIterator {
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

	vi := GraphVertexIterator{
		iterator: iterator,
		prefix: prefix,
		db: &db,
	}
	vi.open()

	return &vi
}

func (db RocksDBGraph) GetVerticesByLabel(vertexLabel Label) VertexIterator {

	/*
	options := grocks.NewDefaultReadOptions()
	data, err := db.db.GetCF(options, db.cfLabel, []byte(vertexLabel))
	if err != nil {
		log.Println("error when trying to get label data", err.Error())
		return nil
	}
	iterator := &GraphVertexIterator{
		data: data,
	}
	return iterator
	*/
	return nil
}

func (db RocksDBGraph) AddProperty(id string, key string, value []byte) {
	opts := grocks.NewDefaultWriteOptions()
	bytes, err := encodeKV(&key, &value)
	if err != nil {
		log.Println("Error when trying to encode values")
		return
	}
	merr := db.db.MergeCF(opts, db.cfhVtx, []byte(id), bytes)
	if merr != nil {
		log.Println("Error doing a merge", merr.Error())
	}

}

func (db RocksDBGraph) AddProperties(id string, properties map[string][]byte) {

}

func (db RocksDBGraph) RemoveVertex(id string) {

}
func (db RocksDBGraph) RemoveProperty(id string, key string) {

}
func (db RocksDBGraph) RemoveEdge(from string, to string, label string) {

}

func (db RocksDBGraph) Tx() Transaction {
	return nil
}
func NewGraph(path string) Graph {
	return &RocksDBGraph{
		dbPath:path,
	}
}

//private functions
func encode(vtx *GraphVertex) ([]byte, error) {
	buf := new(bytes.Buffer)
	labelBytes := []byte(vtx.label.Id())
	labelSize := uint16(len(labelBytes))

	binary.Write(buf, binary.LittleEndian, labelSize)
	binary.Write(buf, binary.LittleEndian, labelBytes)

	for k, v := range vtx.properties {
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

func decode(data *[]byte, db *RocksDBGraph) *GraphVertex {
	vtx := GraphVertex{
		properties:make(map[string][]byte),
	}

	buf := bytes.NewReader(*data)

	//Read vertex label
	var len uint16
	binary.Read(buf, binary.LittleEndian, &len)

	bytes := make([]byte, len)
	binary.Read(buf, binary.LittleEndian, bytes)
	vtx.label = db.GetLabel(string(bytes))

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

