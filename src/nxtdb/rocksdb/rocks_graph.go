package rocksdb

import (
	. "../graph"
	grocks "../../github.com/v1r3n/gorocksdb"
	"../../github.com/google/uuid"
	"log"
	"bytes"
	"fmt"
	"encoding/binary"
)

type RocksDBGraph struct {
	dbPath  string
	db      *grocks.DB
	cfhVtx  *grocks.ColumnFamilyHandle
	cfLabel *grocks.ColumnFamilyHandle
}

func (graphdb *RocksDBGraph) Open() {
	options := grocks.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	options.SetCreateIfMissingColumnFamilies(true)
	options.SetMergeOperator(PropMergeOp{})
	db, cfh, err := grocks.OpenDbColumnFamilies(options, graphdb.dbPath,
		[]string{"default", "vertex","edge","index","labels"},
		[]*grocks.Options{options, options, options, options, options},
	)
	if err != nil {
		log.Fatal("Cannot open the database", err)
		return
	}

	graphdb.db = db
	graphdb.cfhVtx = cfh[1]
	graphdb.cfLabel = cfh[4]

}

func (db *RocksDBGraph) Close() {
	db.cfhVtx.Destroy()
	db.db.Close()
}

/*
 |key|cell|
 |vertex_id|properties
 |vertex_id_out_label|outgoing edges with label|
 |vertex_id_in_label|incoming edges with label|
 */
func (db *RocksDBGraph) Add(label string, properties ...VertexProperty) string {
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
	opts.SetSync(true)
	bytes, err := encode(&vtx)
	if err != nil {
		fmt.Println("Error converting to bytes", err.Error())
	}
	db.db.PutCF(opts, db.cfhVtx, []byte(id), bytes)
	lerr := db.db.MergeCF(opts, db.cfLabel, []byte(label), []byte(id))
	if lerr != nil {
		log.Println("Error writing to labels", lerr.Error())
	}
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
	vtx := decode(&bytes)
	return vtx
}

func (db RocksDBGraph) GetVerticesByLabel(vertexLabel string) *VertexIterator {
	
	options := grocks.NewDefaultReadOptions()
	iterator := db.db.NewIteratorCF(options, db.cfLabel)
	iterator.Seek([]byte(vertexLabel))
	return nil
}

func (db RocksDBGraph) CreateIndex(label string, propertyKey string) {

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

func (db RocksDBGraph) AddEdge(from string, to string, label string) {
}

func (db RocksDBGraph) RemoveVertex(id string) {

}
func (db RocksDBGraph) RemoveProperty(id string, key string) {

}
func (db RocksDBGraph) RemoveEdge(from string, to string, label string) {

}

func (db RocksDBGraph) GetVertices(id string, edgeLabel string, outgoing bool) *VertexIterator {
	return nil
}
func (db RocksDBGraph) CountVertices(vertexLabel string) uint64 {
	return 0
}

func NewGraph(path string) Graph {
	return &RocksDBGraph{
		dbPath:path,
	}
}

//private functions
func encode(vtx *GraphVertex) ([]byte, error) {
	buf := new(bytes.Buffer)
	labelBytes := []byte(vtx.label)
	labelSize := uint16(len(labelBytes))

	binary.Write(buf, binary.LittleEndian, labelSize)
	binary.Write(buf, binary.LittleEndian, labelBytes)
	//propertySize := uint16(len(vtx.properties))
	//binary.Write(buf, binary.LittleEndian, propertySize)

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

func decode(data *[]byte) *GraphVertex {
	vtx := GraphVertex{
		properties:make(map[string][]byte),
	}

	buf := bytes.NewReader(*data)

	//Read vertex label
	var len uint16
	binary.Read(buf, binary.LittleEndian, &len)

	bytes := make([]byte, len)
	binary.Read(buf, binary.LittleEndian, bytes)
	vtx.label = string(bytes)

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

