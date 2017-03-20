package rocksdb

import (
	. "../graph"
	grocks "../../github.com/v1r3n/gorocksdb"
	"log"
)
type RocksDBGraph struct {
	dbPath string
	db *grocks.DB
}

func (graphdb *RocksDBGraph) Open() {
	options := grocks.NewDefaultOptions()
	options.SetCreateIfMissing(true)
	db, err := grocks.OpenDb(options, graphdb.dbPath)
	if err != nil {
		log.Fatal("Cannot open the database", err)
		return
	}
	graphdb.db = db
}

func (db *RocksDBGraph) Close() {
}

func (db *RocksDBGraph) CreateIndex(label string, propertyKey string) {

}

func (db *RocksDBGraph) Add(vertex *Vertex) string {
	
	return ""
}
func (db *RocksDBGraph) AddProperty(id string, key string, value []byte) {

}

func (db *RocksDBGraph) AddProperties(id string, properties map[string][]byte) {

}

func (db *RocksDBGraph) AddEdge(from string, to string, label string) {
}

func (db *RocksDBGraph) RemoveVertex(id string) {

}
func (db *RocksDBGraph) RemoveProperty(id string, key string) {

}
func (db *RocksDBGraph) RemoveEdge(from string, to string, label string) {

}
func (db *RocksDBGraph) GetVertex(id string) *Vertex {
	return nil
}
func (db *RocksDBGraph) GetVerticesByLabel(vertexLabel string) *VertexIterator {
	return nil
}
func (db *RocksDBGraph) GetVertices(id string, edgeLabel string, outgoing bool) *VertexIterator {
	return nil
}
func (db *RocksDBGraph) CountVertices(vertexLabel string) uint64 {
	return 0
}

func NewGraph(path string) Graph {
	return &RocksDBGraph{
		dbPath:path,
	}
}
