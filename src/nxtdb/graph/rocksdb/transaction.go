package rocksdb

import (
	. "nxtdb/graph"
	"log"
	"github.com/google/uuid"
)

type GraphTransaction struct {
	log *TransactionLog
	labelMap map[string]string
	db         *RocksDBGraph
}

func NewTransaction(db *RocksDBGraph) *GraphTransaction {

	tx := GraphTransaction{
		db: db,
		log: new(TransactionLog),
	}
	tx.init()

	return &tx
}

func (tx *GraphTransaction) init() {
	tx.log.vertices = make(map[string]Vertex)
	tx.log.deletedVertices = make([]string, 0)
	tx.log.deletedEdges = make([]GraphEdge, 0)
	tx.log.edges = make(map[string]GraphEdge)
	tx.log.labels = make(map[string]Label)
	tx.log.vtxProperties = make(map[string]map[string][]byte)
	tx.labelMap = make(map[string]string)
}

func (tx *GraphTransaction) GetCommitLog() *TransactionLog {
	return tx.log
}

func (tx *GraphTransaction) Rollback() {
	tx.init()
}


//ok
func (db *GraphTransaction) AddLabel(label string) Label {
	existing := db.GetLabel(label)
	if existing == nil {
		graphLabel := GraphLabel{
			id:    uuid.New().String(),
			label: label,
		}
		db.log.labels[label] = graphLabel
		db.labelMap[graphLabel.id] = label
		return graphLabel
	}
	return existing
}

//ok
func (db *GraphTransaction) GetLabel(label string) Label {
	return db.log.labels[label]
}

//ok
func (db *GraphTransaction) getLabelById(id string) Label {
	existing := db.labelMap[id]
	return db.log.labels[existing]
}

//ok
func (tx *GraphTransaction) Add(label Label, properties ...Property) Vertex {
	id := uuid.New().String()
	vtx := GraphVertex{
		id:         []byte(id),
		label:      label,
		properties: make(map[string][]byte),
	}
	for _, prop := range properties {
		vtx.properties[prop.Key()] = prop.Value()
	}
	tx.log.vertices[id] = vtx
	tx.AddEdge(label.Id(), id, label)
	return vtx
}

//ok
func (tx *GraphTransaction) GetVertex(id string) Vertex {
	existing := tx.log.vertices[id]
	return existing
}

//ok
func (tx *GraphTransaction) AddEdge(from string, to string, label Label) Edge {
	key := from + to + label.Id()
	edge := GraphEdge{label, from, to}
	tx.log.edges[key] = edge
	return edge
}

//ok
func (tx *GraphTransaction) RemoveEdge(from string, to string, label Label) {
	key := from + to + label.Id()
	delete(tx.log.edges, key)
	tx.log.deletedEdges = append(tx.log.deletedEdges, GraphEdge{label, from, to})
}

//ok
func (tx *GraphTransaction) GetVertices(id string, edgeLabel Label, outgoing bool) VertexIterator {
	return nil
}

//ok
func (tx *GraphTransaction) GetVerticesByLabel(vertexLabel Label) VertexIterator {
	if vertexLabel == nil {
		log.Println("missing label", vertexLabel)
		return nil
	}
	return tx.GetVertices(vertexLabel.Id(), vertexLabel, true)
}

//ok
func (tx *GraphTransaction) SetProperty(id string, key string, value []byte) {
	props := tx.log.vtxProperties[id];
	if props == nil {
		tx.log.vtxProperties[id] = make(map[string][]byte)
	}
	tx.log.vtxProperties[id] = map[string][]byte{key: value}
}

//ok
func (tx *GraphTransaction) SetProperties(id string, properties ...Property) {
	props := tx.log.vtxProperties[id];
	if props == nil {
		tx.log.vtxProperties[id] = make(map[string][]byte)
	}

	for _, prop := range properties {
		tx.log.vtxProperties[id] = map[string][]byte{prop.Key(): prop.Value()}
	}
}

//ok
func (tx *GraphTransaction) RemoveProperty(id string, key string) {
	tx.SetProperty(id, key, nil)
}

func (tx *GraphTransaction) RemoveProperties(id string, key ...string) {
	for _, k := range key {
		tx.RemoveProperty(id, k)
	}
}

//ok
func (tx *GraphTransaction) RemoveVertex(id string) {
	delete(tx.log.vertices, id)
	tx.log.deletedVertices = append(tx.log.deletedVertices, id)
}
