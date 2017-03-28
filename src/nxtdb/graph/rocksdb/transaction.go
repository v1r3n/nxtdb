package rocksdb

import (
	grocks "github.com/v1r3n/gorocksdb"
	. "nxtdb/graph"
	"log"
	"github.com/google/uuid"
	"bytes"
	"encoding/binary"
	"fmt"
)

type GraphTransaction struct {
	id       string
	log      *TransactionLog
	labelMap map[string]string
	db       *grocks.DB
	parent   *RocksDBGraph
	cfhVtx   *grocks.ColumnFamilyHandle
	cfhIndx  *grocks.ColumnFamilyHandle
	cfhEdge  *grocks.ColumnFamilyHandle
}

func NewTransaction(db *RocksDBGraph) *GraphTransaction {
	id := uuid.New().String()
	tx := GraphTransaction{
		db:      db.db,
		parent:  db,
		cfhIndx: db.cfhIndx,
		cfhEdge: db.cfhEdge,
		cfhVtx:  db.cfhVtx,
		id:      id,
		log:     new(TransactionLog),
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

func (tx *GraphTransaction) Id() string {
	return tx.id
}

//Add a new graph label.  If the label already exists, return the reference to existing one
func (tx *GraphTransaction) AddLabel(label string) Label {
	existing := tx.GetLabel(label)
	if existing == nil {
		graphLabel := GraphLabel{
			id:    uuid.New().String(),
			label: label,
		}
		tx.log.labels[label] = graphLabel
		tx.labelMap[graphLabel.id] = label
		return graphLabel
	}
	return existing
}

//Get Graph Label
func (tx *GraphTransaction) GetLabel(label string) Label {

	existing := tx.log.labels[label]

	if existing == nil {
		opts := grocks.NewDefaultReadOptions()
		id, err := tx.db.Get(opts, []byte(label))
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
		tx.labelMap[existing.Id()] = label
	}
	return existing
}

//Adds a new vertex to graph with properties
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

//Get vertex by Id
func (tx *GraphTransaction) GetVertex(id string) Vertex {
	existing := tx.log.vertices[id]
	if existing == nil {
		opts := grocks.NewDefaultReadOptions()
		data, err := tx.db.GetCF(opts, tx.cfhVtx, []byte(id))
		if err != nil {
			log.Println("error when trying to get vertex", err.Error())
			return nil
		}
		if data.Size() == 0 {
			return nil
		}
		bytes := make([]byte, data.Size())
		copy(bytes, data.Data())
		existing = decode(&bytes, id, tx)
	}

	return existing
}

//Adds an Edge between two vertices
func (tx *GraphTransaction) AddEdge(from string, to string, label Label) Edge {
	key := from + to + label.Id()
	edge := GraphEdge{label, from, to}
	tx.log.edges[key] = edge
	return edge
}

//Remove the Edge between two vertices with specified label
func (tx *GraphTransaction) RemoveEdge(from string, to string, label Label) {
	key := from + to + label.Id()
	delete(tx.log.edges, key)
	tx.log.deletedEdges = append(tx.log.deletedEdges, GraphEdge{label, from, to})
}

//Get the Vertices connected from the given (id) vertex with specified label
func (tx *GraphTransaction) GetVertices(id string, edgeLabel Label, outgoing bool) VertexIterator {
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
	iterator := tx.db.NewIteratorCF(opts, tx.cfhEdge)
	prefix := buf.Bytes()
	vi := NewGraphVertexIterator(prefix, iterator, tx)
	return vi
}

//ok

//Get all the vertices with the specified label
func (tx *GraphTransaction) GetVerticesByLabel(vertexLabel Label) VertexIterator {
	if vertexLabel == nil {
		log.Println("missing label", vertexLabel)
		return nil
	}
	return tx.GetVertices(vertexLabel.Id(), vertexLabel, true)
}

//Add/Update a property on the vertex
func (tx *GraphTransaction) SetProperty(id string, key string, value []byte) {
	props := tx.log.vtxProperties[id];
	if props == nil {
		tx.log.vtxProperties[id] = make(map[string][]byte)
	}
	tx.log.vtxProperties[id] = map[string][]byte{key: value}
}

//Add/Update properties on the vertex
func (tx *GraphTransaction) SetProperties(id string, properties ...Property) {
	props := tx.log.vtxProperties[id];
	if props == nil {
		tx.log.vtxProperties[id] = make(map[string][]byte)
	}

	for _, prop := range properties {
		tx.log.vtxProperties[id] = map[string][]byte{prop.Key(): prop.Value()}
	}
}

//Remove the named property from vertex
func (tx *GraphTransaction) RemoveProperty(id string, key string) {
	tx.SetProperty(id, key, nil)
}

//Remove the set of properties from vertex
func (tx *GraphTransaction) RemoveProperties(id string, key ...string) {
	for _, k := range key {
		tx.RemoveProperty(id, k)
	}
}

//Remove the Vertex, all the associated edges will be removed
func (tx *GraphTransaction) RemoveVertex(id string) {
	delete(tx.log.vertices, id)
	tx.log.deletedVertices = append(tx.log.deletedVertices, id)
}

//Commit the current transaction
func (tx *GraphTransaction) Commit() error {

	opts := grocks.NewDefaultWriteOptions()
	batch := grocks.NewWriteBatch()
	tx.flushAddLabels(batch)
	tx.flushAddedVertices(batch)
	tx.flushProperties(batch)
	tx.flushEdges(batch)

	err := tx.db.Write(opts, batch)
	if err != nil {
		fmt.Println("Error writing batch", err.Error())
		return err
	}
	tx.init()
	tx.parent.completeTx(tx.id)
	return nil
}

//Rollback the current transaction
func (tx *GraphTransaction) Rollback() {
	tx.init()
	tx.parent.completeTx(tx.id)
}

//Private helper functions
//return the label by it's internal id
func (tx *GraphTransaction) getLabelById(id string) Label {

	existing := tx.labelMap[id]
	if len(existing) == 0 {
		opts := grocks.NewDefaultReadOptions()
		id, err := tx.db.Get(opts, []byte(id))
		if err != nil {
			log.Println("error in get label by id", err.Error())
			return nil
		}
		if err != nil || id == nil || id.Size() == 0 {
			return nil
		}
		existing = string(id.Data())
	}
	tx.labelMap[id] = existing
	label := GraphLabel{existing, id}
	tx.log.labels[existing] = label
	return label
}

//encode/decode
func decode(data *[]byte, id string, tx *GraphTransaction) *GraphVertex {
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
	vtx.label = tx.getLabelById(string(bytes))

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
	bytes := buf.Bytes()
	return bytes, nil
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

//TX Commit to DB
func (tx *GraphTransaction) flushAddedVertices(batch *grocks.WriteBatch) error {
	txLog := tx.log
	for id, vtx := range txLog.vertices {
		bytes, err := encode(&vtx)
		if err != nil {
			return err
		}
		batch.PutCF(tx.cfhVtx, []byte(id), bytes)
	}
	for _, id := range txLog.deletedVertices {
		batch.DeleteCF(tx.cfhVtx, []byte(id))
	}
	return nil
}
func (tx *GraphTransaction) flushAddLabels(batch *grocks.WriteBatch) {
	fmt.Println("Adding labels to DB")
	log := tx.log
	for name, label := range log.labels {
		batch.Put([]byte(name), []byte(label.Id()))
		batch.Put([]byte(label.Id()), []byte(name))
	}

}
func (tx *GraphTransaction) flushEdges(batch *grocks.WriteBatch) {
	log := tx.log
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

		batch.PutCF(tx.cfhEdge, buf.Bytes(), []byte(to))
		batch.PutCF(tx.cfhEdge, buf2.Bytes(), []byte(from))
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

		batch.DeleteCF(tx.cfhEdge, buf.Bytes())
		batch.DeleteCF(tx.cfhEdge, buf2.Bytes())
	}
}

func (tx *GraphTransaction) flushProperties(batch *grocks.WriteBatch) error {
	log := tx.log
	for id, props := range log.vtxProperties {
		for k, v := range props {
			bytes, err := encodeKV(&k, &v)
			if err != nil {
				return err
			}
			batch.MergeCF(tx.cfhVtx, []byte(id), bytes)
		}
	}
	return nil
}
