package rocksdb

import (
	. "nxtdb/graph"
	"bytes"
	"log"
	"github.com/google/uuid"
	grocks "github.com/v1r3n/gorocksdb"
	"encoding/binary"
)

type GraphTransaction struct {
	vertices        map[string]Vertex
	deletedVertices []string
	deletedEdges    []GraphEdge
	vtxProperties   map[string]map[string][]byte
	edges           map[string]GraphEdge
	labels          map[string]Label
	labelsById      map[string]string
	db              *grocks.DB
	cfhVtx          *grocks.ColumnFamilyHandle
	cfhIndx         *grocks.ColumnFamilyHandle
	cfhEdge         *grocks.ColumnFamilyHandle
}

func NewGraphTransaction(db *grocks.DB, cfhVtx, cfhIndx, cfhEdge *grocks.ColumnFamilyHandle) *GraphTransaction {
	tx := GraphTransaction{
		db: db,
		cfhVtx: cfhVtx,
		cfhIndx: cfhIndx,
		cfhEdge: cfhEdge,
		vertices : make(map[string]Vertex),
		deletedVertices : make([]string, 0),
		deletedEdges : make([]GraphEdge, 0),
		edges : make(map[string]GraphEdge),
		labels : make(map[string]Label),
		labelsById: make(map[string]string),
		vtxProperties: make(map[string]map[string][]byte),
	}
	return &tx
}

func (tx *GraphTransaction) Commit() error {

	opts := grocks.NewDefaultWriteOptions()
	batch := grocks.NewWriteBatch()
	tx.flushAddLabels(batch)
	tx.flushAddedVertices(batch)
	tx.flushProperties(batch)
	tx.flushEdges(batch)
	err := tx.db.Write(opts, batch)
	if err != nil {
		return err
	}
	tx.clean()
	return nil
}

func (tx *GraphTransaction) Rollback() {
	tx.clean()
}
func (tx *GraphTransaction) clean() {
	tx.vertices = make(map[string]Vertex)
	tx.deletedVertices = make([]string, 0)
	tx.deletedEdges = make([]GraphEdge, 0)
	tx.edges = make(map[string]GraphEdge)
	tx.labels = make(map[string]Label)
	tx.vtxProperties = make(map[string]map[string][]byte)
}

func (db *GraphTransaction) AddLabel(label string) Label {
	existing := db.GetLabel(label)
	if existing == nil {
		graphLabel := GraphLabel{
			id : uuid.New().String(),
			label: label,
		}
		db.labels[label] = graphLabel
		db.labelsById[graphLabel.id] = label
		return graphLabel
	}
	return existing
}

func (db *GraphTransaction) GetLabel(label string) Label {
	existing := db.labels[label]
	if existing == nil {
		opts := grocks.NewDefaultReadOptions()
		id, err := db.db.Get(opts, []byte(label))
		if err != nil {
			log.Println("error in get label", err.Error())
			return nil
		}
		if err != nil || id == nil || id.Size() == 0 {
			return nil
		}
		existing = GraphLabel{
			id : string(id.Data()),
			label: label,
		}
	}
	db.labelsById[existing.Id()] = label
	return existing
}

func (db *GraphTransaction) getLabelById(id string) Label {
	existing := db.labelsById[id]
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
	db.labelsById[id] = existing
	label := GraphLabel{existing, id}
	db.labels[existing] = label
	return label
}

func (tx *GraphTransaction) Add(label Label, properties ...Property) string {
	id := uuid.New().String()
	vtx := GraphVertex{
		id : []byte(id),
		label : label,
		properties: make(map[string][]byte),
	}
	for _, prop := range properties {
		vtx.properties[prop.Key()] = prop.Value()
	}
	tx.vertices[id] = vtx
	tx.AddEdge(label.Id(), id, label)
	return id
}

func (tx *GraphTransaction) GetVertex(id string) Vertex {
	existing := tx.vertices[id]
	if existing == nil {
		opts := grocks.NewDefaultReadOptions()
		data, err := tx.db.GetCF(opts, tx.cfhVtx, []byte(id))
		if err != nil {
			log.Println("error when trying to get vertex", err.Error())
			return nil
		}
		bytes := make([]byte, data.Size())
		copy(bytes, data.Data())
		existing = decode(&bytes, id, tx)
	}

	return existing
}

func (tx *GraphTransaction) AddEdge(from string, to string, label Label) {
	key := from + to + label.Id()
	edge := GraphEdge{label, from, to}
	tx.edges[key] = edge
}

func (tx *GraphTransaction) RemoveEdge(from string, to string, label Label) {
	key := from + to + label.Id()
	delete(tx.edges, key)
	tx.deletedEdges = append(tx.deletedEdges, GraphEdge{label, from, to})
}

func (db *GraphTransaction) GetVertices(id string, edgeLabel Label, outgoing bool) VertexIterator {
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

func (tx *GraphTransaction) GetVerticesByLabel(vertexLabel Label) VertexIterator {
	if vertexLabel == nil {
		log.Println("missing label", vertexLabel)
		return nil
	}
	return tx.GetVertices(vertexLabel.Id(), vertexLabel, true)
}

func (tx *GraphTransaction) AddProperty(id string, key string, value []byte) {
	props := tx.vtxProperties[id];
	if props == nil {
		tx.vtxProperties[id] = make(map[string][]byte)
	}
	tx.vtxProperties[id] = map[string][]byte{key : value}
}

func (tx *GraphTransaction) AddProperties(id string, properties...Property) {
	props := tx.vtxProperties[id];
	if props == nil {
		tx.vtxProperties[id] = make(map[string][]byte)
	}

	for _, prop := range properties {
		tx.vtxProperties[id] = map[string][]byte{prop.Key() : prop.Value()}
	}
}

func (tx *GraphTransaction) RemoveProperty(id string, key string) {
	tx.AddProperty(id, key, nil)
}

func (tx *GraphTransaction) RemoveVertex(id string) {
	delete(tx.vertices, id)
	tx.deletedVertices = append(tx.deletedVertices, id)
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

func decode(data *[]byte, id string, tx *GraphTransaction) *GraphVertex {
	vtx := GraphVertex{
		id: []byte(id),
		properties:make(map[string][]byte),
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
func (tx *GraphTransaction) flushAddedVertices(batch *grocks.WriteBatch) error {
	for id, vtx := range tx.vertices {
		bytes, err := encode(&vtx)
		if err != nil {
			return err
		}
		batch.PutCF(tx.cfhVtx, []byte(id), bytes)
	}
	for _, id := range tx.deletedVertices {
		batch.DeleteCF(tx.cfhVtx, []byte(id))
	}
	return nil
}
func (db *GraphTransaction) flushAddLabels(batch *grocks.WriteBatch) {
	for name, label := range db.labels {
		batch.Put([]byte(name), []byte(label.Id()))
		batch.Put([]byte(label.Id()), []byte(name))
	}

}
func (tx *GraphTransaction) flushEdges(batch *grocks.WriteBatch) {

	for _, edge := range tx.edges {
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

	for _, edge := range tx.deletedEdges {

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
	for id, props := range tx.vtxProperties {
		for k, v := range props {
			bytes, err := encodeKV(&k, &v)
			if err != nil {
				log.Println("error in encoding...", err.Error())
				return err
			}
			batch.MergeCF(tx.cfhVtx, []byte(id), bytes)
		}
	}
	return nil
}