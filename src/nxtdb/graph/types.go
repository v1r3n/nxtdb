package graph

type Vertex struct {
	label      string
	properties map[string][]byte
	id         []byte
}

type Edge struct {
	Label string
	From  []byte
	To    []byte
}

func NewVertex(label string, id []byte) Vertex {
	return Vertex{
		label : label,
		id : id,
		properties : make(map[string][]byte),
	}
}

func (vertex *Vertex) Property(key string, value []byte) {
	vertex.properties[key] = value
}

func (vertex *Vertex) GetProperty(key string) []byte {
	return vertex.properties[key]
}

func (vertex *Vertex) GetProperties() map[string][]byte {
	return vertex.properties
}



/*
 |vertex_id|properties
 |vertex_id_out_label|outgoing edges with label|
 |vertex_id_in_label|incoming edges with label|
 */