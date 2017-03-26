package rocksdb

import (
	. "nxtdb/graph"
)

type GraphLabel struct {
	label string
	id    string
}

type GraphVertex struct {
	label      Label
	properties map[string][]byte
	id         []byte
	tx         *GraphTransaction
}

type GraphProperty struct {
	key   string
	value []byte
}

type GraphEdge struct {
	label Label
	from  string
	to    string
}

func (label GraphLabel) Name() string {
	return label.label
}

func (label GraphLabel) Id() string {
	return label.id
}

func (vtx GraphVertex) Id() string {
	return string(vtx.id)
}

func (vtx GraphVertex) Property(name string) []byte {
	return vtx.properties[name]
}

func (vtx GraphVertex) Properties() []Property {
	props := make([]Property, len(vtx.properties))
	i := 0
	for k, v := range vtx.properties {
		props[i] = GraphProperty{k, v}
		i++
	}
	return props
}

func (vtx GraphVertex) Label() Label {
	return vtx.label
}

func (vtx GraphVertex) String() string {
	str := string(vtx.id)
	for k, v := range vtx.properties {
		str = str + "," + k + ":" + string(v)
	}
	return str
}

func (vtx GraphVertex) Out(label Label) VertexIterator {
	return vtx.tx.GetVertices(vtx.Id(), label, true)
}

func (vtx GraphVertex) In(label Label) VertexIterator {
	return vtx.tx.GetVertices(vtx.Id(), label, false)
}

func (prop GraphProperty) Key() string {
	return prop.key
}

func (prop GraphProperty) Value() []byte {
	return prop.value
}

func (edge GraphEdge) Label() Label {
	return edge.label
}

func (edge GraphEdge) From() string {
	return edge.from
}

func (edge GraphEdge) To() string {
	return edge.to
}
