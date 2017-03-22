package rocksdb

import "nxtdb/graph"

type GraphLabel struct {
	label string
	id    string
}

type GraphVertex struct {
	label      graph.Label
	properties map[string][]byte
	id         []byte
}

type GraphProperty struct {
	key   string
	value []byte
}

type GraphEdge struct {
	label GraphLabel
	from  GraphVertex
	to    GraphVertex
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

func (vtx GraphVertex) Properties() []graph.Property {
	return nil
}

func (vtx GraphVertex) Label() graph.Label {
	return vtx.label
}

func (vtx GraphVertex) String() string {
	str := string(vtx.id)
	for k, v := range vtx.properties {
		str = str + "," + k + ":" + string(v)
	}
	return str
}

func (prop GraphProperty) Key() string {
	return prop.key
}

func (prop GraphProperty) Value() []byte {
	return prop.value
}

func Property(key string, value []byte) GraphProperty {
	return GraphProperty{
		key : key,
		value : value,
	}
}
