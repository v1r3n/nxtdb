package rocksdb

import ()

type GraphVertex struct {
	label      string
	properties map[string][]byte
	id         []byte
}

type GraphVertexProperty struct {
	key string
	value []byte
}

func (vtx GraphVertex) Id() string {
	return string(vtx.id)
}

func (vtx GraphVertex) Property(name string) []byte {
	return vtx.properties[name]
}

func (vtx GraphVertex) Label() string {
	return vtx.label
}

func (prop GraphVertexProperty) Key() string {
	return prop.key
}

func (prop GraphVertexProperty) Value() []byte {
	return prop.value
}

func Property(key string, value []byte) GraphVertexProperty {
	return GraphVertexProperty{
		key : key,
		value : value,
	}
}
