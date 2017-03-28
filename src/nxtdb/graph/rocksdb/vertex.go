package rocksdb

import (
	. "nxtdb/graph"
)

type GraphVertex struct {
	label      Label
	properties map[string][]byte
	id         []byte
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

func (vtx GraphVertex) Out(label Label) VertexIterator {
	//return vtx.tx.GetVertices(vtx.Id(), label, true)
	return nil
}

func (vtx GraphVertex) In(label Label) VertexIterator {
	//return vtx.tx.GetVertices(vtx.Id(), label, false)
	return nil
}


//Adds or updates a property on the vertex
func (vtx GraphVertex) SetProperty(property string, value []byte) {

}

//Adds or updates properties on the vertex
func (vtx GraphVertex) SetProperties(properties...Property) {

}

//Remove the property on the vertex
func (vtx GraphVertex) RemoveProperty(property string) {

}

//Remove the properties on the vertex
func (vtx GraphVertex) RemoveProperties(property...string) {

}